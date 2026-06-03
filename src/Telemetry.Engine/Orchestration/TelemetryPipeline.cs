using System.Diagnostics;
using System.Threading.Channels;
using Telemetry.Engine.Aggregation;
using Telemetry.Engine.Producer;
using Telemetry.Engine.Sink;

namespace Telemetry.Engine.Orchestration;

/// <summary>
/// Composition root for the whole pipeline. Owns the channel and wires the
/// producer, the consumer fan-out, the aggregator, the sink and the live console
/// reporter together, then runs them for a bounded duration with a clean shutdown.
///
/// Keeping this orchestration out of <c>Program.cs</c> honours the separation of
/// concerns directive: <c>Program</c> only chooses parameters and prints the final
/// report; <c>TelemetryPipeline</c> knows how the parts connect.
/// </summary>
public sealed class TelemetryPipeline
{
    private readonly PipelineOptions _options;

    public TelemetryPipeline(PipelineOptions? options = null) =>
        _options = options ?? new PipelineOptions();

    public async Task<PipelineReport> RunAsync(CancellationToken externalToken = default)
    {
        // Bounded channel = built-in back-pressure. FullMode.Wait makes a producer
        // that outruns the consumers asynchronously park rather than drop data or
        // grow unbounded. SingleReader is false (we fan out), SingleWriter is true.
        var channel = Channel.CreateBounded<TelemetryBatch>(new BoundedChannelOptions(_options.ChannelCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleWriter = true,
            SingleReader = false,
        });

        var aggregator = new SensorAggregator();
        var database = new DummySlowDatabase();
        var sink = new AsyncDataSink(aggregator, database, _options.FlushInterval, _options.SinkShardCount);
        var producer = new FirehoseGenerator(
            channel.Writer,
            _options.TargetReadingsPerSecond,
            _options.BatchSize,
            _options.SensorCount);

        // The simulation stops after the configured duration, or earlier if the
        // caller cancels (e.g. Ctrl+C wired in Program).
        using var lifetime = CancellationTokenSource.CreateLinkedTokenSource(externalToken);
        lifetime.CancelAfter(_options.Duration);
        CancellationToken token = lifetime.Token;

        var wallClock = Stopwatch.StartNew();

        // --- Start every stage ---
        Task producerTask = producer.RunAsync(token);

        var consumerTasks = new Task[_options.ConsumerCount];
        for (int i = 0; i < consumerTasks.Length; i++)
            consumerTasks[i] = ConsumeAsync(channel.Reader, aggregator);

        Task sinkTask = sink.RunAsync(token);
        Task reporterTask = ReportAsync(aggregator, token);

        // --- Shutdown sequence ---
        // 1. Producer returns once the duration elapses; on its way out it completes
        //    the channel writer, which is what lets the consumers below terminate.
        await producerTask.ConfigureAwait(false);

        // 2. Consumers drain whatever is still queued, then finish.
        await Task.WhenAll(consumerTasks).ConfigureAwait(false);

        // 3. Sink and reporter observe the same token and wind down; the sink also
        //    performs its final flush internally.
        await sinkTask.ConfigureAwait(false);
        await reporterTask.ConfigureAwait(false);

        wallClock.Stop();

        return new PipelineReport(
            Produced: producer.TotalProduced,
            Processed: aggregator.TotalProcessed,
            DistinctSensors: aggregator.SensorCount,
            Flushes: sink.FlushCount,
            RowsPersisted: database.TotalRowsWritten,
            Elapsed: wallClock.Elapsed);
    }

    /// <summary>
    /// One consumer worker: drain batches, decode each with the zero-allocation
    /// parser via the aggregator, and always return the pooled buffer.
    /// </summary>
    private static async Task ConsumeAsync(ChannelReader<TelemetryBatch> reader, SensorAggregator aggregator)
    {
        // ReadAllAsync (no token) drains until the writer is completed, so in-flight
        // batches are never dropped on shutdown. The await yields the thread while
        // the channel is momentarily empty.
        await foreach (TelemetryBatch batch in reader.ReadAllAsync().ConfigureAwait(false))
        {
            try
            {
                aggregator.IngestBatch(batch.Span);
            }
            finally
            {
                // Critical: hand the rented array back so steady-state GC stays flat.
                batch.Return();
            }
        }
    }

    /// <summary>Prints a live throughput line roughly once per second.</summary>
    private async Task ReportAsync(SensorAggregator aggregator, CancellationToken token)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        long previousProcessed = 0;
        var sw = Stopwatch.StartNew();

        try
        {
            while (await timer.WaitForNextTickAsync(token).ConfigureAwait(false))
            {
                long processed = aggregator.TotalProcessed;
                long delta = processed - previousProcessed;
                previousProcessed = processed;

                Console.WriteLine(
                    $"[{sw.Elapsed.TotalSeconds,5:F1}s] " +
                    $"throughput: {delta,8:N0} readings/s | " +
                    $"total: {processed,10:N0} | " +
                    $"sensors: {aggregator.SensorCount,3}");
            }
        }
        catch (OperationCanceledException)
        {
            // Expected at end-of-run.
        }
    }
}
