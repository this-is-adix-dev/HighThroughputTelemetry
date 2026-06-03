using System.Diagnostics;
using System.Threading.Channels;
using Telemetry.Engine.Aggregation;
using Telemetry.Engine.Observability;
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

        // One metrics instance owns the Meter for this run. Disposing it at the end
        // unpublishes the instruments and detaches any live MeterListener. It is
        // shared (not static) so it stays unit-testable and matches the pipeline's
        // constructor-injection style; the console exporter still finds the meter by
        // name, so the two sides remain decoupled.
        using var metrics = new EngineMetrics();

        var aggregator = new SensorAggregator(_options.SensorCount);
        var database = new DummySlowDatabase();
        var sink = new AsyncDataSink(aggregator, database, _options.FlushInterval, _options.SinkShardCount);
        var producer = new FirehoseGenerator(
            channel.Writer,
            metrics,
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
            consumerTasks[i] = ConsumeAsync(channel.Reader, aggregator, metrics);

        Task sinkTask = sink.RunAsync(token);

        // Live reporting is no longer wired here: it now flows through the standard
        // System.Diagnostics.Metrics pipeline. Program.cs attaches a MeterListener
        // (ConsoleMetricsExporter) that streams the throughput summary every second.

        // --- Shutdown sequence ---
        // 1. Producer returns once the duration elapses; on its way out it completes
        //    the channel writer, which is what lets the consumers below terminate.
        await producerTask.ConfigureAwait(false);

        // 2. Consumers drain whatever is still queued, then finish.
        await Task.WhenAll(consumerTasks).ConfigureAwait(false);

        // 3. The sink observes the same token and winds down; it also performs its
        //    final flush internally so the last window of data is never lost.
        await sinkTask.ConfigureAwait(false);

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
    /// parser via the aggregator, record observability, and always return the pooled
    /// buffer.
    /// </summary>
    private static async Task ConsumeAsync(
        ChannelReader<TelemetryBatch> reader,
        SensorAggregator aggregator,
        EngineMetrics metrics)
    {
        // ReadAllAsync (no token) drains until the writer is completed, so in-flight
        // batches are never dropped on shutdown. The await yields the thread while
        // the channel is momentarily empty.
        await foreach (TelemetryBatch batch in reader.ReadAllAsync().ConfigureAwait(false))
        {
            try
            {
                // IngestBatch is where TelemetryParser and SensorStatistics run; its
                // return value is the count of readings successfully parsed and folded
                // in — exactly the "consumed" figure and this batch's processed size.
                int ingested = aggregator.IngestBatch(batch.Span, metrics.RejectedTampered);

                // Two tag-less recordings. Add(long)/Record(int) take the value by
                // value, so there is no boxing and no tag array — zero heap traffic on
                // the consumer hot path, which is what keeps GC flat at 100k/sec.
                metrics.ReadingsConsumed.Add(ingested);
                metrics.BatchSize.Record(ingested);
            }
            finally
            {
                // Critical: hand the rented array back so steady-state GC stays flat.
                batch.Return();
            }
        }
    }
}
