using System.Diagnostics;
using System.Threading.Channels;
using Telemetry.Engine.Aggregation;
using Telemetry.Engine.Observability;
using Telemetry.Engine.Processing;
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
    /// <summary>
    /// Reading value at or above which a batch is considered to contain a critical anomaly.
    /// Fed to the SIMD <see cref="BatchAnomalyDetector"/> as a single broadcast comparand.
    /// </summary>
    private const float CriticalThreshold = 95.0f;

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

        // One shard per consumer: each consumer below is handed a stable shard index and is the
        // sole writer of that shard, so the per-sensor accumulator updates are fully uncontended.
        var aggregator = new SensorAggregator(_options.SensorCount, _options.ConsumerCount);
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
            consumerTasks[i] = ConsumeAsync(channel.Reader, aggregator, metrics, shardIndex: i);

        Task sinkTask = sink.RunAsync(token);

        // Live reporting is no longer wired here: it now flows through the standard
        // System.Diagnostics.Metrics pipeline. Program.cs attaches a MeterListener
        // (ConsoleMetricsExporter) that streams the throughput summary every second.

        // --- Fast-fail shutdown ---
        // Collect every pipeline stage into a single set so we can race them.
        // If *any* task faults, we cancel the shared token immediately so the
        // surviving stages unwind quickly instead of hanging on channel I/O.
        var allTasks = new HashSet<Task>(consumerTasks) { producerTask, sinkTask };

        while (allTasks.Count > 0)
        {
            Task finished = await Task.WhenAny(allTasks).ConfigureAwait(false);
            allTasks.Remove(finished);

            if (finished.IsFaulted)
            {
                // Signal every other stage to stop, then await them so nothing
                // is left dangling.  After that, re-throw the original failure.
                await lifetime.CancelAsync().ConfigureAwait(false);
                // Complete the channel so consumers unblock from ReadAllAsync.
                channel.Writer.TryComplete(finished.Exception);

                try
                {
                    await Task.WhenAll(allTasks).ConfigureAwait(false);
                }
                catch
                {
                    // Expected: surviving tasks will throw OperationCanceledException
                    // or ChannelClosedException once we cancelled them.  We only care
                    // about the *original* fault that triggered the teardown.
                }

                // Propagate — this surfaces the original exception with its
                // full stack trace rather than a wrapped AggregateException.
                await finished.ConfigureAwait(false);
            }
        }

        wallClock.Stop();

        return new PipelineReport(
            Produced: producer.TotalProduced,
            Processed: aggregator.TotalProcessed,
            // Observed cardinality, not the configured domain size: a run that only ever
            // saw 40 of 64 possible sensors should report 40. Safe to compute here — every
            // consumer has finished, so no thread is mutating the aggregator anymore.
            DistinctSensors: aggregator.ActiveSensorCount,
            Flushes: sink.FlushCount,
            RowsPersisted: database.TotalRowsWritten,
            Elapsed: wallClock.Elapsed);
    }

    /// <summary>
    /// One consumer worker: drain batches, decode each with the zero-allocation
    /// parser via the aggregator, record observability, and always return the pooled
    /// buffer. Owns the aggregator shard identified by <paramref name="shardIndex"/> — it is the
    /// sole writer of that shard, which is what makes the per-sensor update path lock-free.
    /// </summary>
    private static async Task ConsumeAsync(
        ChannelReader<TelemetryBatch> reader,
        SensorAggregator aggregator,
        EngineMetrics metrics,
        int shardIndex)
    {
        // ReadAllAsync (no token) drains until the writer is completed, so in-flight
        // batches are never dropped on shutdown. The await yields the thread while
        // the channel is momentarily empty.
        await foreach (TelemetryBatch batch in reader.ReadAllAsync().ConfigureAwait(false))
        {
            try
            {
                // Fast SIMD pre-screen BEFORE the per-frame decode: one gather-based sweep
                // over the whole batch tells us if any reading breaches the critical
                // threshold. It is far cheaper than the full parse, so running it first lets
                // us flag an alarming batch up front; the authoritative per-frame HMAC
                // verification still happens in IngestBatch below.
                if (BatchAnomalyDetector.HasCriticalAnomalies(batch.Span, CriticalThreshold))
                    metrics.BatchesAnomalous.Add(1);

                // IngestBatch is where TelemetryParser and SensorStatistics run; its
                // return value is the count of readings successfully parsed and folded
                // in — exactly the "consumed" figure and this batch's processed size.
                // Tampered frames and authentic-but-out-of-domain SensorIds are dropped
                // and counted separately, never folded in — so one bad frame can neither
                // skew the aggregate nor fault this consumer.
                int ingested = aggregator.IngestBatch(
                    shardIndex, batch.Span, metrics.RejectedTampered, metrics.RejectedOutOfRange);

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
