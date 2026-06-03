using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Telemetry.Engine.Observability;

/// <summary>
/// A dependency-free, Native-AOT-safe replacement for an OpenTelemetry console
/// exporter. It attaches a <see cref="MeterListener"/> to the engine's
/// <see cref="EngineMetrics.MeterName"/> meter and, on a one-second
/// <see cref="PeriodicTimer"/>, prints a compact live throughput summary derived
/// purely from the recorded metrics.
///
/// Design notes:
/// <list type="bullet">
///   <item><b>Pull/aggregate split.</b> <see cref="MeterListener"/> measurement
///   callbacks fire <i>synchronously on the recording thread</i> (producer /
///   consumer threads). We must therefore do the absolute minimum there: a single
///   wait-free <see cref="Interlocked"/> add into a running total. The formatting
///   and console I/O happen later, on the reporter's own task — never on a hot
///   path.</item>
///   <item><b>Zero-allocation callbacks.</b> Routing is done by reference-comparing
///   the <see cref="Instrument"/> captured at subscription time, so no strings are
///   compared and nothing is boxed per measurement.</item>
///   <item><b>Subscribe by name.</b> The listener needs no reference to
///   <see cref="EngineMetrics"/>; it matches the meter by name. This keeps the
///   producing and observing sides fully decoupled and order-independent — the
///   listener catches instruments whether they already exist at
///   <see cref="Start"/> time or are created afterwards.</item>
/// </list>
/// </summary>
public sealed class ConsoleMetricsExporter : IAsyncDisposable
{
    private readonly string _meterName;
    private readonly TimeSpan _interval;
    private readonly MeterListener _listener;
    private readonly CancellationTokenSource _stopReporting = new();

    // Cumulative running totals, mutated only via Interlocked from the measurement
    // callbacks and read via Interlocked from the reporting loop.
    private long _producedTotal;
    private long _consumedTotal;
    private long _batchesTotal;
    private long _rejectedTotal;

    // Instrument identities captured in InstrumentPublished. `volatile` because they
    // are published on the thread that creates the meter and then read on the
    // recording threads; in our wiring the writes happen-before any measurement, but
    // volatile makes the cross-thread hand-off correct regardless of call order.
    private volatile Instrument? _producedInstrument;
    private volatile Instrument? _consumedInstrument;
    private volatile Instrument? _batchSizeInstrument;
    private volatile Instrument? _rejectedInstrument;

    // volatile: Start() writes this on the caller's thread; DisposeAsync() reads and
    // nulls it — potentially from a different thread. The other instrument fields are
    // already volatile for the same reason; this field follows the same pattern.
    private volatile Task? _reportingLoop;

    /// <summary>
    /// Cumulative number of frames the consumers rejected for a failed HMAC signature,
    /// observed purely through the metrics pipeline. Surfaced so the final report can show
    /// that tamper detection actually fired during the run.
    /// </summary>
    public long RejectedTamperedTotal => Interlocked.Read(ref _rejectedTotal);

    public ConsoleMetricsExporter(string? meterName = null, TimeSpan? reportInterval = null)
    {
        _meterName = meterName ?? EngineMetrics.MeterName;
        _interval = reportInterval ?? TimeSpan.FromSeconds(1);

        _listener = new MeterListener { InstrumentPublished = OnInstrumentPublished };

        // One callback per measurement CLR type. The two counters are Counter<long>
        // (long measurements); the histogram is Histogram<int> (int measurements).
        // The delegates are created once here, never per measurement.
        _listener.SetMeasurementEventCallback<long>(OnCounterMeasured);
        _listener.SetMeasurementEventCallback<int>(OnHistogramMeasured);
    }

    /// <summary>
    /// Starts listening and kicks off the background reporting loop. Safe to call once.
    /// </summary>
    public void Start()
    {
        // Start() invokes InstrumentPublished for every instrument that already
        // exists, and the listener keeps receiving the callback for instruments
        // created later — so this works no matter the relative ordering of Start()
        // and EngineMetrics construction.
        _listener.Start();
        _reportingLoop = RunReportingLoopAsync(_stopReporting.Token);
    }

    /// <summary>
    /// Decide which instruments to observe. We only enable the engine meter's three
    /// instruments and stash their identities for fast, allocation-free routing.
    /// </summary>
    private void OnInstrumentPublished(Instrument instrument, MeterListener listener)
    {
        if (instrument.Meter.Name != _meterName)
            return;

        switch (instrument.Name)
        {
            case EngineMetrics.ReadingsProducedName:
                _producedInstrument = instrument;
                break;
            case EngineMetrics.ReadingsConsumedName:
                _consumedInstrument = instrument;
                break;
            case EngineMetrics.BatchSizeName:
                _batchSizeInstrument = instrument;
                break;
            case EngineMetrics.RejectedTamperedName:
                _rejectedInstrument = instrument;
                break;
            default:
                return; // an instrument we don't track — leave it unsubscribed.
        }

        listener.EnableMeasurementEvents(instrument);
    }

    /// <summary>
    /// Hot-path callback for the two <see cref="Counter{T}"/> instruments. Runs on the
    /// producing/consuming thread, so it does nothing but a single wait-free add.
    /// </summary>
    private void OnCounterMeasured(
        Instrument instrument,
        long measurement,
        ReadOnlySpan<KeyValuePair<string, object?>> tags,
        object? state)
    {
        if (ReferenceEquals(instrument, _consumedInstrument))
            Interlocked.Add(ref _consumedTotal, measurement);
        else if (ReferenceEquals(instrument, _producedInstrument))
            Interlocked.Add(ref _producedTotal, measurement);
        else if (ReferenceEquals(instrument, _rejectedInstrument))
            Interlocked.Add(ref _rejectedTotal, measurement);
    }

    /// <summary>
    /// Hot-path callback for the batch-size <see cref="Histogram{T}"/>. The number of
    /// recordings equals the number of processed batches, which is the figure we
    /// surface; the per-batch value itself is folded into the throughput totals via
    /// the consumed counter.
    /// </summary>
    private void OnHistogramMeasured(
        Instrument instrument,
        int measurement,
        ReadOnlySpan<KeyValuePair<string, object?>> tags,
        object? state)
    {
        if (ReferenceEquals(instrument, _batchSizeInstrument))
            Interlocked.Increment(ref _batchesTotal);
    }

    /// <summary>
    /// Reads the accumulated metrics once per interval and prints a throughput line.
    /// Throughput is the consumed-reading delta over the elapsed interval; totals are
    /// cumulative. This is the only place the exporter touches the console.
    /// </summary>
    private async Task RunReportingLoopAsync(CancellationToken token)
    {
        using var timer = new PeriodicTimer(_interval);
        var elapsed = Stopwatch.StartNew();
        long previousConsumed = 0;
        long previousProduced = 0;

        try
        {
            while (await timer.WaitForNextTickAsync(token).ConfigureAwait(false))
            {
                long consumed = Interlocked.Read(ref _consumedTotal);
                long produced = Interlocked.Read(ref _producedTotal);
                long batches = Interlocked.Read(ref _batchesTotal);
                long rejected = Interlocked.Read(ref _rejectedTotal);

                // Divide by the real interval so the "per sec" figure stays correct
                // even if the reporting cadence is ever retuned.
                double seconds = Math.Max(_interval.TotalSeconds, double.Epsilon);
                long consumedRate = (long)((consumed - previousConsumed) / seconds);
                long producedRate = (long)((produced - previousProduced) / seconds);

                previousConsumed = consumed;
                previousProduced = produced;

                Console.WriteLine(
                    $"[{elapsed.Elapsed.TotalSeconds,5:F1}s] " +
                    $"Throughput: {consumedRate,8:N0} msg/sec | " +
                    $"Produced: {producedRate,8:N0} msg/sec | " +
                    $"Batches processed: {batches,6:N0} | " +
                    $"Total: {consumed,11:N0} | " +
                    $"Tampered: {rejected,5:N0}");
            }
        }
        catch (OperationCanceledException)
        {
            // Expected when the exporter is disposed; fall through to a clean exit.
        }
    }

    /// <summary>
    /// Stops the reporting loop and tears down the listener. Disposing the listener
    /// also detaches every measurement callback. Idempotent.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (!_stopReporting.IsCancellationRequested)
            await _stopReporting.CancelAsync().ConfigureAwait(false);

        if (_reportingLoop is { } loop)
        {
            _reportingLoop = null;
            await loop.ConfigureAwait(false); // loop swallows cancellation internally.
        }

        _listener.Dispose();
        _stopReporting.Dispose();
    }
}
