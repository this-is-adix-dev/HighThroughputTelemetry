using System.Diagnostics.Metrics;

namespace Telemetry.Engine.Observability;

/// <summary>
/// Single source of truth for the engine's <see cref="System.Diagnostics.Metrics"/>
/// instrumentation. Owns one <see cref="Meter"/> and the three instruments the
/// pipeline records into, so the call sites stay a single dotted access away from
/// a measurement (<c>metrics.ReadingsProduced.Add(n)</c>).
///
/// Why <c>System.Diagnostics.Metrics</c> rather than an OpenTelemetry package:
/// <list type="bullet">
///   <item><b>Native AOT.</b> These types live in the runtime and use no
///   reflection on the recording path, so they survive trimming and AOT without
///   the rooting/reflection caveats most exporter packages carry.</item>
///   <item><b>Zero-allocation recording.</b> The instrument's no-tag overloads
///   (<see cref="Counter{T}.Add(T)"/>, <see cref="Histogram{T}.Record(T)"/>) take
///   the measurement by value. With no <c>KeyValuePair&lt;string, object?&gt;</c>
///   tags there is no boxing of the value and no tag array — recording is heap-free
///   on the hot path. Callers MUST keep using those tag-less overloads.</item>
/// </list>
///
/// The instruments are exposed as properties (not wrapped behind helper methods) so
/// the JIT/AOT compiler can inline the <c>Add</c>/<c>Record</c> call directly and no
/// closure is ever captured.
/// </summary>
public sealed class EngineMetrics : IDisposable
{
    /// <summary>
    /// Meter name. A <see cref="MeterListener"/> subscribes by this exact string, so
    /// it is shared as a constant between the producer of the metrics (this class)
    /// and any consumer (e.g. <see cref="ConsoleMetricsExporter"/>).
    /// </summary>
    public const string MeterName = "HighThroughputTelemetry.Engine";

    /// <summary>Instrument names, also shared as constants for listener-side routing.</summary>
    public const string ReadingsProducedName = "telemetry.readings.produced";
    public const string ReadingsConsumedName = "telemetry.readings.consumed";
    public const string BatchSizeName = "telemetry.batch.size";
    public const string RejectedTamperedName = "telemetry.readings.rejected.tampered";
    public const string RejectedOutOfRangeName = "telemetry.readings.rejected.outofrange";
    public const string BatchesAnomalousName = "telemetry.batches.anomalous";

    private readonly Meter _meter;

    /// <summary>Counts readings synthesized by the firehose and handed to the channel.</summary>
    public Counter<long> ReadingsProduced { get; }

    /// <summary>Counts readings that were successfully parsed and folded into the aggregate.</summary>
    public Counter<long> ReadingsConsumed { get; }

    /// <summary>Distribution of processed batch sizes (readings per batch).</summary>
    public Histogram<int> BatchSize { get; }

    /// <summary>
    /// Counts readings discarded because their HMAC signature failed verification —
    /// i.e. the frame was tampered with or corrupted in flight. A healthy pipeline keeps
    /// this at (or very near) zero.
    /// </summary>
    public Counter<long> RejectedTampered { get; }

    /// <summary>
    /// Counts authentic readings discarded because their <c>SensorId</c> fell outside the
    /// configured <c>[0, SensorCount)</c> domain — e.g. a signer/consumer configuration
    /// drift. A healthy, correctly-configured pipeline keeps this at exactly zero; a
    /// non-zero value is an early warning of a misconfiguration upstream.
    /// </summary>
    public Counter<long> RejectedOutOfRange { get; }

    /// <summary>
    /// Counts whole batches in which the SIMD <see cref="Processing.BatchAnomalyDetector"/>
    /// found at least one reading breaching the critical threshold. Incremented once per
    /// flagged batch (not per anomalous reading), so it tracks how often the fast pre-screen
    /// fires rather than the total volume of out-of-range samples.
    /// </summary>
    public Counter<long> BatchesAnomalous { get; }

    public EngineMetrics()
    {
        // A version string lets downstream collectors disambiguate schema changes.
        _meter = new Meter(MeterName, version: "1.0.0");

        ReadingsProduced = _meter.CreateCounter<long>(
            ReadingsProducedName,
            unit: "{reading}",
            description: "Total sensor readings synthesized and published to the channel.");

        ReadingsConsumed = _meter.CreateCounter<long>(
            ReadingsConsumedName,
            unit: "{reading}",
            description: "Total sensor readings successfully parsed and aggregated.");

        BatchSize = _meter.CreateHistogram<int>(
            BatchSizeName,
            unit: "{reading}",
            description: "Number of readings in each processed batch.");

        RejectedTampered = _meter.CreateCounter<long>(
            RejectedTamperedName,
            unit: "{reading}",
            description: "Total readings rejected because their HMAC signature failed verification (tampered/corrupted).");

        RejectedOutOfRange = _meter.CreateCounter<long>(
            RejectedOutOfRangeName,
            unit: "{reading}",
            description: "Total authentic readings dropped because their SensorId fell outside the configured domain.");

        BatchesAnomalous = _meter.CreateCounter<long>(
            BatchesAnomalousName,
            unit: "{batch}",
            description: "Total batches the SIMD anomaly detector flagged for breaching the critical threshold.");
    }

    /// <summary>
    /// Disposes the underlying <see cref="Meter"/>, which unpublishes its instruments
    /// and detaches any live listeners. Owned for the lifetime of a pipeline run.
    /// </summary>
    public void Dispose() => _meter.Dispose();
}
