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

    private readonly Meter _meter;

    /// <summary>Counts readings synthesized by the firehose and handed to the channel.</summary>
    public Counter<long> ReadingsProduced { get; }

    /// <summary>Counts readings that were successfully parsed and folded into the aggregate.</summary>
    public Counter<long> ReadingsConsumed { get; }

    /// <summary>Distribution of processed batch sizes (readings per batch).</summary>
    public Histogram<int> BatchSize { get; }

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
    }

    /// <summary>
    /// Disposes the underlying <see cref="Meter"/>, which unpublishes its instruments
    /// and detaches any live listeners. Owned for the lifetime of a pipeline run.
    /// </summary>
    public void Dispose() => _meter.Dispose();
}
