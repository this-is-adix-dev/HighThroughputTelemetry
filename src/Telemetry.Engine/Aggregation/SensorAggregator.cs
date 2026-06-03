using System.Collections.Concurrent;
using System.Threading;
using Telemetry.Engine.Domain;
using Telemetry.Engine.Parsing;

namespace Telemetry.Engine.Aggregation;

/// <summary>
/// Module C — the concurrent aggregator. Fans many consumer threads into a single
/// coherent view of per-sensor statistics.
///
/// The design deliberately mixes two levels of synchronization to match the
/// contention profile of each piece of state:
/// <list type="bullet">
///   <item><b>Lock-free</b> for the global "readings processed" counter — a single
///   hot integer touched on every reading, so it uses
///   <see cref="Interlocked"/> and never blocks.</item>
///   <item><b>Low-lock</b> for per-sensor statistics — a
///   <see cref="ConcurrentDictionary{TKey,TValue}"/> for safe, sharded lookup,
///   with each bucket guarding its own compound update behind a fine-grained
///   <see cref="Lock"/> (see <see cref="SensorStatistics"/>). Different sensors
///   never contend with each other.</item>
/// </list>
/// </summary>
public sealed class SensorAggregator
{
    private readonly ConcurrentDictionary<int, SensorStatistics> _bySensor = new();

    // Padded onto its own field; mutated only via Interlocked so it is wait-free.
    private long _totalProcessed;

    /// <summary>Total readings folded in so far, across all sensors. Wait-free read.</summary>
    public long TotalProcessed => Interlocked.Read(ref _totalProcessed);

    /// <summary>Number of distinct sensors seen.</summary>
    public int SensorCount => _bySensor.Count;

    /// <summary>Fold one reading into the aggregate. Safe to call from any thread.</summary>
    public void Update(in SensorReading reading)
    {
        // GetOrAdd with the factory-delegate overload that takes no captured state,
        // so the lambda is cached as a static and allocates nothing per call.
        SensorStatistics stats = _bySensor.GetOrAdd(reading.SensorId, static _ => new SensorStatistics());
        stats.Update(in reading);

        Interlocked.Increment(ref _totalProcessed);
    }

    /// <summary>
    /// Decode a whole batch with the zero-allocation <see cref="TelemetryParser"/>
    /// and fold every frame in. This is the bridge between Module B and Module C.
    /// </summary>
    public int IngestBatch(ReadOnlySpan<byte> batch)
    {
        var parser = new TelemetryParser(batch);
        int ingested = 0;

        while (parser.TryReadNext(out SensorReading reading))
        {
            Update(in reading);
            ingested++;
        }

        return ingested;
    }

    /// <summary>
    /// Produce an immutable snapshot of every sensor's current statistics.
    /// Each bucket is sampled under its own lock, so this is consistent per sensor
    /// without ever stopping the world.
    /// </summary>
    public IReadOnlyList<SensorSnapshot> CreateSnapshot()
    {
        var snapshots = new List<SensorSnapshot>(_bySensor.Count);
        foreach (KeyValuePair<int, SensorStatistics> entry in _bySensor)
            snapshots.Add(entry.Value.Snapshot(entry.Key));

        return snapshots;
    }
}
