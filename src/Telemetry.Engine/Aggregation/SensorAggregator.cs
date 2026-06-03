using System.Diagnostics.Metrics;
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
///   <item><b>Lock-free</b> for the global "readings processed" counter — batched
///   once per <see cref="IngestBatch"/> call via <see cref="Interlocked.Add"/>
///   rather than once per reading. With <c>BatchSize = 1000</c> this is a
///   1000× reduction in atomic operations and MESI cache-line coherence traffic on
///   <c>_totalProcessed</c>.</item>
///   <item><b>Low-lock</b> for per-sensor statistics — each entry guards its own
///   compound update (count + sum + min + max) behind a fine-grained
///   <see cref="Lock"/>. Different sensors never contend with each other.</item>
/// </list>
///
/// Sensor IDs are bounded and known at construction time (<see cref="SensorCount"/>),
/// so a pre-sized <c>SensorStatistics[]</c> replaces the previous
/// <c>ConcurrentDictionary&lt;int, SensorStatistics&gt;</c>. Direct array indexing
/// via <c>reading.SensorId</c> eliminates hash computation, stripe-lock contention,
/// and per-entry <c>Node&lt;K,V&gt;</c> heap allocations on every hot-path update.
///
/// <see cref="CreateSnapshot"/> writes into a pre-allocated <c>SensorSnapshot[]</c>
/// and returns a <see cref="ReadOnlySpan{T}"/> slice — zero heap allocation per call.
/// The span lifetime is bounded to one synchronous scope; callers must not hold it
/// across an <c>await</c> or past the next <see cref="CreateSnapshot"/> call.
/// </summary>
public sealed class SensorAggregator
{
    // Pre-sized to the configured sensor domain; indexed directly by SensorId.
    // All entries are eagerly initialized at construction so the hot update path
    // is a single array-bounds-check + pointer-deref, not a dictionary lookup.
    private readonly SensorStatistics[] _bySensor;

    // Reused across every CreateSnapshot call — eliminates the per-flush List<T>
    // allocation. Sized to sensorCount so it can hold all entries in the worst case.
    private readonly SensorSnapshot[] _snapshotBuffer;

    // Mutated only via Interlocked so reads and writes are always sequentially consistent.
    private long _totalProcessed;

    /// <summary>Total readings folded in so far, across all sensors. Wait-free read.</summary>
    public long TotalProcessed => Interlocked.Read(ref _totalProcessed);

    /// <summary>
    /// Number of sensor slots this aggregator was configured to track.
    /// Valid <c>SensorId</c> values are in <c>[0, SensorCount)</c>.
    /// </summary>
    public int SensorCount => _bySensor.Length;

    public SensorAggregator(int sensorCount = 64)
    {
        _bySensor = new SensorStatistics[sensorCount];
        _snapshotBuffer = new SensorSnapshot[sensorCount];
        for (int i = 0; i < sensorCount; i++)
            _bySensor[i] = new SensorStatistics();
    }

    /// <summary>
    /// Fold one reading into the aggregate. Safe to call from any thread.
    /// Direct array index — O(1) with no hashing or lock contention across sensors.
    /// </summary>
    public void Update(in SensorReading reading) =>
        _bySensor[reading.SensorId].Update(in reading);

    /// <summary>
    /// Decode and integrity-check a whole batch with the zero-allocation
    /// <see cref="TelemetryParser"/> and fold every authentic frame in. Frames whose
    /// HMAC fails verification are dropped and counted via
    /// <paramref name="rejectedTamperedCounter"/>. This is the bridge between Module B
    /// and Module C.
    /// </summary>
    public int IngestBatch(ReadOnlySpan<byte> batch, Counter<long>? rejectedTamperedCounter = null)
    {
        // The parser verifies each frame's HMAC as it goes and increments the supplied
        // counter for any frame whose signature does not match, transparently skipping it.
        // The count we return is therefore the number of *authentic* readings folded in —
        // tampered frames never reach Update.
        var parser = new TelemetryParser(batch, rejectedTamperedCounter);
        int ingested = 0;

        while (parser.TryReadNext(out SensorReading reading))
        {
            Update(in reading);
            ingested++;
        }

        // One Interlocked.Add per batch rather than one Interlocked.Increment per
        // reading. At BatchSize = 1000 this is a 1000× reduction in LOCK XADD
        // instructions and the associated MESI Remote-Read-for-Ownership broadcasts
        // on the _totalProcessed cache line.
        Interlocked.Add(ref _totalProcessed, ingested);
        return ingested;
    }

    /// <summary>
    /// Produce an immutable snapshot of every sensor that has received at least one
    /// reading. Each bucket is sampled under its own lock, so this is consistent per
    /// sensor without ever stopping the world. The iteration order is ascending by
    /// <c>SensorId</c> and has perfect cache locality (sequential array walk).
    ///
    /// The returned <see cref="ReadOnlySpan{T}"/> points into <c>_snapshotBuffer</c>,
    /// which is overwritten on the next call. Callers must consume the span within
    /// the same synchronous scope and must not hold it past the next call.
    /// </summary>
    public ReadOnlySpan<SensorSnapshot> CreateSnapshot()
    {
        int count = 0;
        for (int i = 0; i < _bySensor.Length; i++)
        {
            SensorSnapshot snapshot = _bySensor[i].Snapshot(i);
            if (snapshot.Count > 0)
                _snapshotBuffer[count++] = snapshot;
        }
        return _snapshotBuffer.AsSpan(0, count);
    }
}
