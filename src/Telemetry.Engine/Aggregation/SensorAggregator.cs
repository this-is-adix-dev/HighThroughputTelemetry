using System.Diagnostics.Metrics;
using System.Threading;
using Telemetry.Engine.Domain;
using Telemetry.Engine.Parsing;

namespace Telemetry.Engine.Aggregation;

/// <summary>
/// Module C — the concurrent aggregator. Fans many consumer threads into a single
/// coherent view of per-sensor statistics with a <b>fully lock-free hot path</b>.
///
/// <para><b>Sharded, single-writer accumulators.</b> Instead of one shared
/// <c>SensorStatistics[]</c> guarded by a per-sensor <see cref="Lock"/>, the aggregator
/// holds one private <c>SensorStatistics[]</c> <i>per consumer shard</i>. Each consumer is
/// handed a stable <c>shardIndex</c> (see <see cref="Orchestration.TelemetryPipeline"/>, where
/// shard count equals consumer count) and only ever writes its own row. Two consumers updating
/// the same hot sensor now touch two <i>different</i> cache lines in two <i>different</i> arrays,
/// so the Read-For-Ownership storm that the old single shared object produced disappears: updates
/// are uncontended, lock-free, and need neither <see cref="Lock"/> nor <see cref="Interlocked"/>
/// on the per-sensor fields.</para>
///
/// <para><b>Where the synchronization went.</b> The only coordination left is the global
/// "readings processed" counter — still batched once per <see cref="IngestBatch"/> call via
/// <see cref="Interlocked.Add"/> rather than once per reading — and the periodic
/// <see cref="CreateSnapshot"/> fan-in, which sums counts/sums and min/max-reduces across the
/// shards. The snapshot runs at the flush cadence (seconds apart), so moving the only
/// cross-thread reads there makes them effectively free.</para>
///
/// <para>Sensor IDs are bounded and known at construction time (<see cref="SensorCount"/>), so
/// direct array indexing via <c>reading.SensorId</c> eliminates hashing and per-entry allocation.
/// A reading whose <c>SensorId</c> falls outside <c>[0, SensorCount)</c> — possible from a
/// misconfigured signer even when its HMAC is valid — is dropped and counted rather than indexing
/// out of bounds: authenticity does not imply a valid domain.</para>
///
/// <para><see cref="CreateSnapshot"/> writes into a pre-allocated <c>SensorSnapshot[]</c> and
/// returns a <see cref="ReadOnlySpan{T}"/> slice — zero heap allocation per call. The span lifetime
/// is bounded to one synchronous scope; callers must not hold it across an <c>await</c> or past the
/// next <see cref="CreateSnapshot"/> call.</para>
/// </summary>
public sealed class SensorAggregator
{
    // One independent accumulator array per consumer shard: _shards[shardIndex][sensorId].
    // Each inner array is a SEPARATE heap allocation, so different shards' rows live in different
    // memory regions and never false-share. Within a shard the array is contiguous (struct stored
    // by value) and single-writer, so the hot update is an in-place read-modify-write with no
    // pointer-chasing and no coherence traffic.
    private readonly SensorStatistics[][] _shards;

    private readonly int _sensorCount;

    // Reused across every CreateSnapshot call — eliminates the per-flush List<T>
    // allocation. Sized to sensorCount so it can hold all entries in the worst case.
    private readonly SensorSnapshot[] _snapshotBuffer;

    // Mutated only via Interlocked so reads and writes are always sequentially consistent.
    private long _totalProcessed;

    /// <summary>Total readings folded in so far, across all sensors and shards. Wait-free read.</summary>
    public long TotalProcessed => Interlocked.Read(ref _totalProcessed);

    /// <summary>
    /// Number of sensor slots this aggregator was configured to track.
    /// Valid <c>SensorId</c> values are in <c>[0, SensorCount)</c>.
    /// </summary>
    public int SensorCount => _sensorCount;

    /// <summary>Number of independent consumer shards. One per consumer keeps every update uncontended.</summary>
    public int ShardCount => _shards.Length;

    /// <summary>
    /// Number of sensors that have actually received at least one reading — the
    /// <i>observed</i> cardinality, as opposed to the configured domain size
    /// <see cref="SensorCount"/>. A sensor is active if any shard recorded a count for it.
    /// Intended for occasional reporting, not the hot path.
    /// </summary>
    public int ActiveSensorCount
    {
        get
        {
            int active = 0;
            for (int sensor = 0; sensor < _sensorCount; sensor++)
            {
                for (int shard = 0; shard < _shards.Length; shard++)
                {
                    if (_shards[shard][sensor].Count > 0)
                    {
                        active++;
                        break; // counted once; move to the next sensor.
                    }
                }
            }
            return active;
        }
    }

    /// <param name="sensorCount">Size of the sensor domain; valid ids are <c>[0, sensorCount)</c>.</param>
    /// <param name="shardCount">
    /// Number of single-writer shards. The pipeline passes its consumer count so each consumer owns
    /// exactly one shard and the hot path is contention-free. Defaults to 1 for single-threaded use
    /// (tests, simple callers), where shard 0 is the only shard.
    /// </param>
    public SensorAggregator(int sensorCount = 64, int shardCount = 1)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(shardCount, 1);
        ArgumentOutOfRangeException.ThrowIfNegative(sensorCount);

        _sensorCount = sensorCount;
        _snapshotBuffer = new SensorSnapshot[sensorCount];

        _shards = new SensorStatistics[shardCount][];
        for (int shard = 0; shard < shardCount; shard++)
        {
            var row = new SensorStatistics[sensorCount];
            // Seed min/max to ±∞ once, up front, so the hot Update path is branch-free.
            for (int sensor = 0; sensor < sensorCount; sensor++)
                row[sensor] = SensorStatistics.CreateEmpty();
            _shards[shard] = row;
        }
    }

    /// <summary>
    /// Fold one reading into the given consumer's shard. <b>Lock-free.</b> The caller (one consumer)
    /// must own <paramref name="shardIndex"/> exclusively — that single-writer discipline is what
    /// makes the plain field updates inside <see cref="SensorStatistics.Update"/> data-race-free.
    ///
    /// Returns <c>true</c> if the reading was folded, or <c>false</c> if its
    /// <see cref="SensorReading.SensorId"/> falls outside <c>[0, SensorCount)</c> and was dropped.
    /// HMAC verification upstream proves a frame is <i>authentic</i>, not that its id is in range, so
    /// a single misconfigured-but-validly-signed reading must never be able to throw
    /// <see cref="IndexOutOfRangeException"/> and tear down a consumer.
    /// </summary>
    public bool Update(int shardIndex, in SensorReading reading)
    {
        SensorStatistics[] shard = _shards[shardIndex];

        // Single unsigned compare — the canonical branch-free bounds check. A negative id
        // wraps to a large uint and fails the same test, so this catches both id < 0 and
        // id >= Length without a second branch.
        int id = reading.SensorId;
        if ((uint)id >= (uint)shard.Length)
            return false;

        // ref into the array element so the mutation lands in place, not on a copy.
        ref SensorStatistics stats = ref shard[id];
        stats.Update(reading.Value);
        return true;
    }

    /// <summary>Single-shard convenience for single-threaded callers (tests, simple usage). Targets shard 0.</summary>
    public bool Update(in SensorReading reading) => Update(0, reading);

    /// <summary>
    /// Decode and integrity-check a whole batch with the zero-allocation
    /// <see cref="TelemetryParser"/> and fold every authentic frame into the given consumer's shard.
    /// Frames whose HMAC fails verification are dropped and counted via
    /// <paramref name="rejectedTamperedCounter"/>. Frames that are authentic but carry a
    /// <see cref="SensorReading.SensorId"/> outside <c>[0, SensorCount)</c> are likewise dropped —
    /// counted via <paramref name="rejectedOutOfRangeCounter"/> — so a misconfigured signer can never
    /// crash the consumer. This is the bridge between Module B and Module C.
    /// </summary>
    /// <param name="shardIndex">The calling consumer's exclusively-owned shard.</param>
    /// <returns>The number of readings actually folded in (authentic <i>and</i> in-domain).</returns>
    public int IngestBatch(
        int shardIndex,
        ReadOnlySpan<byte> batch,
        Counter<long>? rejectedTamperedCounter = null,
        Counter<long>? rejectedOutOfRangeCounter = null)
    {
        // The parser verifies each frame's HMAC as it goes and increments the tampered
        // counter for any frame whose signature does not match, transparently skipping it.
        // Update then drops any authentic frame whose SensorId is out of domain. The count
        // we return is therefore the number of readings that were both authentic and
        // in-domain — neither tampered nor out-of-range frames are counted as ingested.
        var parser = new TelemetryParser(batch, rejectedTamperedCounter);
        int ingested = 0;

        while (parser.TryReadNext(out SensorReading reading))
        {
            if (Update(shardIndex, in reading))
                ingested++;
            else
                rejectedOutOfRangeCounter?.Add(1);
        }

        // One Interlocked.Add per batch rather than one Interlocked.Increment per
        // reading. At BatchSize = 1000 this is a 1000× reduction in LOCK XADD
        // instructions and the associated MESI Remote-Read-for-Ownership broadcasts
        // on the _totalProcessed cache line. This is the ONLY atomic on the hot path now.
        Interlocked.Add(ref _totalProcessed, ingested);
        return ingested;
    }

    /// <summary>Single-shard convenience for single-threaded callers (tests, simple usage). Targets shard 0.</summary>
    public int IngestBatch(
        ReadOnlySpan<byte> batch,
        Counter<long>? rejectedTamperedCounter = null,
        Counter<long>? rejectedOutOfRangeCounter = null) =>
        IngestBatch(0, batch, rejectedTamperedCounter, rejectedOutOfRangeCounter);

    /// <summary>
    /// Produce an immutable snapshot of every sensor that has received at least one reading,
    /// fanning the per-shard accumulators in: for each sensor it sums the counts and sums and
    /// min/max-reduces across every shard. Iteration is ascending by <c>SensorId</c>.
    ///
    /// <para><b>The fan-in is where the only cross-thread reads live.</b> It runs at the flush
    /// cadence while consumers may still be writing their shards. The reads are intentionally
    /// lock-free: each shard field is naturally aligned, so an individual <c>long</c>/<c>double</c>
    /// read cannot tear, and a snapshot that observes a count one or two readings stale for a
    /// still-active sensor is fine for periodic monitoring — it is not an accounting boundary.
    /// (The authoritative running total, <see cref="TotalProcessed"/>, is the Interlocked counter.)</para>
    ///
    /// <para><b>Not reentrant.</b> The result is written into the shared, reused
    /// <c>_snapshotBuffer</c> and returned as a <see cref="ReadOnlySpan{T}"/> over it, so a single
    /// consumer (here, the sink) must own every call: concurrent callers would corrupt one another's
    /// buffer and hand back aliasing spans. The buffer is also overwritten on the next call, so
    /// callers must consume the span within the same synchronous scope and must not hold it across an
    /// <c>await</c> or past the next call.</para>
    /// </summary>
    public ReadOnlySpan<SensorSnapshot> CreateSnapshot()
    {
        int shardCount = _shards.Length;
        int count = 0;

        for (int sensor = 0; sensor < _sensorCount; sensor++)
        {
            long totalCount = 0;
            double totalSum = 0.0;
            double min = double.PositiveInfinity;
            double max = double.NegativeInfinity;

            // Reduce this sensor across every shard. Reading the struct by ref avoids copying
            // 32 bytes per shard and keeps the access an in-place field read.
            for (int shard = 0; shard < shardCount; shard++)
            {
                ref readonly SensorStatistics stats = ref _shards[shard][sensor];
                
                // Acquire fence: ensures we don't read Sum, Min, Max before Count
                long observedCount = Volatile.Read(ref System.Runtime.CompilerServices.Unsafe.AsRef(in stats.Count));
                
                if (observedCount == 0)
                    continue; // untouched in this shard; its min/max are still the ±∞ identities.

                totalCount += observedCount;
                totalSum += stats.Sum;
                if (stats.Min < min) min = stats.Min;
                if (stats.Max > max) max = stats.Max;
            }

            if (totalCount > 0)
            {
                double average = totalSum / totalCount;
                _snapshotBuffer[count++] = new SensorSnapshot(sensor, totalCount, min, max, average);
            }
        }

        return _snapshotBuffer.AsSpan(0, count);
    }
}
