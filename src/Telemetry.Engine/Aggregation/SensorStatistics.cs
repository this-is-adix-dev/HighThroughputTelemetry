namespace Telemetry.Engine.Aggregation;

/// <summary>
/// Running statistics for a single sensor <i>within one consumer shard</i>
/// (count, sum, min, max).
///
/// <para><b>Why this is now a lock-free mutable <c>struct</c>, not a locked class.</b>
/// The previous design shared one <see cref="SensorStatistics"/> object per sensor across
/// every consumer thread and guarded the compound (count + sum + min + max) update behind a
/// <see cref="System.Threading.Lock"/>. When many consumers hammered the <i>same</i> hot
/// sensor that single cache line ping-ponged between cores — a Read-For-Ownership (RFO) storm
/// — and every update serialized on the gate.</para>
///
/// <para><see cref="SensorAggregator"/> now gives <b>each consumer its own shard</b>: a private
/// <c>SensorStatistics[]</c> indexed by sensor id. A shard therefore has exactly one writer at
/// a time, so the compound invariant no longer needs a lock — there is no second thread to race
/// against — and the fields can be plain, non-atomic, single-writer values updated in place. The
/// only synchronization left is the once-every-flush fan-in (<see cref="SensorAggregator.CreateSnapshot"/>),
/// which sums and min/max-reduces the shards.</para>
///
/// <para>Being a <c>struct</c> stored <i>by value</i> in the shard array also restores genuine
/// data cache-locality on the hot path: <c>shard[id].Update(...)</c> mutates the bytes inline in
/// the contiguous array — no reference walk feeding scattered heap pointer-chases the way an array
/// of class references did. Adjacent sensors share a cache line, but because a shard is
/// single-writer that is harmless: false sharing only costs when <i>different cores</i> write the
/// same line, and here only one core ever writes a given shard.</para>
/// </summary>
public struct SensorStatistics
{
    // Plain single-writer fields. No Interlocked, no Lock: within a shard there is only ever one
    // writer, so a simple read-modify-write is correct and is the whole point of the redesign.
    public long Count;
    public double Sum;
    public double Min;
    public double Max;

    /// <summary>
    /// An empty accumulator with min/max seeded to the identities of their reductions
    /// (+∞ for min, −∞ for max) so the very first <see cref="Update"/> always wins both
    /// comparisons without a special-case branch. The array of these is bulk-initialized by
    /// <see cref="SensorAggregator"/> so the hot path never has to test "is this the first reading?".
    /// </summary>
    public static SensorStatistics CreateEmpty() => new()
    {
        Count = 0,
        Sum = 0.0,
        Min = double.PositiveInfinity,
        Max = double.NegativeInfinity,
    };

    /// <summary>
    /// Fold a single reading's value into the running totals. Lock-free and called only by the
    /// shard's owning consumer, so a plain read-modify-write of each field is data-race-free.
    /// Invoke through a <c>ref</c> to the array element (<c>ref var s = ref shard[id]; s.Update(v);</c>)
    /// so the mutation lands in the array, not on a copy.
    /// </summary>
    public void Update(double value)
    {
        Sum += value;
        if (value < Min) Min = value;
        if (value > Max) Max = value;
        System.Threading.Volatile.Write(ref Count, Count + 1);
    }
}
