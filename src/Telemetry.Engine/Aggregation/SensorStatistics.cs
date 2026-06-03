using Telemetry.Engine.Domain;

namespace Telemetry.Engine.Aggregation;

/// <summary>
/// Mutable running statistics for a single sensor (count, sum, min, max).
///
/// Several consumer threads may update the <i>same</i> sensor concurrently, and an
/// update touches four fields that must move together (otherwise a snapshot could
/// observe a new <c>Max</c> with a stale <c>Count</c>). That compound invariant is
/// why this uses a tiny critical section rather than a pile of <c>Interlocked</c>
/// calls.
///
/// The lock is the new .NET 9 <see cref="System.Threading.Lock"/> type — a
/// purpose-built mutual-exclusion primitive that is faster than
/// <c>lock(object)</c> (no syncblock indirection) and whose <c>EnterScope()</c>
/// returns a <c>ref struct</c> we dispose with a <c>using</c>, so the lock can
/// never accidentally outlive its scope or be boxed.
/// </summary>
public sealed class SensorStatistics
{
    private readonly Lock _gate = new();

    private long _count;
    private double _sum;
    private double _min = double.PositiveInfinity;
    private double _max = double.NegativeInfinity;

    /// <summary>Fold a single reading into the running totals.</summary>
    public void Update(in SensorReading reading)
    {
        double value = reading.Value;

        // EnterScope() returns a ref-struct scope; `using` guarantees release even
        // if the body throws. The section is a handful of comparisons — nanoseconds.
        using (_gate.EnterScope())
        {
            _count++;
            _sum += value;
            if (value < _min) _min = value;
            if (value > _max) _max = value;
        }
    }

    /// <summary>Atomically copy the current state into an immutable snapshot.</summary>
    public SensorSnapshot Snapshot(int sensorId)
    {
        using (_gate.EnterScope())
        {
            double average = _count == 0 ? 0.0 : _sum / _count;
            double min = _count == 0 ? 0.0 : _min;
            double max = _count == 0 ? 0.0 : _max;
            return new SensorSnapshot(sensorId, _count, min, max, average);
        }
    }
}
