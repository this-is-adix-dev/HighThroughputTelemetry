using System.Threading;

namespace Telemetry.Benchmarks;

/// <summary>
/// A faithful reproduction of the <i>previous</i> aggregator hot-path design: a single
/// <see cref="SensorStatistics"/>-style object, shared by every consumer, whose compound
/// (count + sum + min + max) update is guarded by a per-sensor <see cref="Lock"/>.
///
/// It is kept in the benchmark project — not in the engine — purely as the "before" baseline for
/// <see cref="ContentionBenchmarks"/>. When N threads all update the <i>same</i> hot sensor, every
/// <see cref="Update"/> serializes on <see cref="_gate"/> and dirties the one shared cache line, so
/// the throughput collapses as cores are added (the Read-For-Ownership storm the redesign removes).
/// </summary>
public sealed class LockedSensorStatistics
{
    private readonly Lock _gate = new();

    private long _count;
    private double _sum;
    private double _min = double.PositiveInfinity;
    private double _max = double.NegativeInfinity;

    public void Update(double value)
    {
        using (_gate.EnterScope())
        {
            _count++;
            _sum += value;
            if (value < _min) _min = value;
            if (value > _max) _max = value;
        }
    }

    public long Count
    {
        get
        {
            using (_gate.EnterScope())
                return _count;
        }
    }
}
