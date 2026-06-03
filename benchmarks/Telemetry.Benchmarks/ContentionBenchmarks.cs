using BenchmarkDotNet.Attributes;
using Telemetry.Engine.Aggregation;
using Telemetry.Engine.Domain;

namespace Telemetry.Benchmarks;

/// <summary>
/// The headline measurement for Red-Flag-B: per-object lock contention vs. per-consumer sharding.
///
/// <para>Every benchmark spins up <see cref="ThreadCount"/> threads that all hammer the <b>same hot
/// sensor</b> (id 0) with a fixed <see cref="UpdatesPerThread"/> updates each — the worst case for a
/// shared accumulator. Because the per-thread work is held constant, the total work grows with the
/// thread count, so the shapes tell the whole story:</para>
/// <list type="bullet">
///   <item><b><see cref="SharedLocked"/></b> — one shared object behind a <see cref="System.Threading.Lock"/>.
///   All updates serialize on the gate and the single cache line ping-pongs between cores, so wall time
///   climbs roughly <i>linearly</i> with threads: the throughput cliff.</item>
///   <item><b><see cref="PerConsumerSharded"/></b> — the real <see cref="SensorAggregator"/> with one
///   shard per thread. Updates are uncontended and lock-free, so adding cores barely moves wall time:
///   the cliff flattens out.</item>
/// </list>
///
/// <para>The ratio column (sharded vs. locked baseline) is the number that matters: it should widen
/// dramatically as <see cref="ThreadCount"/> rises. Run with:
/// <c>dotnet run -c Release --project benchmarks/Telemetry.Benchmarks -- --filter *ContentionBenchmarks*</c></para>
/// </summary>
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class ContentionBenchmarks
{
    /// <summary>Threads all updating the single hot sensor concurrently. 1 is the uncontended reference point.</summary>
    [Params(1, 2, 4, 8, 16)]
    public int ThreadCount { get; set; }

    /// <summary>
    /// Updates each thread performs. Fixed (not divided among threads) so per-thread work is constant
    /// and the locked baseline's serialization shows up directly as wall time growing with thread count.
    /// Large enough that thread start/join overhead is negligible against the measured region.
    /// </summary>
    private const int UpdatesPerThread = 2_000_000;

    // The single hot reading every thread folds in. Sensor 0 is the contention point.
    private static readonly SensorReading HotReading = new(SensorId: 0, TimestampTicks: 0, Value: 42.0f);

    /// <summary>Baseline: one shared locked accumulator — the design the refactor replaced.</summary>
    [Benchmark(Baseline = true)]
    public long SharedLocked()
    {
        var shared = new LockedSensorStatistics();
        RunOnThreads(ThreadCount, _ =>
        {
            for (int i = 0; i < UpdatesPerThread; i++)
                shared.Update(HotReading.Value);
        });
        return shared.Count;
    }

    /// <summary>Optimized: the sharded aggregator — each thread owns its own shard, fully lock-free.</summary>
    [Benchmark]
    public long PerConsumerSharded()
    {
        // sensorCount 1 keeps the comparison honest: both designs maintain exactly one sensor's stats.
        var aggregator = new SensorAggregator(sensorCount: 1, shardCount: ThreadCount);
        RunOnThreads(ThreadCount, shard =>
        {
            for (int i = 0; i < UpdatesPerThread; i++)
                aggregator.Update(shard, in HotReading);
        });
        // Fan-in once, exactly as the sink would at flush time — included so the merge cost is visible.
        return aggregator.CreateSnapshot()[0].Count;
    }

    /// <summary>
    /// Launch <paramref name="threadCount"/> dedicated threads, hand each its index, and join them all.
    /// Dedicated <see cref="Thread"/>s (not the thread pool) guarantee true parallelism for the
    /// contention measurement instead of leaving it to pool heuristics.
    /// </summary>
    private static void RunOnThreads(int threadCount, Action<int> body)
    {
        var threads = new Thread[threadCount];
        for (int t = 0; t < threadCount; t++)
        {
            int index = t; // capture a copy per thread
            threads[t] = new Thread(() => body(index)) { IsBackground = true };
        }

        foreach (Thread thread in threads)
            thread.Start();
        foreach (Thread thread in threads)
            thread.Join();
    }
}
