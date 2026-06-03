using System.Diagnostics;
using BenchmarkDotNet.Attributes;
using Telemetry.Engine.Producer;

namespace Telemetry.Benchmarks;

/// <summary>
/// Documents <see cref="PreciseDelay"/> as a standalone primitive: how accurately each pacing
/// strategy lands on a requested sub-tick delay, and at what CPU cost.
///
/// <para><b>Accuracy</b> is what the <c>Mean</c> column shows directly: for a requested
/// <see cref="TargetMicroseconds"/> delay, how long the call <i>actually</i> took. The closer the
/// Mean is to the target, the more accurate the pacer.
/// <list type="bullet">
///   <item><b><see cref="TaskDelay_Coarse"/></b> — <see cref="Task.Delay(TimeSpan, CancellationToken)"/>
///   is bounded by the OS timer queue. It overshoots a sub-millisecond request up toward the platform
///   tick (~1–15 ms), so its Mean sits well above the target. CPU cost: ~zero — the thread is parked.</item>
///   <item><b><see cref="PreciseDelay_Hybrid"/></b> — coarse sleep for the bulk, then a
///   <see cref="System.Threading.SpinWait"/> tail. Its Mean lands within microseconds of the target.
///   CPU cost: the fine phase <i>busy-spins</i>, so for the spin tail wall-time ≈ CPU-time — up to
///   roughly one OS tick of a core burned per call. That asymmetry — accurate but CPU-hungry vs. free
///   but coarse — is exactly why the firehose pacer uses Task.Delay and keeps PreciseDelay here.</item>
/// </list></para>
///
/// <para>Run with:
/// <c>dotnet run -c Release --project benchmarks/Telemetry.Benchmarks -- --filter *PacingBenchmarks*</c></para>
/// </summary>
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class PacingBenchmarks
{
    /// <summary>
    /// Requested delay in microseconds. 250 µs and 1 ms are the firehose's real inter-batch ballpark;
    /// 4 ms is comfortably above the Linux hrtimer floor so the coarse phase actually engages.
    /// </summary>
    [Params(250, 1_000, 4_000)]
    public int TargetMicroseconds { get; set; }

    private TimeSpan _target;

    [GlobalSetup]
    public void Setup() => _target = TimeSpan.FromTicks(TargetMicroseconds * (TimeSpan.TicksPerMillisecond / 1000));

    /// <summary>Baseline: the cheap, coarse timer the production pacer actually uses.</summary>
    [Benchmark(Baseline = true)]
    public async Task<long> TaskDelay_Coarse()
    {
        long start = Stopwatch.GetTimestamp();
        await Task.Delay(_target, CancellationToken.None).ConfigureAwait(false);
        return Stopwatch.GetElapsedTime(start).Ticks;
    }

    /// <summary>The high-resolution hybrid primitive: accurate to microseconds, at the cost of a spin tail.</summary>
    [Benchmark]
    public async Task<long> PreciseDelay_Hybrid()
    {
        long start = Stopwatch.GetTimestamp();
        await PreciseDelay.WaitAsync(_target, CancellationToken.None).ConfigureAwait(false);
        return Stopwatch.GetElapsedTime(start).Ticks;
    }
}
