using System.Diagnostics;
using System.Runtime.InteropServices;

namespace Telemetry.Engine.Producer;

/// <summary>
/// A high-resolution replacement for <c>Task.Delay(1, …)</c> on hot pacing loops.
///
/// <para><b>The trap this exists to avoid:</b> the .NET timer queue — and therefore
/// <see cref="Task.Delay(TimeSpan, CancellationToken)"/> — is bounded by the operating
/// system's timer tick. On stock Windows that tick is ~15.6 ms, so a request to sleep
/// for 1 ms actually parks the thread for a full tick. In a 100k-readings/sec firehose,
/// pacing with <c>Task.Delay(1)</c> turns a smooth stream into ~15 ms bursts followed by
/// frantic catch-up — i.e. enormous jitter. (Linux high-resolution timers hide this, but
/// we want code that behaves identically across platforms and AOT targets.)</para>
///
/// <para><b>The fix — a two-phase hybrid wait:</b>
/// <list type="number">
///   <item><b>Coarse phase:</b> for the portion of the wait that is comfortably longer
///   than one OS timer tick, defer to <see cref="Task.Delay(TimeSpan, CancellationToken)"/>.
///   This frees the core back to the scheduler — no spinning — and cannot overshoot the
///   real deadline because we always leave a platform-calibrated spin margin.</item>
///   <item><b>Fine phase:</b> burn the remaining sub-tick tail with a
///   <see cref="SpinWait"/> measured against a <see cref="Stopwatch"/> timestamp, landing
///   on the deadline with microsecond accuracy.</item>
/// </list>
/// This is the <c>SpinWait</c>/precise-timer pattern you would reach for in a genuine
/// low-latency system, expressed as a reusable primitive instead of being smeared across
/// the producer loop.</para>
///
/// <para><b>Why it is no longer the demo producer's pacer.</b> The firehose downstream is a
/// bounded channel with <c>FullMode.Wait</c>, which already absorbs producer jitter, so the
/// sub-tick precision this delivers is accuracy that pipeline cannot consume — and the fine-phase
/// spin would burn a meaningful slice of a core to provide it. The producer therefore paces with a
/// plain <see cref="Task.Delay(TimeSpan, CancellationToken)"/>; this primitive is kept and measured
/// on its own (accuracy and CPU cost) in <c>Telemetry.Benchmarks</c> so its trade-off is documented
/// with numbers rather than applied where it is not warranted.</para>
/// </summary>
public static class PreciseDelay
{
    /// <summary>
    /// Conservative platform estimate of the minimum time <see cref="Task.Delay"/> may
    /// consume beyond its requested duration. The coarse phase is skipped entirely when
    /// the full delay is shorter than this — there is nothing to amortize.
    ///
    /// <list type="bullet">
    ///   <item>Windows scheduler tick: ~15.6 ms (timeBeginPeriod not assumed)</item>
    ///   <item>Linux hrtimer granularity: ~1–4 µs; 1.5 ms gives headroom for load spikes</item>
    /// </list>
    /// </summary>
    private static readonly TimeSpan OsTimerGranularity = TimeSpan.FromMilliseconds(
        RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? 15.6 : 1.5);

    /// <summary>
    /// Asynchronously waits for <paramref name="delay"/> with sub-millisecond precision.
    /// Returns immediately for non-positive delays. Honours <paramref name="cancellationToken"/>
    /// by throwing <see cref="OperationCanceledException"/>, matching <see cref="Task.Delay(TimeSpan, CancellationToken)"/>.
    /// </summary>
    public static async ValueTask WaitAsync(TimeSpan delay, CancellationToken cancellationToken)
    {
        if (delay <= TimeSpan.Zero)
            return;

        // Capture the start on the monotonic, high-resolution Stopwatch clock — never on
        // DateTime, which is itself only tick-granular and is wall-clock (NTP-adjustable).
        long startTimestamp = Stopwatch.GetTimestamp();

        // Phase 1 — coarse, thread-yielding sleep for the bulk of the wait.
        //
        // Spin margin = max(20% of delay, OS granularity floor). The two constraints:
        //   (a) ≥ OsTimerGranularity  — Task.Delay must never overshoot the real deadline;
        //       if the delay is shorter than one OS tick the coarse phase is skipped entirely.
        //   (b) ≥ delay/5             — for very long delays, keeps the spin tail proportional
        //       rather than a fixed absolute value that could drift under scheduling pressure.
        //
        // Before this fix, CoarseMargin was a hardcoded 16 ms. At 100k readings/sec with
        // BatchSize=1000 the inter-batch period is 10 ms < 16 ms, so the coarse phase never
        // fired and the producer busy-spun for the full 10 ms between every batch (~100% CPU).
        TimeSpan margin = TimeSpan.FromTicks(Math.Max(delay.Ticks / 5, OsTimerGranularity.Ticks));
        TimeSpan coarse = delay - margin;
        if (coarse > TimeSpan.Zero)
            await Task.Delay(coarse, cancellationToken).ConfigureAwait(false);

        // Phase 2 — fine, busy-spin until the true deadline.
        var spinner = new SpinWait();
        while (Stopwatch.GetElapsedTime(startTimestamp) < delay)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Once SpinWait decides its next iteration would yield, it escalates toward
            // Thread.Sleep(1) — which costs a full ~15.6 ms tick and would defeat the whole
            // purpose. We reset the spinner just before that point so we stay in cheap
            // PAUSE-instruction territory and keep our microsecond resolution.
            if (spinner.NextSpinWillYield)
                spinner = new SpinWait();

            spinner.SpinOnce();
        }
    }
}
