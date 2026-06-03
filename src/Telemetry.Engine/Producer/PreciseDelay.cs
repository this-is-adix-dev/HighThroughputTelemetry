using System.Diagnostics;

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
///   than one timer tick, defer to <see cref="Task.Delay(TimeSpan, CancellationToken)"/>.
///   This frees the core back to the scheduler — no spinning — and cannot overshoot the
///   real deadline because we always leave a tick-sized safety margin.</item>
///   <item><b>Fine phase:</b> burn the remaining sub-tick margin with a
///   <see cref="SpinWait"/> measured against a <see cref="Stopwatch"/> timestamp, landing
///   on the deadline with microsecond accuracy.</item>
/// </list>
/// This is the <c>SpinWait</c>/precise-timer pattern you would reach for in a genuine
/// low-latency system, expressed as a reusable primitive instead of being smeared across
/// the producer loop.</para>
/// </summary>
internal static class PreciseDelay
{
    /// <summary>
    /// Largest wait we are willing to satisfy by busy-spinning. It is sized one full
    /// timer tick above the worst-case OS granularity (~15.6 ms): the coarse phase may
    /// overshoot its target by up to one tick, so the spin margin must be at least that
    /// wide for the deadline to always fall inside the fine phase rather than be missed.
    /// </summary>
    private static readonly TimeSpan CoarseMargin = TimeSpan.FromMilliseconds(16);

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

        // Phase 1 — coarse, thread-yielding sleep for everything beyond the spin margin.
        TimeSpan coarse = delay - CoarseMargin;
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
