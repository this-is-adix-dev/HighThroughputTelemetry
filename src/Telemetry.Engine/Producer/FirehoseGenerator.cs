using System.Buffers;
using System.Diagnostics;
using System.Threading.Channels;
using Telemetry.Engine.Domain;
using Telemetry.Engine.Observability;
using Telemetry.Engine.Parsing;

namespace Telemetry.Engine.Producer;

/// <summary>
/// Module A — the "firehose". Synthesizes a sustained stream of 16-byte sensor
/// frames at a target rate (default 100,000 readings/sec) and publishes them as
/// pooled <see cref="TelemetryBatch"/> envelopes onto a bounded channel.
///
/// Two modern primitives carry the design:
/// <list type="bullet">
///   <item><b><see cref="Channel{T}"/> (bounded)</b> — a lock-free MPMC handoff with
///   built-in back-pressure. When consumers fall behind, <see cref="ChannelWriter{T}.WriteAsync"/>
///   asynchronously parks the producer instead of letting the queue grow without
///   bound and blowing up memory.</item>
///   <item><b><see cref="ArrayPool{T}"/></b> — every batch buffer is rented, not
///   allocated, so a steady state of 100k readings/sec produces ~zero GC pressure.</item>
/// </list>
/// </summary>
public sealed class FirehoseGenerator
{
    private readonly ChannelWriter<TelemetryBatch> _writer;
    private readonly EngineMetrics _metrics;
    private readonly int _targetReadingsPerSecond;
    private readonly int _batchSize;
    private readonly int _sensorCount;

    public FirehoseGenerator(
        ChannelWriter<TelemetryBatch> writer,
        EngineMetrics metrics,
        int targetReadingsPerSecond = 100_000,
        int batchSize = 1_000,
        int sensorCount = 64)
    {
        _writer = writer;
        _metrics = metrics;
        _targetReadingsPerSecond = targetReadingsPerSecond;
        _batchSize = batchSize;
        _sensorCount = sensorCount;
    }

    /// <summary>
    /// Total readings emitted so far. Written only by the single producer thread;
    /// read only after <see cref="RunAsync"/> has completed (the <c>await</c> provides
    /// the memory barrier). No <see cref="System.Threading.Interlocked"/> needed.
    /// </summary>
    public long TotalProduced { get; private set; }

    /// <summary>
    /// Runs until <paramref name="cancellationToken"/> fires, then completes the
    /// channel writer so consumers drain cleanly and stop.
    /// </summary>
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        // A cheap, deterministic PRNG — we only need plausible noise, not crypto.
        var random = new Random(Seed: 1789);
        var stopwatch = Stopwatch.StartNew();

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                // Pace generation against the wall clock. We compute how many readings
                // *should* have been produced by now and only emit the deficit, so the
                // long-run average tracks the target rate regardless of scheduling jitter.
                long shouldHaveProduced = (long)(stopwatch.Elapsed.TotalSeconds * _targetReadingsPerSecond);
                long deficit = shouldHaveProduced - TotalProduced;

                if (deficit < _batchSize)
                {
                    // Ahead of schedule. The naive move here is `await Task.Delay(1, …)`,
                    // but the .NET timer queue can't resolve 1 ms on stock Windows — it
                    // rounds up to a ~15.6 ms tick, so the producer oversleeps and then
                    // emits a jittery burst to catch up. Instead we wait *precisely* until
                    // the next full batch is genuinely owed, which at 100k/sec is on the
                    // order of milliseconds. PreciseDelay sleeps coarsely for the bulk and
                    // spin-waits only the sub-tick tail, so pacing stays smooth.
                    double readingsUntilBatchDue = _batchSize - deficit;
                    TimeSpan untilDue = TimeSpan.FromSeconds(readingsUntilBatchDue / _targetReadingsPerSecond);
                    await PreciseDelay.WaitAsync(untilDue, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                TelemetryBatch batch = BuildBatch(random);
                await _writer.WriteAsync(batch, cancellationToken).ConfigureAwait(false);
                TotalProduced += batch.ReadingCount;

                // Record AFTER the handoff so the counter reflects readings that
                // actually entered the pipeline (back-pressure may have parked us
                // above). The tag-less Add(long) overload boxes nothing and rents no
                // tag array — this is a heap-free increment on the producer hot path.
                _metrics.ReadingsProduced.Add(batch.ReadingCount);
            }
        }
        catch (OperationCanceledException)
        {
            // Expected at end-of-simulation: fall through to graceful completion.
        }
        finally
        {
            // Signal "no more data". Consumers reading via ReadAllAsync() will finish
            // their current drain and then exit their await-foreach loop.
            _writer.TryComplete();
        }
    }

    /// <summary>
    /// Rent a buffer and fill it with <see cref="_batchSize"/> freshly generated frames.
    /// </summary>
    private TelemetryBatch BuildBatch(Random random)
    {
        // Each frame is now FrameSize (32) bytes — 16 of data plus a 16-byte signature —
        // so the batch buffer is twice the width it was before signing was introduced.
        int byteLength = _batchSize * TelemetryCodec.FrameSize;
        byte[] buffer = ArrayPool<byte>.Shared.Rent(byteLength);
        try
        {
            Span<byte> destination = buffer.AsSpan(0, byteLength);

            long nowTicks = DateTime.UtcNow.Ticks;

            for (int i = 0; i < _batchSize; i++)
            {
                var reading = new SensorReading(
                    SensorId: random.Next(_sensorCount),
                    TimestampTicks: nowTicks,
                    // A sine-ish + noise value so aggregated min/max/avg look alive.
                    Value: 20f + (float)(random.NextDouble() * 60.0));

                // Encode the data and append its truncated HMAC straight into this frame's
                // slot in the pooled buffer. EncodeFrame stages the data through the inline-
                // array PayloadBuffer internally, so that zero-allocation primitive still runs
                // on the real hot path; signing then adds the 16-byte signature in place.
                Span<byte> frame = destination.Slice(i * TelemetryCodec.FrameSize, TelemetryCodec.FrameSize);
                TelemetryCodec.EncodeFrame(in reading, frame);

                // Integrity demo: with a small probability, flip one random bit in the frame
                // AFTER it was signed — simulating an attacker (or line noise) tampering with
                // the packet in flight. The signature no longer matches the data, so the
                // consumer will detect and reject exactly these frames.
                if (random.NextDouble() < TamperProbabilityPerFrame)
                    CorruptRandomBit(frame, random);
            }

            return new TelemetryBatch(buffer, _batchSize);
        }
        catch
        {
            // Return the rented buffer before propagating — the caller's finally block
            // only calls batch.Return(), which never executes if we throw before returning
            // the TelemetryBatch. Without this guard the buffer leaks back to the pool.
            ArrayPool<byte>.Shared.Return(buffer);
            throw;
        }
    }

    /// <summary>
    /// Fraction of produced frames that get a single bit deliberately corrupted to
    /// exercise the tamper-detection path. 0.1% keeps the stream overwhelmingly valid
    /// while guaranteeing a steady trickle of rejections to observe.
    /// </summary>
    private const double TamperProbabilityPerFrame = 0.001;

    /// <summary>
    /// Flip a single random bit somewhere in the 32-byte frame. Because the HMAC was
    /// computed over the pristine bytes, ANY one-bit change makes verification fail
    /// downstream — which is precisely the integrity guarantee we are demonstrating.
    /// </summary>
    private static void CorruptRandomBit(Span<byte> frame, Random random)
    {
        int byteIndex = random.Next(frame.Length);
        int bitMask = 1 << random.Next(8);
        frame[byteIndex] ^= (byte)bitMask;
    }
}
