using System.Buffers;
using System.Diagnostics;
using System.Threading.Channels;
using Telemetry.Engine.Domain;
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
    private readonly int _targetReadingsPerSecond;
    private readonly int _batchSize;
    private readonly int _sensorCount;

    public FirehoseGenerator(
        ChannelWriter<TelemetryBatch> writer,
        int targetReadingsPerSecond = 100_000,
        int batchSize = 1_000,
        int sensorCount = 64)
    {
        _writer = writer;
        _targetReadingsPerSecond = targetReadingsPerSecond;
        _batchSize = batchSize;
        _sensorCount = sensorCount;
    }

    /// <summary>Total readings emitted so far. Read across threads, so kept as a volatile-ish long via Interlocked at the call site.</summary>
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
                    // Ahead of schedule — yield briefly rather than busy-spin a core.
                    await Task.Delay(1, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                TelemetryBatch batch = BuildBatch(random);
                await _writer.WriteAsync(batch, cancellationToken).ConfigureAwait(false);
                TotalProduced += batch.ReadingCount;
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
        int byteLength = _batchSize * SensorReading.Size;
        byte[] buffer = ArrayPool<byte>.Shared.Rent(byteLength);
        Span<byte> destination = buffer.AsSpan(0, byteLength);

        long nowTicks = DateTime.UtcNow.Ticks;

        for (int i = 0; i < _batchSize; i++)
        {
            var reading = new SensorReading(
                SensorId: random.Next(_sensorCount),
                TimestampTicks: nowTicks,
                // A sine-ish + noise value so aggregated min/max/avg look alive.
                Value: 20f + (float)(random.NextDouble() * 60.0));

            // Stage the reading through an inline-array frame (zero heap allocation),
            // then copy the 16 bytes into the batch buffer. This exercises the
            // PayloadBuffer inline array on the real hot path.
            PayloadBuffer frame = TelemetryCodec.Encode(in reading);
            ((ReadOnlySpan<byte>)frame).CopyTo(destination.Slice(i * SensorReading.Size, SensorReading.Size));
        }

        return new TelemetryBatch(buffer, _batchSize);
    }
}
