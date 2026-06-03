using System.Buffers;
using Telemetry.Engine.Parsing;

namespace Telemetry.Engine.Producer;

/// <summary>
/// A unit of work handed from the producer to a consumer: a pooled byte buffer
/// holding <see cref="ReadingCount"/> back-to-back 32-byte signed frames.
///
/// Batching is the single most important throughput decision in the pipeline.
/// Pushing one reading at a time through a <c>Channel</c> would pay the channel's
/// synchronization cost 100,000 times per second; batching amortizes it across a
/// thousand readings at a time.
///
/// The backing array is rented from <see cref="ArrayPool{T}.Shared"/>, so it
/// MUST be returned exactly once after consumption via <see cref="Return"/>.
/// Consume data through <see cref="Span"/> — never the raw array — to avoid
/// reading uninitialized bytes beyond <see cref="ByteLength"/>.
/// A <c>readonly struct</c> keeps the envelope itself allocation-free.
/// </summary>
public readonly struct TelemetryBatch
{
    /// <summary>
    /// The pooled backing array. May be larger than <see cref="ByteLength"/> — bytes beyond
    /// that boundary are uninitialized memory from a previous rental. Internal only; external
    /// callers must use <see cref="Span"/> to avoid silently reading stale pooled bytes.
    /// </summary>
    internal byte[] Buffer { get; }

    /// <summary>Number of readings actually present in this batch.</summary>
    public int ReadingCount { get; }

    public TelemetryBatch(byte[] buffer, int readingCount)
    {
        Buffer = buffer;
        ReadingCount = readingCount;
    }

    /// <summary>Total number of meaningful bytes (<see cref="ReadingCount"/> × 32).</summary>
    public int ByteLength => ReadingCount * TelemetryCodec.FrameSize;

    /// <summary>A read-only view over exactly the populated region of the buffer.</summary>
    public ReadOnlySpan<byte> Span => Buffer.AsSpan(0, ByteLength);

    /// <summary>Return the rented buffer to the shared pool. Call once, after parsing.</summary>
    public void Return() => ArrayPool<byte>.Shared.Return(Buffer);
}
