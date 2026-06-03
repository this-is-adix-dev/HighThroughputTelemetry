using System.Buffers;
using Telemetry.Engine.Domain;

namespace Telemetry.Engine.Producer;

/// <summary>
/// A unit of work handed from the producer to a consumer: a pooled byte buffer
/// holding <see cref="ReadingCount"/> back-to-back 16-byte frames.
///
/// Batching is the single most important throughput decision in the pipeline.
/// Pushing one reading at a time through a <c>Channel</c> would pay the channel's
/// synchronization cost 100,000 times per second; batching amortizes it across a
/// thousand readings at a time.
///
/// The <see cref="Buffer"/> is rented from <see cref="ArrayPool{T}.Shared"/>, so it
/// MUST be returned exactly once after consumption via <see cref="Return"/>.
/// A <c>readonly struct</c> keeps the envelope itself allocation-free.
/// </summary>
public readonly struct TelemetryBatch
{
    /// <summary>The pooled backing array. May be larger than <see cref="ByteLength"/>.</summary>
    public byte[] Buffer { get; }

    /// <summary>Number of readings actually present in this batch.</summary>
    public int ReadingCount { get; }

    public TelemetryBatch(byte[] buffer, int readingCount)
    {
        Buffer = buffer;
        ReadingCount = readingCount;
    }

    /// <summary>Total number of meaningful bytes (<see cref="ReadingCount"/> × 16).</summary>
    public int ByteLength => ReadingCount * SensorReading.Size;

    /// <summary>A read-only view over exactly the populated region of the buffer.</summary>
    public ReadOnlySpan<byte> Span => Buffer.AsSpan(0, ByteLength);

    /// <summary>Return the rented buffer to the shared pool. Call once, after parsing.</summary>
    public void Return() => ArrayPool<byte>.Shared.Return(Buffer);
}
