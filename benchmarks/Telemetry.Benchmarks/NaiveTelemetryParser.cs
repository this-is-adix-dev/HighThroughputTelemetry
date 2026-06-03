using Telemetry.Engine.Domain;
using Telemetry.Engine.Parsing;

namespace Telemetry.Benchmarks;

/// <summary>
/// A deliberately naive, allocation-heavy parser used purely as a benchmark
/// baseline. It performs the <i>same</i> logical work as the optimized
/// <see cref="TelemetryParser"/> — decode <b>plus</b> per-frame HMAC verification — but
/// written the allocation-heavy way "obvious" parsing code usually is:
/// <list type="bullet">
///   <item>copies each frame's 16-byte data half and 16-byte signature half into two
///   freshly allocated temporary <c>byte[]</c>s before touching them;</item>
///   <item>verifies the HMAC over those throwaway arrays via the same constant-time
///   <see cref="TelemetryCodec.Verify"/> the hot path uses;</item>
///   <item>boxes every decoded reading into a heap <see cref="object"/> (here a
///   reference-type wrapper) and collects them into a growing <see cref="List{T}"/>.</item>
/// </list>
/// Holding the work identical to the optimized path isolates the single variable the
/// benchmark is about — allocation strategy — which is exactly what shows up as GC
/// pressure under load.
/// </summary>
public static class NaiveTelemetryParser
{
    /// <summary>Reference-type reading — one heap allocation per parsed frame.</summary>
    public sealed class HeapReading
    {
        public int SensorId { get; init; }
        public long TimestampTicks { get; init; }
        public float Value { get; init; }
    }

    public static List<HeapReading> Parse(byte[] buffer, int readingCount)
    {
        var results = new List<HeapReading>(); // grows + reallocates as it fills

        for (int i = 0; i < readingCount; i++)
        {
            // Frames are 32 bytes (16 data + 16 signature). The baseline strides by the
            // full frame width to find each one.
            int baseOffset = i * TelemetryCodec.FrameSize;

            // Allocate a throwaway 16-byte array and copy the data section into it.
            var frame = new byte[SensorReading.Size];
            Array.Copy(buffer, baseOffset, frame, 0, SensorReading.Size);

            // Allocate a *second* throwaway array for the signature half and copy it out
            // too, so the baseline runs the exact same integrity check as the hot path —
            // only over freshly allocated garbage instead of in place over a span.
            var signature = new byte[TelemetryCodec.SignatureSize];
            Array.Copy(buffer, baseOffset + SensorReading.Size, signature, 0, TelemetryCodec.SignatureSize);

            // Same constant-time HMAC check the real parser performs; a frame that fails
            // verification is dropped and skipped, matching TelemetryParser's semantics.
            if (!TelemetryCodec.Verify(frame, signature))
                continue;

            // BitConverter over freshly sliced arrays — more copies, more garbage.
            int sensorId = BitConverter.ToInt32(frame, SensorReading.SensorIdOffset);
            long ticks = BitConverter.ToInt64(frame, SensorReading.TimestampOffset);
            float value = BitConverter.ToSingle(frame, SensorReading.ValueOffset);

            results.Add(new HeapReading
            {
                SensorId = sensorId,
                TimestampTicks = ticks,
                Value = value,
            });
        }

        return results;
    }
}
