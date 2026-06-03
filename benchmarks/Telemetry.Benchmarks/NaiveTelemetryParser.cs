using Telemetry.Engine.Domain;

namespace Telemetry.Benchmarks;

/// <summary>
/// A deliberately naive, allocation-heavy parser used purely as a benchmark
/// baseline. It does everything the optimized path avoids:
/// <list type="bullet">
///   <item>copies each 16-byte frame into a freshly allocated temporary
///   <c>byte[]</c> before reading it;</item>
///   <item>boxes every decoded reading into a heap <see cref="object"/> (here a
///   reference-type wrapper) and collects them into a growing <see cref="List{T}"/>.</item>
/// </list>
/// This mirrors how a lot of "obvious" parsing code is written — and is exactly
/// what shows up as GC pressure under load.
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
            int baseOffset = i * SensorReading.Size;

            // Allocate a throwaway 16-byte array and copy the frame into it.
            var frame = new byte[SensorReading.Size];
            Array.Copy(buffer, baseOffset, frame, 0, SensorReading.Size);

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
