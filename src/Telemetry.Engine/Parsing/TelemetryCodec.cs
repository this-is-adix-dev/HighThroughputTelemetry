using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using Telemetry.Engine.Domain;

namespace Telemetry.Engine.Parsing;

/// <summary>
/// Zero-allocation encoder/decoder for the 16-byte wire frame.
///
/// Every method works directly over spans and primitive intrinsics
/// (<see cref="BinaryPrimitives"/>), so encoding or decoding a reading touches
/// the heap exactly zero times. This is the heart of "Module B".
/// </summary>
public static class TelemetryCodec
{
    /// <summary>
    /// Decode a single 16-byte frame into a <see cref="SensorReading"/>.
    /// Takes a <see cref="ReadOnlySpan{T}"/> so it can read from a pooled array,
    /// a network buffer, or a stack buffer without caring which.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SensorReading Decode(ReadOnlySpan<byte> source)
    {
        // A single bounds check up front; the slices below are then provably in range.
        if (source.Length < SensorReading.Size)
            ThrowTooShort(source.Length);

        // BinaryPrimitives compiles down to a single unaligned load + bswap (on BE
        // hardware) per field — no allocation, no temporary objects.
        int sensorId = BinaryPrimitives.ReadInt32LittleEndian(source[SensorReading.SensorIdOffset..]);
        long ticks = BinaryPrimitives.ReadInt64LittleEndian(source[SensorReading.TimestampOffset..]);
        float value = BinaryPrimitives.ReadSingleLittleEndian(source[SensorReading.ValueOffset..]);

        return new SensorReading(sensorId, ticks, value);
    }

    /// <summary>
    /// Encode a reading straight into a caller-provided destination span (the hot path).
    /// The reading is passed by <c>in</c> to avoid copying the 16-byte struct.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Encode(in SensorReading reading, Span<byte> destination)
    {
        if (destination.Length < SensorReading.Size)
            ThrowTooShort(destination.Length);

        BinaryPrimitives.WriteInt32LittleEndian(destination[SensorReading.SensorIdOffset..], reading.SensorId);
        BinaryPrimitives.WriteInt64LittleEndian(destination[SensorReading.TimestampOffset..], reading.TimestampTicks);
        BinaryPrimitives.WriteSingleLittleEndian(destination[SensorReading.ValueOffset..], reading.Value);
    }

    /// <summary>
    /// Encode a reading into a fresh stack-resident <see cref="PayloadBuffer"/> and
    /// return it <b>by value</b>. Demonstrates how an inline array doubles as a
    /// zero-allocation, escapable 16-byte frame.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static PayloadBuffer Encode(in SensorReading reading)
    {
        PayloadBuffer buffer = default;
        // The inline-array value implicitly converts to Span<byte> here.
        Encode(in reading, buffer);
        return buffer;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ThrowTooShort(int actual) =>
        throw new ArgumentException(
            $"A telemetry frame requires {SensorReading.Size} bytes but only {actual} were available.");
}
