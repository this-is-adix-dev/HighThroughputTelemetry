using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using Telemetry.Engine.Domain;

namespace Telemetry.Engine.Parsing;

/// <summary>
/// Zero-allocation encoder/decoder + integrity layer for the wire frame.
///
/// The on-wire frame is a fixed <see cref="FrameSize"/> (32) bytes:
/// <code>
///   | 0                       15 | 16                      31 |
///   |        16-byte data        |    16-byte HMAC (trunc.)   |
///   | SensorId / Timestamp /Value |    truncated HMAC-SHA256   |
/// </code>
/// The first half is the plaintext reading (see <see cref="SensorReading"/>); the
/// second half is a truncated HMAC-SHA256 signature over that half, letting a consumer
/// prove the data was not altered in flight.
///
/// Every method works directly over spans and primitive intrinsics
/// (<see cref="BinaryPrimitives"/>) or the allocation-free one-shot crypto APIs, so
/// encoding, signing, decoding or verifying a frame touches the heap exactly zero
/// times. This is the heart of "Module B".
/// </summary>
public static class TelemetryCodec
{
    /// <summary>Length of the truncated HMAC signature carried on the wire, in bytes.</summary>
    public const int SignatureSize = 16;

    /// <summary>
    /// Total wire-frame size: the 16-byte data payload followed by the 16-byte truncated
    /// signature. The producer writes this many bytes per reading and the parser advances
    /// by exactly this stride.
    /// </summary>
    public const int FrameSize = SensorReading.Size + SignatureSize;

    /// <summary>
    /// Shared secret for the HMAC, exposed as a <see cref="ReadOnlySpan{T}"/> over a
    /// constant blob. The C# compiler lowers a <c>ReadOnlySpan&lt;byte&gt;</c> returned
    /// from a constant collection expression to a reference into the assembly's read-only
    /// data section, so there is no array object on the heap — reading the key allocates
    /// nothing and the bytes are immutable.
    ///
    /// SECURITY: this is a hard-coded DUMMY key for the simulation only. A production
    /// system MUST load the signing key from a secret manager / KMS / HSM at startup and
    /// never commit it to source control. Both the producer and the consumer reference
    /// this single property, so signing and verification can never drift out of sync.
    /// </summary>
    private static ReadOnlySpan<byte> SigningKey =>
    [
        0x48, 0x54, 0x54, 0x2D, 0x44, 0x45, 0x4D, 0x4F, 0x2D, 0x4B, 0x45, 0x59, 0x2D, 0x44, 0x55, 0x4D, // "HTT-DEMO-KEY-DUM"
        0x4D, 0x59, 0x2D, 0x33, 0x32, 0x2D, 0x42, 0x59, 0x54, 0x45, 0x53, 0x2D, 0x21, 0x21, 0x21, 0x21, // "MY-32-BYTES-!!!!"
    ];

    /// <summary>
    /// Decode a single frame's 16-byte data section into a <see cref="SensorReading"/>.
    /// Takes a <see cref="ReadOnlySpan{T}"/> so it can read from a pooled array, a network
    /// buffer, or a stack buffer without caring which.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SensorReading Decode(ReadOnlySpan<byte> source)
    {
        // A single bounds check up front; the slices below are then provably in range.
        if (source.Length < SensorReading.Size)
            ThrowTooShort(SensorReading.Size, source.Length);

        // BinaryPrimitives compiles down to a single unaligned load + bswap (on BE
        // hardware) per field — no allocation, no temporary objects.
        int sensorId = BinaryPrimitives.ReadInt32LittleEndian(source[SensorReading.SensorIdOffset..]);
        long ticks = BinaryPrimitives.ReadInt64LittleEndian(source[SensorReading.TimestampOffset..]);
        float value = BinaryPrimitives.ReadSingleLittleEndian(source[SensorReading.ValueOffset..]);

        return new SensorReading(sensorId, ticks, value);
    }

    /// <summary>
    /// Encode a reading straight into a caller-provided 16-byte data span (the hot path).
    /// The reading is passed by <c>in</c> to avoid copying the 16-byte struct.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Encode(in SensorReading reading, Span<byte> destination)
    {
        if (destination.Length < SensorReading.Size)
            ThrowTooShort(SensorReading.Size, destination.Length);

        BinaryPrimitives.WriteInt32LittleEndian(destination[SensorReading.SensorIdOffset..], reading.SensorId);
        BinaryPrimitives.WriteInt64LittleEndian(destination[SensorReading.TimestampOffset..], reading.TimestampTicks);
        BinaryPrimitives.WriteSingleLittleEndian(destination[SensorReading.ValueOffset..], reading.Value);
    }

    /// <summary>
    /// Encode a reading into a fresh stack-resident <see cref="PayloadBuffer"/> and return
    /// it <b>by value</b>. Demonstrates how an inline array doubles as a zero-allocation,
    /// escapable 16-byte data frame.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static PayloadBuffer Encode(in SensorReading reading)
    {
        PayloadBuffer buffer = default;
        // The inline-array value implicitly converts to Span<byte> here.
        Encode(in reading, buffer);
        return buffer;
    }

    /// <summary>
    /// Encode a reading <b>and</b> its signature into a full <see cref="FrameSize"/>-byte
    /// wire frame: the 16-byte data section followed by the 16-byte truncated HMAC. This
    /// is the single canonical "build a signed frame" path shared by the producer, the
    /// tests and the benchmarks, so what constitutes a valid frame is defined in exactly
    /// one place.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void EncodeFrame(in SensorReading reading, Span<byte> destination)
    {
        if (destination.Length < FrameSize)
            ThrowTooShort(FrameSize, destination.Length);

        // Stage the 16-byte data through the inline-array PayloadBuffer (zero heap), then
        // lay it into the frame's data section. Routing through the inline array keeps
        // that zero-allocation primitive exercised on the real hot path.
        Span<byte> dataSection = destination[..SensorReading.Size];
        PayloadBuffer payload = Encode(in reading);
        ((ReadOnlySpan<byte>)payload).CopyTo(dataSection);

        // Append the truncated HMAC computed over the data we just wrote.
        Sign(dataSection, destination[SensorReading.Size..]);
    }

    /// <summary>
    /// Compute the truncated HMAC-SHA256 of <paramref name="data"/> and write the leading
    /// <see cref="SignatureSize"/> bytes into <paramref name="destination"/>.
    /// </summary>
    /// <remarks>
    /// Uses the one-shot static
    /// <see cref="HMACSHA256.HashData(ReadOnlySpan{byte}, ReadOnlySpan{byte}, Span{byte})"/>
    /// overload, which hashes into a caller-provided buffer and so allocates nothing — no
    /// <c>HMACSHA256</c> instance and no result array. The full 32-byte digest lands in a
    /// <c>stackalloc</c> buffer and we copy only the leading 16 bytes; that truncation is
    /// what defines our wire signature.
    /// </remarks>
    [SkipLocalsInit] // HashData fills all 32 bytes before we read them, so zero-init is wasted work.
    public static void Sign(ReadOnlySpan<byte> data, Span<byte> destination)
    {
        Span<byte> fullDigest = stackalloc byte[HMACSHA256.HashSizeInBytes]; // 32 bytes, stack only
        HMACSHA256.HashData(SigningKey, data, fullDigest);
        fullDigest[..SignatureSize].CopyTo(destination);
    }

    /// <summary>
    /// Re-compute the truncated HMAC over <paramref name="data"/> and compare it against
    /// the <paramref name="signature"/> that arrived on the wire. Returns <c>true</c> only
    /// if they match.
    /// </summary>
    /// <remarks>
    /// The comparison uses <see cref="CryptographicOperations.FixedTimeEquals"/>, which
    /// always inspects every byte and so runs in time independent of where the first
    /// mismatch occurs. A naive <c>SequenceEqual</c> / <c>==</c> short-circuits on the
    /// first differing byte, leaking — through timing — how many leading bytes an attacker
    /// guessed correctly; over many probes that lets them forge a signature one byte at a
    /// time. Constant-time comparison closes that side channel. (It also safely returns
    /// <c>false</c> when the lengths differ.)
    /// </remarks>
    [SkipLocalsInit] // HashData fills all 32 bytes before we read them, so zero-init is wasted work.
    public static bool Verify(ReadOnlySpan<byte> data, ReadOnlySpan<byte> signature)
    {
        Span<byte> fullDigest = stackalloc byte[HMACSHA256.HashSizeInBytes];
        HMACSHA256.HashData(SigningKey, data, fullDigest);
        return CryptographicOperations.FixedTimeEquals(fullDigest[..SignatureSize], signature);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ThrowTooShort(int required, int actual) =>
        throw new ArgumentException(
            $"A telemetry operation requires {required} bytes but only {actual} were available.");
}
