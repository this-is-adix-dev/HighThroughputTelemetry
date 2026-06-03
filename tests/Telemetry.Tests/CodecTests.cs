using Telemetry.Engine.Domain;
using Telemetry.Engine.Parsing;

namespace Telemetry.Tests;

public class CodecTests
{
    [Fact]
    public void Encode_Then_Decode_RoundTrips()
    {
        var original = new SensorReading(SensorId: 42, TimestampTicks: 638_000_000_000_000_000L, Value: 3.14159f);

        Span<byte> frame = stackalloc byte[SensorReading.Size];
        TelemetryCodec.Encode(in original, frame);
        SensorReading decoded = TelemetryCodec.Decode(frame);

        Assert.Equal(original, decoded);
    }

    [Fact]
    public void Encode_WritesExactly16Bytes_InLittleEndianLayout()
    {
        var reading = new SensorReading(SensorId: 1, TimestampTicks: 2, Value: 0f);

        Span<byte> frame = stackalloc byte[SensorReading.Size];
        TelemetryCodec.Encode(in reading, frame);

        // SensorId == 1 little-endian -> first byte is 0x01, rest of the int zero.
        Assert.Equal(0x01, frame[SensorReading.SensorIdOffset]);
        Assert.Equal(0x00, frame[SensorReading.SensorIdOffset + 1]);
        // TimestampTicks == 2 little-endian -> first byte of the long is 0x02.
        Assert.Equal(0x02, frame[SensorReading.TimestampOffset]);
    }

    [Fact]
    public void InlineArrayEncode_ProducesSameBytes_AsSpanEncode()
    {
        var reading = new SensorReading(SensorId: 7, TimestampTicks: 99, Value: 12.5f);

        PayloadBuffer inlineFrame = TelemetryCodec.Encode(in reading);

        Span<byte> spanFrame = stackalloc byte[SensorReading.Size];
        TelemetryCodec.Encode(in reading, spanFrame);

        Assert.True(((ReadOnlySpan<byte>)inlineFrame).SequenceEqual(spanFrame));
    }

    [Fact]
    public void Decode_TooShortBuffer_Throws()
    {
        byte[] tooShort = new byte[SensorReading.Size - 1];
        Assert.Throws<ArgumentException>(() => TelemetryCodec.Decode(tooShort));
    }

    [Fact]
    public void EncodeFrame_ProducesAVerifiableFrame_ThatDecodesToOriginal()
    {
        var reading = new SensorReading(SensorId: 5, TimestampTicks: 123_456_789L, Value: 2.71828f);

        Span<byte> frame = stackalloc byte[TelemetryCodec.FrameSize];
        TelemetryCodec.EncodeFrame(in reading, frame);

        ReadOnlySpan<byte> data = frame[..SensorReading.Size];
        ReadOnlySpan<byte> signature = frame[SensorReading.Size..];

        Assert.True(TelemetryCodec.Verify(data, signature));
        Assert.Equal(reading, TelemetryCodec.Decode(data));
    }

    [Fact]
    public void Sign_Then_Verify_RoundTrips()
    {
        Span<byte> data = stackalloc byte[SensorReading.Size];
        TelemetryCodec.Encode(new SensorReading(22, 11, 3.5f), data);

        Span<byte> signature = stackalloc byte[TelemetryCodec.SignatureSize];
        TelemetryCodec.Sign(data, signature);

        Assert.True(TelemetryCodec.Verify(data, signature));
    }

    [Fact]
    public void Sign_IsDeterministic_ForTheSameData()
    {
        Span<byte> data = stackalloc byte[SensorReading.Size];
        TelemetryCodec.Encode(new SensorReading(2, 1, 3f), data);

        Span<byte> a = stackalloc byte[TelemetryCodec.SignatureSize];
        Span<byte> b = stackalloc byte[TelemetryCodec.SignatureSize];
        TelemetryCodec.Sign(data, a);
        TelemetryCodec.Sign(data, b);

        Assert.True(a.SequenceEqual(b));
    }

    [Fact]
    public void Verify_Fails_WhenDataIsTampered()
    {
        Span<byte> data = stackalloc byte[SensorReading.Size];
        TelemetryCodec.Encode(new SensorReading(8, 7, 9f), data);

        Span<byte> signature = stackalloc byte[TelemetryCodec.SignatureSize];
        TelemetryCodec.Sign(data, signature);

        // Flip one data bit after signing: verification must now fail.
        data[0] ^= 0x01;
        Assert.False(TelemetryCodec.Verify(data, signature));
    }

    [Fact]
    public void Verify_Fails_WhenSignatureIsTampered()
    {
        Span<byte> data = stackalloc byte[SensorReading.Size];
        TelemetryCodec.Encode(new SensorReading(8, 7, 9f), data);

        Span<byte> signature = stackalloc byte[TelemetryCodec.SignatureSize];
        TelemetryCodec.Sign(data, signature);

        signature[^1] ^= 0x80;
        Assert.False(TelemetryCodec.Verify(data, signature));
    }
}
