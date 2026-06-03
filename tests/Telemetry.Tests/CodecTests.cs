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
}
