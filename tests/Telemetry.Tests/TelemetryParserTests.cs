using Telemetry.Engine.Domain;
using Telemetry.Engine.Parsing;

namespace Telemetry.Tests;

public class TelemetryParserTests
{
    private static byte[] BuildBuffer(params SensorReading[] readings)
    {
        var buffer = new byte[readings.Length * SensorReading.Size];
        for (int i = 0; i < readings.Length; i++)
            TelemetryCodec.Encode(in readings[i], buffer.AsSpan(i * SensorReading.Size, SensorReading.Size));
        return buffer;
    }

    [Fact]
    public void TryReadNext_DecodesEveryFrame_InOrder()
    {
        var expected = new[]
        {
            new SensorReading(1, 100, 1.0f),
            new SensorReading(2, 200, 2.0f),
            new SensorReading(3, 300, 3.0f),
        };
        byte[] buffer = BuildBuffer(expected);

        var parser = new TelemetryParser(buffer);
        var actual = new List<SensorReading>();
        while (parser.TryReadNext(out SensorReading reading))
            actual.Add(reading);

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Remaining_ReflectsWholeFramesLeft()
    {
        byte[] buffer = BuildBuffer(new SensorReading(1, 1, 1f), new SensorReading(2, 2, 2f));
        var parser = new TelemetryParser(buffer);

        Assert.Equal(2, parser.Remaining);
        parser.TryReadNext(out _);
        Assert.Equal(1, parser.Remaining);
    }

    [Fact]
    public void TryReadNext_IgnoresTrailingPartialFrame()
    {
        // 1 full frame + 5 dangling bytes that do not make a 16-byte frame.
        byte[] buffer = new byte[SensorReading.Size + 5];
        var reading = new SensorReading(9, 9, 9f);
        TelemetryCodec.Encode(in reading, buffer.AsSpan(0, SensorReading.Size));

        var parser = new TelemetryParser(buffer);
        Assert.True(parser.TryReadNext(out SensorReading first));
        Assert.Equal(reading, first);
        Assert.False(parser.TryReadNext(out _)); // partial tail is not yielded
    }

    [Fact]
    public void EmptyBuffer_YieldsNothing()
    {
        var parser = new TelemetryParser(ReadOnlySpan<byte>.Empty);
        Assert.False(parser.TryReadNext(out _));
    }
}
