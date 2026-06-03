using System.Diagnostics.Metrics;
using Telemetry.Engine.Domain;
using Telemetry.Engine.Observability;
using Telemetry.Engine.Parsing;

namespace Telemetry.Tests;

public class TelemetryParserTests
{
    // Build a buffer of fully signed 32-byte frames via the canonical codec path.
    private static byte[] BuildBuffer(params SensorReading[] readings)
    {
        var buffer = new byte[readings.Length * TelemetryCodec.FrameSize];
        for (int i = 0; i < readings.Length; i++)
            TelemetryCodec.EncodeFrame(in readings[i], buffer.AsSpan(i * TelemetryCodec.FrameSize, TelemetryCodec.FrameSize));
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
        // 1 full 32-byte frame + 5 dangling bytes that do not make a whole frame.
        byte[] buffer = new byte[TelemetryCodec.FrameSize + 5];
        var reading = new SensorReading(9, 9, 9f);
        TelemetryCodec.EncodeFrame(in reading, buffer.AsSpan(0, TelemetryCodec.FrameSize));

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

    [Fact]
    public void TryReadNext_SkipsTamperedFrame_AndContinuesToNext()
    {
        var readings = new[]
        {
            new SensorReading(1, 100, 1.0f),
            new SensorReading(2, 200, 2.0f), // this one gets corrupted
            new SensorReading(3, 300, 3.0f),
        };
        byte[] buffer = BuildBuffer(readings);

        // Flip a bit in the middle frame's data section so its HMAC no longer matches.
        buffer[TelemetryCodec.FrameSize + 1] ^= 0b0000_0100;

        var parser = new TelemetryParser(buffer);
        var actual = new List<SensorReading>();
        while (parser.TryReadNext(out SensorReading reading))
            actual.Add(reading);

        // The tampered frame is dropped; the two authentic frames still come through.
        Assert.Equal(new[] { readings[0], readings[2] }, actual);
    }

    [Fact]
    public void TryReadNext_IncrementsRejectedCounter_OnTamperedFrame()
    {
        byte[] buffer = BuildBuffer(
            new SensorReading(1, 100, 1.0f),
            new SensorReading(2, 200, 2.0f));

        // Corrupt the second frame's signature section (its very last byte).
        buffer[(2 * TelemetryCodec.FrameSize) - 1] ^= 0xFF;

        using var metrics = new EngineMetrics();
        long rejected = 0;
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == EngineMetrics.MeterName &&
                    instrument.Name == EngineMetrics.RejectedTamperedName)
                    l.EnableMeasurementEvents(instrument);
            },
        };
        listener.SetMeasurementEventCallback<long>((_, measurement, _, _) => rejected += measurement);
        listener.Start();

        var parser = new TelemetryParser(buffer, metrics.RejectedTampered);
        int read = 0;
        while (parser.TryReadNext(out _))
            read++;

        Assert.Equal(1, read);      // only the authentic frame is yielded
        Assert.Equal(1, rejected);  // exactly one rejection counted
    }
}
