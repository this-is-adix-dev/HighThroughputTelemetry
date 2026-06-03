using Telemetry.Engine.Aggregation;
using Telemetry.Engine.Domain;
using Telemetry.Engine.Parsing;
using Telemetry.Engine.Producer;

namespace Telemetry.Tests;

/// <summary>
/// Guards the producer/consumer boundary: a <see cref="TelemetryBatch"/> must expose
/// the <b>whole</b> populated region (32 bytes per frame). An off-by-frame-width here
/// would silently let consumers parse only part of every batch.
/// </summary>
public class TelemetryBatchTests
{
    [Fact]
    public void Span_CoversEveryFullSignedFrame_AndIngestsCompletely()
    {
        const int count = 10;
        var buffer = new byte[count * TelemetryCodec.FrameSize];
        for (int i = 0; i < count; i++)
        {
            var reading = new SensorReading(SensorId: i, TimestampTicks: i, Value: i);
            TelemetryCodec.EncodeFrame(in reading, buffer.AsSpan(i * TelemetryCodec.FrameSize, TelemetryCodec.FrameSize));
        }

        var batch = new TelemetryBatch(buffer, count);

        Assert.Equal(count * TelemetryCodec.FrameSize, batch.ByteLength);
        Assert.Equal(count, batch.Span.Length / TelemetryCodec.FrameSize);

        // Every frame the batch exposes must be authentic and decode cleanly.
        var aggregator = new SensorAggregator();
        Assert.Equal(count, aggregator.IngestBatch(batch.Span));
    }
}
