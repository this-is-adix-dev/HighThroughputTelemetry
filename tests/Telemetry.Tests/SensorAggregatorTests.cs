using Telemetry.Engine.Aggregation;
using Telemetry.Engine.Domain;
using Telemetry.Engine.Parsing;

namespace Telemetry.Tests;

public class SensorAggregatorTests
{
    [Fact]
    public void Update_ComputesMinMaxAverage()
    {
        var aggregator = new SensorAggregator();

        foreach (float v in new[] { 10f, 20f, 30f })
            aggregator.Update(new SensorReading(SensorId: 1, TimestampTicks: 0, Value: v));

        ReadOnlySpan<SensorSnapshot> snapshotSpan = aggregator.CreateSnapshot();
        Assert.Equal(1, snapshotSpan.Length);
        SensorSnapshot snapshot = snapshotSpan[0];
        Assert.Equal(1, snapshot.SensorId);
        Assert.Equal(3, snapshot.Count);
        Assert.Equal(10.0, snapshot.Min, precision: 5);
        Assert.Equal(30.0, snapshot.Max, precision: 5);
        Assert.Equal(20.0, snapshot.Average, precision: 5);

        // Only sensor 1 was ever touched, so observed cardinality is 1 even though the
        // configured domain is the default 64. This is the exact distinction the pipeline
        // reports as DistinctSensors.
        Assert.Equal(1, aggregator.ActiveSensorCount);
        Assert.Equal(64, aggregator.SensorCount);
    }

    [Fact]
    public void IngestBatch_FoldsEveryFrame()
    {
        const int count = 50;
        var buffer = new byte[count * TelemetryCodec.FrameSize];
        for (int i = 0; i < count; i++)
        {
            var reading = new SensorReading(SensorId: i % 5, TimestampTicks: i, Value: i);
            TelemetryCodec.EncodeFrame(in reading, buffer.AsSpan(i * TelemetryCodec.FrameSize, TelemetryCodec.FrameSize));
        }

        // sensorCount: 5 matches the i % 5 domain above, so SensorCount is exact.
        var aggregator = new SensorAggregator(sensorCount: 5);
        int ingested = aggregator.IngestBatch(buffer);

        Assert.Equal(count, ingested);
        Assert.Equal(count, aggregator.TotalProcessed);
        Assert.Equal(5, aggregator.SensorCount);
    }

    [Fact]
    public async Task Update_IsThreadSafe_UnderConcurrency()
    {
        var aggregator = new SensorAggregator();
        const int threads = 8;
        const int perThread = 25_000;

        // Hammer the same sensors from many threads; totals must be exact.
        await Task.WhenAll(Enumerable.Range(0, threads).Select(t => Task.Run(() =>
        {
            for (int i = 0; i < perThread; i++)
                aggregator.Update(new SensorReading(SensorId: i % 16, TimestampTicks: 0, Value: 1f));
        })));

        // TotalProcessed is only incremented via IngestBatch (batched Interlocked.Add),
        // not via direct Update calls. Verify correctness via per-sensor counts instead.
        long summedCounts = aggregator.CreateSnapshot().ToArray().Sum(s => s.Count);
        Assert.Equal((long)threads * perThread, summedCounts);
    }

    [Fact]
    public async Task IngestBatch_IsThreadSafe_TotalProcessedIsExact()
    {
        const int sensorCount = 16;
        const int framesPerBatch = 1_000;
        const int threads = 8;

        // One fully-signed batch that every thread will ingest concurrently.
        var buffer = new byte[framesPerBatch * TelemetryCodec.FrameSize];
        for (int i = 0; i < framesPerBatch; i++)
        {
            var reading = new SensorReading(SensorId: i % sensorCount, TimestampTicks: i, Value: i);
            TelemetryCodec.EncodeFrame(in reading, buffer.AsSpan(i * TelemetryCodec.FrameSize, TelemetryCodec.FrameSize));
        }

        var aggregator = new SensorAggregator(sensorCount);

        // Fan the same batch through IngestBatch from many threads at once. The batched
        // Interlocked.Add inside IngestBatch is what must keep TotalProcessed exact under
        // contention — a plain non-atomic accumulation would lose updates here.
        await Task.WhenAll(Enumerable.Range(0, threads).Select(_ =>
            Task.Run(() => aggregator.IngestBatch(buffer))));

        long expected = (long)threads * framesPerBatch;

        // The headline assertion the Update-based test cannot make: the batched counter is exact.
        Assert.Equal(expected, aggregator.TotalProcessed);

        // And no folded reading was lost: per-sensor counts sum to the same total.
        long summedCounts = aggregator.CreateSnapshot().ToArray().Sum(s => s.Count);
        Assert.Equal(expected, summedCounts);
        Assert.Equal(sensorCount, aggregator.ActiveSensorCount);
    }
}
