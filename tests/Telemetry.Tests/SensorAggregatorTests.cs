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
        const int threads = 8;
        const int perThread = 25_000;

        // One shard per thread — the contract the lock-free hot path relies on. Every thread hammers
        // the SAME sensors (i % 16), but each writes its OWN shard, so the plain non-atomic field
        // updates never race. The fan-in in CreateSnapshot is what must recombine them without loss.
        var aggregator = new SensorAggregator(shardCount: threads);

        await Task.WhenAll(Enumerable.Range(0, threads).Select(t => Task.Run(() =>
        {
            for (int i = 0; i < perThread; i++)
                aggregator.Update(t, new SensorReading(SensorId: i % 16, TimestampTicks: 0, Value: 1f));
        })));

        // TotalProcessed is only incremented via IngestBatch (batched Interlocked.Add),
        // not via direct Update calls. Verify correctness via the fanned-in per-sensor counts:
        // no update may be lost across the per-shard merge.
        long summedCounts = aggregator.CreateSnapshot().ToArray().Sum(s => s.Count);
        Assert.Equal((long)threads * perThread, summedCounts);
    }

    [Fact]
    public void CreateSnapshot_FansInAcrossShards()
    {
        // Three shards each fold disjoint values into the SAME sensor. The snapshot must sum the
        // counts and sums and min/max-reduce across all shards — this is the fan-in correctness guard.
        var aggregator = new SensorAggregator(sensorCount: 4, shardCount: 3);

        aggregator.Update(0, new SensorReading(SensorId: 2, TimestampTicks: 0, Value: 10f));
        aggregator.Update(1, new SensorReading(SensorId: 2, TimestampTicks: 0, Value: 50f));
        aggregator.Update(1, new SensorReading(SensorId: 2, TimestampTicks: 0, Value: 30f));
        aggregator.Update(2, new SensorReading(SensorId: 2, TimestampTicks: 0, Value: 90f));

        ReadOnlySpan<SensorSnapshot> span = aggregator.CreateSnapshot();
        Assert.Equal(1, span.Length);
        SensorSnapshot snapshot = span[0];

        Assert.Equal(2, snapshot.SensorId);
        Assert.Equal(4, snapshot.Count);              // 1 + 2 + 1 across the three shards
        Assert.Equal(10.0, snapshot.Min, precision: 5); // global min from shard 0
        Assert.Equal(90.0, snapshot.Max, precision: 5); // global max from shard 2
        Assert.Equal(45.0, snapshot.Average, precision: 5); // (10+50+30+90)/4
        Assert.Equal(1, aggregator.ActiveSensorCount);
        Assert.Equal(3, aggregator.ShardCount);
    }

    [Fact]
    public void Update_DropsOutOfDomainSensorId_WithoutThrowing()
    {
        var aggregator = new SensorAggregator(sensorCount: 4);

        // In-domain ids fold and report true.
        Assert.True(aggregator.Update(new SensorReading(SensorId: 0, TimestampTicks: 0, Value: 1f)));
        Assert.True(aggregator.Update(new SensorReading(SensorId: 3, TimestampTicks: 0, Value: 1f)));

        // Authentic-but-out-of-domain ids must be dropped (return false), never throw —
        // this is the regression guard for the IndexOutOfRangeException that an array-backed
        // aggregator would otherwise raise on a misconfigured-but-validly-signed reading.
        Assert.False(aggregator.Update(new SensorReading(SensorId: 4, TimestampTicks: 0, Value: 1f)));
        Assert.False(aggregator.Update(new SensorReading(SensorId: int.MaxValue, TimestampTicks: 0, Value: 1f)));
        Assert.False(aggregator.Update(new SensorReading(SensorId: -1, TimestampTicks: 0, Value: 1f)));

        // Only the two valid sensors were ever touched.
        Assert.Equal(2, aggregator.ActiveSensorCount);
    }

    [Fact]
    public void IngestBatch_DropsFramesWithOutOfDomainSensorIds()
    {
        const int sensorCount = 4;

        // Four correctly-signed frames: ids 0 and 3 are in-domain; 4 and 7 are not.
        int[] ids = [0, 3, 4, 7];
        var buffer = new byte[ids.Length * TelemetryCodec.FrameSize];
        for (int i = 0; i < ids.Length; i++)
        {
            var reading = new SensorReading(SensorId: ids[i], TimestampTicks: i, Value: i);
            TelemetryCodec.EncodeFrame(in reading, buffer.AsSpan(i * TelemetryCodec.FrameSize, TelemetryCodec.FrameSize));
        }

        var aggregator = new SensorAggregator(sensorCount);
        int ingested = aggregator.IngestBatch(buffer);

        // Only the two in-domain frames are folded; the two out-of-domain frames are
        // dropped (not thrown) and excluded from both the return value and TotalProcessed.
        Assert.Equal(2, ingested);
        Assert.Equal(2, aggregator.TotalProcessed);
        Assert.Equal(2, aggregator.ActiveSensorCount);
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

        // One shard per thread, mirroring the pipeline's consumer-per-shard wiring.
        var aggregator = new SensorAggregator(sensorCount, shardCount: threads);

        // Fan the same batch through IngestBatch from many threads at once — each into its own shard.
        // The batched Interlocked.Add inside IngestBatch is what must keep TotalProcessed exact under
        // contention (a plain non-atomic accumulation would lose updates), while the single-writer
        // shards keep the per-sensor counts exact without any lock on the hot path.
        await Task.WhenAll(Enumerable.Range(0, threads).Select(t =>
            Task.Run(() => aggregator.IngestBatch(t, buffer))));

        long expected = (long)threads * framesPerBatch;

        // The headline assertion the Update-based test cannot make: the batched counter is exact.
        Assert.Equal(expected, aggregator.TotalProcessed);

        // And no folded reading was lost: per-sensor counts sum to the same total.
        long summedCounts = aggregator.CreateSnapshot().ToArray().Sum(s => s.Count);
        Assert.Equal(expected, summedCounts);
        Assert.Equal(sensorCount, aggregator.ActiveSensorCount);
    }
}
