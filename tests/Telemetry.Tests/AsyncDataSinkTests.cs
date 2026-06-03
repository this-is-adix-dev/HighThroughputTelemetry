using Telemetry.Engine.Aggregation;
using Telemetry.Engine.Domain;
using Telemetry.Engine.Sink;

namespace Telemetry.Tests;

public class AsyncDataSinkTests
{
    [Fact]
    public async Task FlushOnce_PersistsOneRowPerSensor()
    {
        var aggregator = new SensorAggregator();
        for (int sensor = 0; sensor < 10; sensor++)
            aggregator.Update(new SensorReading(SensorId: sensor, TimestampTicks: 0, Value: sensor));

        var database = new DummySlowDatabase(minLatencyMs: 0, maxLatencyMs: 1);
        var sink = new AsyncDataSink(aggregator, database, flushInterval: TimeSpan.FromHours(1), shardCount: 4);

        await sink.FlushOnceAsync(CancellationToken.None);

        // One snapshot per distinct sensor should have been written, across all shards.
        Assert.Equal(10, database.TotalRowsWritten);
        Assert.Equal(10, sink.RowsFlushed);
        Assert.Equal(1, sink.FlushCount);
    }

    [Fact]
    public async Task FlushOnce_WithNoData_IsANoOp()
    {
        var aggregator = new SensorAggregator();
        var database = new DummySlowDatabase(minLatencyMs: 0, maxLatencyMs: 1);
        var sink = new AsyncDataSink(aggregator, database);

        await sink.FlushOnceAsync(CancellationToken.None);

        Assert.Equal(0, database.TotalRowsWritten);
        Assert.Equal(0, sink.FlushCount);
    }
}
