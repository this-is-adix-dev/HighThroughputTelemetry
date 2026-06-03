using System.Diagnostics.Metrics;
using Telemetry.Engine.Observability;

namespace Telemetry.Tests;

/// <summary>
/// Verifies the engine's <see cref="EngineMetrics"/> instrumentation: that
/// recordings are observable through a <see cref="MeterListener"/> (the same
/// mechanism the console exporter uses) and — critically — that recording on the
/// hot path puts nothing on the heap.
/// </summary>
public class EngineMetricsTests
{
    [Fact]
    public void Instruments_AreObservable_ThroughMeterListener()
    {
        using var metrics = new EngineMetrics();

        long produced = 0;
        long consumed = 0;
        int batchRecords = 0;
        int lastBatchValue = 0;

        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == EngineMetrics.MeterName)
                    l.EnableMeasurementEvents(instrument);
            },
        };
        listener.SetMeasurementEventCallback<long>((instrument, measurement, _, _) =>
        {
            if (instrument.Name == EngineMetrics.ReadingsProducedName) produced += measurement;
            else if (instrument.Name == EngineMetrics.ReadingsConsumedName) consumed += measurement;
        });
        listener.SetMeasurementEventCallback<int>((instrument, measurement, _, _) =>
        {
            if (instrument.Name != EngineMetrics.BatchSizeName) return;
            batchRecords++;
            lastBatchValue = measurement;
        });
        listener.Start();

        metrics.ReadingsProduced.Add(1000);
        metrics.ReadingsConsumed.Add(750);
        metrics.BatchSize.Record(750);

        Assert.Equal(1000, produced);
        Assert.Equal(750, consumed);
        Assert.Equal(1, batchRecords);
        Assert.Equal(750, lastBatchValue);
    }

    // A static sink lets the measurement callbacks be `static` lambdas that capture
    // nothing, so the measured region below contains no closure allocation of our own
    // — only whatever the instrument path itself would allocate (which must be zero).
    private static long s_sink;

    [Fact]
    public void Recording_WithActiveListener_IsAllocationFree()
    {
        using var metrics = new EngineMetrics();
        using var listener = new MeterListener
        {
            InstrumentPublished = static (instrument, l) =>
            {
                if (instrument.Meter.Name == EngineMetrics.MeterName)
                    l.EnableMeasurementEvents(instrument);
            },
        };
        listener.SetMeasurementEventCallback<long>(static (_, measurement, _, _) => s_sink += measurement);
        listener.SetMeasurementEventCallback<int>(static (_, measurement, _, _) => s_sink += measurement);
        listener.Start();

        // Warm up so every method is JITted and any one-time, per-instrument
        // bookkeeping is already done before we begin counting allocated bytes.
        for (int i = 0; i < 10_000; i++)
        {
            metrics.ReadingsProduced.Add(1000);
            metrics.ReadingsConsumed.Add(1000);
            metrics.BatchSize.Record(1000);
        }

        const int iterations = 100_000;
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < iterations; i++)
        {
            metrics.ReadingsProduced.Add(1000);
            metrics.ReadingsConsumed.Add(1000);
            metrics.BatchSize.Record(1000);
        }
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        // 300,000 tag-less recordings, each traversing the live measurement-callback
        // path, must not allocate. Any boxing of the value or per-call tag array would
        // surface here as hundreds of kilobytes.
        Assert.Equal(0, allocated);
    }
}
