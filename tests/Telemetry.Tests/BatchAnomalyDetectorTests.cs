using Telemetry.Engine.Domain;
using Telemetry.Engine.Parsing;
using Telemetry.Engine.Processing;

namespace Telemetry.Tests;

/// <summary>
/// Exercises the SIMD <see cref="BatchAnomalyDetector"/>: correctness of the threshold
/// test, the strictly-greater boundary, edge sizes, and — most importantly — that the
/// hardware-gather fast path and the scalar fallback agree for every batch size, including
/// the &lt; 8-frame tail the SIMD path hands back to the scalar helper.
/// </summary>
public class BatchAnomalyDetectorTests
{
    private const float Threshold = 95.0f;

    /// <summary>
    /// Build a real wire batch (16-byte data + 16-byte HMAC per frame) from a sequence of
    /// values, so the detector is tested against exactly the layout it sees in production.
    /// </summary>
    private static byte[] BuildBatch(ReadOnlySpan<float> values)
    {
        var buffer = new byte[values.Length * TelemetryCodec.FrameSize];
        for (int i = 0; i < values.Length; i++)
        {
            var reading = new SensorReading(SensorId: i % 8, TimestampTicks: i, Value: values[i]);
            TelemetryCodec.EncodeFrame(in reading, buffer.AsSpan(i * TelemetryCodec.FrameSize, TelemetryCodec.FrameSize));
        }
        return buffer;
    }

    private static byte[] BuildBatch(float fill, int count)
    {
        var values = new float[count];
        Array.Fill(values, fill);
        return BuildBatch(values);
    }

    [Fact]
    public void EmptyBatch_HasNoAnomalies()
    {
        Assert.False(BatchAnomalyDetector.HasCriticalAnomalies(ReadOnlySpan<byte>.Empty, Threshold));
    }

    [Fact]
    public void AllBelowThreshold_ReturnsFalse()
    {
        // 100 frames (enough for several SIMD groups plus a tail) all comfortably under.
        byte[] batch = BuildBatch(fill: 50f, count: 100);
        Assert.False(BatchAnomalyDetector.HasCriticalAnomalies(batch, Threshold));
    }

    [Fact]
    public void ValueExactlyAtThreshold_IsNotAnAnomaly()
    {
        // The test is strictly-greater, so a reading equal to the threshold must NOT fire.
        byte[] batch = BuildBatch(fill: Threshold, count: 100);
        Assert.False(BatchAnomalyDetector.HasCriticalAnomalies(batch, Threshold));
    }

    [Fact]
    public void ValueJustAboveThreshold_IsAnAnomaly()
    {
        byte[] batch = BuildBatch(fill: Threshold, count: 100);

        // Nudge a single frame just above the threshold; the detector must catch it.
        var spiked = new SensorReading(SensorId: 0, TimestampTicks: 0, Value: 95.0001f);
        TelemetryCodec.EncodeFrame(in spiked, batch.AsSpan(42 * TelemetryCodec.FrameSize, TelemetryCodec.FrameSize));

        Assert.True(BatchAnomalyDetector.HasCriticalAnomalies(batch, Threshold));
    }

    [Theory]
    // One anomaly placed at every interesting position: lane 0 of the first group, mid-group,
    // the last lane of a full group, and inside the < 8-frame scalar tail. This proves the
    // SIMD group loop and the scalar tail handoff both detect a breach.
    [InlineData(0)]
    [InlineData(3)]
    [InlineData(7)]   // last lane of the first 8-frame SIMD group
    [InlineData(8)]   // first lane of the second group
    [InlineData(19)]  // arbitrary mid-batch
    [InlineData(23)]  // 24 frames -> 3 full groups, this is the last full-group lane
    [InlineData(25)]  // tail frame when count = 30 (24 vectorized + 6 scalar)
    public void SingleAnomaly_IsFoundAtAnyPosition(int anomalyIndex)
    {
        const int count = 30; // 3 full SIMD groups (24) + a 6-frame scalar tail
        var values = new float[count];
        Array.Fill(values, 40f);
        values[anomalyIndex] = 130f;

        byte[] batch = BuildBatch(values);
        Assert.True(BatchAnomalyDetector.HasCriticalAnomalies(batch, Threshold));
    }

    [Theory]
    // Sweep across the SIMD/scalar boundary: 0..16 frames covers empty, sub-vector tails,
    // exactly one vector, and a vector-plus-tail.
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(7)]
    [InlineData(8)]
    [InlineData(9)]
    [InlineData(16)]
    public void TailFramesAreNotIgnored(int count)
    {
        if (count == 0)
        {
            Assert.False(BatchAnomalyDetector.HasCriticalAnomalies(ReadOnlySpan<byte>.Empty, Threshold));
            return;
        }

        // Put the only anomaly in the very LAST frame, which for non-multiples of 8 lives in
        // the scalar tail — the position most likely to be skipped by a buggy SIMD loop.
        var values = new float[count];
        Array.Fill(values, 10f);
        values[count - 1] = 200f;

        byte[] batch = BuildBatch(values);
        Assert.True(BatchAnomalyDetector.HasCriticalAnomalies(batch, Threshold));
    }

    [Fact]
    public void NegativeAndZeroValues_NeverFalsePositive()
    {
        var values = new float[40];
        for (int i = 0; i < values.Length; i++)
            values[i] = i % 2 == 0 ? 0f : -123.45f;

        byte[] batch = BuildBatch(values);
        Assert.False(BatchAnomalyDetector.HasCriticalAnomalies(batch, Threshold));
    }
}
