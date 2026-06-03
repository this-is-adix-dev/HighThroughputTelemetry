namespace Telemetry.Engine.Aggregation;

/// <summary>
/// An immutable, point-in-time copy of one sensor's running statistics.
///
/// A <c>readonly record struct</c> again: snapshots are produced in bulk every
/// flush interval and streamed to the sink, so we want value semantics and no
/// per-snapshot heap allocation.
/// </summary>
public readonly record struct SensorSnapshot(
    int SensorId,
    long Count,
    double Min,
    double Max,
    double Average);
