namespace Telemetry.Engine.Orchestration;

/// <summary>
/// Tuning knobs for a pipeline run. Sensible defaults match the brief:
/// 100,000 readings/sec for 10 seconds.
/// </summary>
public sealed class PipelineOptions
{
    /// <summary>How long the simulation runs before graceful shutdown.</summary>
    public TimeSpan Duration { get; init; } = TimeSpan.FromSeconds(10);

    /// <summary>Target generation rate handed to the firehose.</summary>
    public int TargetReadingsPerSecond { get; init; } = 100_000;

    /// <summary>Readings per batch envelope (amortizes channel sync cost).</summary>
    public int BatchSize { get; init; } = 1_000;

    /// <summary>Number of distinct simulated sensors.</summary>
    public int SensorCount { get; init; } = 64;

    /// <summary>Bounded channel capacity in batches — the back-pressure window.</summary>
    public int ChannelCapacity { get; init; } = 64;

    /// <summary>Parallel consumer workers fanning out of the channel.</summary>
    public int ConsumerCount { get; init; } = Math.Max(2, Environment.ProcessorCount / 2);

    /// <summary>How often the sink flushes aggregated data to the dummy database.</summary>
    public TimeSpan FlushInterval { get; init; } = TimeSpan.FromSeconds(2);

    /// <summary>Number of concurrent write shards the sink splits each flush into.</summary>
    public int SinkShardCount { get; init; } = 4;
}

/// <summary>Immutable summary returned when a pipeline run completes.</summary>
public readonly record struct PipelineReport(
    long Produced,
    long Processed,
    int DistinctSensors,
    long Flushes,
    long RowsPersisted,
    TimeSpan Elapsed);
