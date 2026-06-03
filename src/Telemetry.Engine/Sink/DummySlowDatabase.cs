using System.Threading;
using Telemetry.Engine.Aggregation;

namespace Telemetry.Engine.Sink;

/// <summary>
/// Stand-in for a slow, network-bound persistence layer (think: a remote
/// time-series database). It does no real work — it just sleeps for a randomized
/// interval to emulate I/O latency — but its async shape is exactly what a real
/// driver would expose.
///
/// <see cref="WriteAsync"/> returns a <see cref="Task{T}"/>: this implementation
/// always suspends on <c>Task.Delay</c>, so the async path is never elided and a
/// <c>Task</c> is always allocated. A real driver that frequently completes
/// synchronously (e.g. a cache hit) would benefit from <c>ValueTask&lt;int&gt;</c>,
/// but advertising that optimization here would be misleading.
/// </summary>
public sealed class DummySlowDatabase
{
    private readonly int _minLatencyMs;
    private readonly int _maxLatencyMs;
    private long _totalRowsWritten;

    public DummySlowDatabase(int minLatencyMs = 15, int maxLatencyMs = 60)
    {
        _minLatencyMs = minLatencyMs;
        _maxLatencyMs = maxLatencyMs;
    }

    /// <summary>Total rows "persisted" across the lifetime of the database.</summary>
    public long TotalRowsWritten => Interlocked.Read(ref _totalRowsWritten);

    /// <summary>
    /// Persist a shard of snapshots. Simulates a variable-latency round-trip and
    /// returns how many rows were written.
    /// </summary>
    public async Task<int> WriteAsync(IReadOnlyCollection<SensorSnapshot> rows, CancellationToken cancellationToken)
    {
        if (rows.Count == 0)
            return 0;

        // Random.Shared is itself thread-safe, so no per-call allocation or locking.
        int latency = Random.Shared.Next(_minLatencyMs, _maxLatencyMs + 1);
        await Task.Delay(latency, cancellationToken).ConfigureAwait(false);

        Interlocked.Add(ref _totalRowsWritten, rows.Count);
        return rows.Count;
    }
}
