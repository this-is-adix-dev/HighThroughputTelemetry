using System.Threading;
using Telemetry.Engine.Aggregation;

namespace Telemetry.Engine.Sink;

/// <summary>
/// Module D — the asynchronous data sink. On a fixed cadence it samples the
/// aggregator, shards the snapshots, and flushes every shard to the (slow)
/// database concurrently.
///
/// Two modern async building blocks do the heavy lifting:
/// <list type="bullet">
///   <item><b><see cref="ValueTask"/></b> — the per-flush entry point avoids a
///   <c>Task</c> allocation on the (common) empty-window path.</item>
///   <item><b><see cref="Task.WhenEach(System.Collections.Generic.IEnumerable{Task})"/></b>
///   (.NET 9) — instead of <c>WhenAll</c> (which forces us to wait for the slowest
///   shard before reacting to <i>any</i>), we observe each shard's completion the
///   instant it lands and account for it immediately.</item>
/// </list>
///
/// The flush path is zero-allocation after construction: shard buckets and the
/// pending-write list are pre-allocated once and cleared between flushes.
/// Snapshot iteration uses a plain synchronous <see cref="IEnumerable{T}"/> to
/// avoid the async state-machine overhead of <c>IAsyncEnumerable</c> for what is
/// today an in-memory source. The <c>StreamSnapshots</c> method remains the seam
/// where a genuinely async (e.g. paged-remote) source would be plugged in.
/// </summary>
public sealed class AsyncDataSink
{
    private readonly SensorAggregator _aggregator;
    private readonly DummySlowDatabase _database;
    private readonly TimeSpan _flushInterval;
    private readonly int _shardCount;

    // Pre-allocated once; cleared and reused every flush so the hot path is
    // zero-allocation. Each bucket is sized for the expected sensor fan-out.
    private readonly List<SensorSnapshot>[] _shardBuckets;
    private readonly List<Task<int>> _pendingWrites;

    private long _flushCount;
    private long _rowsFlushed;

    public AsyncDataSink(
        SensorAggregator aggregator,
        DummySlowDatabase database,
        TimeSpan? flushInterval = null,
        int shardCount = 4)
    {
        _aggregator = aggregator;
        _database = database;
        _flushInterval = flushInterval ?? TimeSpan.FromSeconds(2);
        _shardCount = shardCount;

        _shardBuckets = new List<SensorSnapshot>[shardCount];
        for (int i = 0; i < shardCount; i++)
            _shardBuckets[i] = new List<SensorSnapshot>(capacity: 64);

        _pendingWrites = new List<Task<int>>(shardCount);
    }

    public long FlushCount => Interlocked.Read(ref _flushCount);
    public long RowsFlushed => Interlocked.Read(ref _rowsFlushed);

    /// <summary>
    /// Flush on a timer until cancelled, then perform one final flush so the last
    /// window of data is never lost on shutdown.
    /// </summary>
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        // PeriodicTimer is the allocation-light, async-native replacement for the
        // callback-based System.Threading.Timer — it integrates with await directly.
        using var timer = new PeriodicTimer(_flushInterval);

        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
                await FlushOnceAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Expected on shutdown.
        }

        // Final drain — use None so the simulated I/O is allowed to finish.
        await FlushOnceAsync(CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>
    /// Sample, shard, and persist a single window. Returns a <see cref="ValueTask"/>
    /// so an empty window costs nothing.
    /// </summary>
    public async ValueTask FlushOnceAsync(CancellationToken cancellationToken)
    {
        // Clear buckets from the previous flush; the pre-allocated lists are reused
        // in-place so no heap allocation occurs here.
        foreach (List<SensorSnapshot> bucket in _shardBuckets)
            bucket.Clear();

        foreach (SensorSnapshot snapshot in StreamSnapshots(cancellationToken))
        {
            int shard = (snapshot.SensorId & int.MaxValue) % _shardCount;
            _shardBuckets[shard].Add(snapshot);
        }

        // Kick off only non-empty shards. WriteAsync returns a ValueTask<int>;
        // materialise each as a Task so they can be awaited via Task.WhenEach.
        _pendingWrites.Clear();
        foreach (List<SensorSnapshot> bucket in _shardBuckets)
        {
            if (bucket.Count > 0)
                _pendingWrites.Add(_database.WriteAsync(bucket, cancellationToken).AsTask());
        }

        if (_pendingWrites.Count == 0)
            return;

        // Observe completions in the order they actually finish, not submission order.
        await foreach (Task<int> completed in Task.WhenEach(_pendingWrites).ConfigureAwait(false))
        {
            int rows = await completed.ConfigureAwait(false); // already complete; just unwraps the result
            Interlocked.Add(ref _rowsFlushed, rows);
        }

        Interlocked.Increment(ref _flushCount);
    }

    /// <summary>
    /// Expose the aggregator's current snapshot as a synchronous stream.
    /// This is the seam where a real implementation would page results from a
    /// remote store; switching to <c>async IAsyncEnumerable</c> here requires no
    /// changes to <see cref="FlushOnceAsync"/>.
    /// </summary>
    private IEnumerable<SensorSnapshot> StreamSnapshots(CancellationToken cancellationToken)
    {
        foreach (SensorSnapshot snapshot in _aggregator.CreateSnapshot())
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return snapshot;
        }
    }
}
