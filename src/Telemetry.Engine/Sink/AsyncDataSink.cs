using System.Runtime.CompilerServices;
using System.Threading;
using Telemetry.Engine.Aggregation;

namespace Telemetry.Engine.Sink;

/// <summary>
/// Module D — the asynchronous data sink. On a fixed cadence it samples the
/// aggregator, shards the snapshots, and flushes every shard to the (slow)
/// database concurrently.
///
/// Three modern async building blocks do the heavy lifting:
/// <list type="bullet">
///   <item><b><see cref="IAsyncEnumerable{T}"/></b> — snapshots are produced as an
///   async stream, so the source could just as easily be a paged remote query
///   without changing the consumer.</item>
///   <item><b><see cref="ValueTask"/></b> — the per-flush entry point avoids a
///   <c>Task</c> allocation on the (common) empty-window path.</item>
///   <item><b><see cref="Task.WhenEach(System.Collections.Generic.IEnumerable{Task})"/></b>
///   (.NET 9) — instead of <c>WhenAll</c> (which forces us to wait for the slowest
///   shard before reacting to <i>any</i>), we observe each shard's completion the
///   instant it lands and account for it immediately.</item>
/// </list>
/// </summary>
public sealed class AsyncDataSink
{
    private readonly SensorAggregator _aggregator;
    private readonly DummySlowDatabase _database;
    private readonly TimeSpan _flushInterval;
    private readonly int _shardCount;

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
        // Bucket snapshots into shards consumed from the async stream.
        Dictionary<int, List<SensorSnapshot>>? shards = null;

        await foreach (SensorSnapshot snapshot in StreamSnapshotsAsync(cancellationToken).ConfigureAwait(false))
        {
            shards ??= new Dictionary<int, List<SensorSnapshot>>(_shardCount);
            int shard = (snapshot.SensorId & int.MaxValue) % _shardCount;

            if (!shards.TryGetValue(shard, out List<SensorSnapshot>? bucket))
                shards[shard] = bucket = new List<SensorSnapshot>();

            bucket.Add(snapshot);
        }

        if (shards is null || shards.Count == 0)
            return;

        // Kick off every shard's write concurrently. WriteAsync hands back a
        // ValueTask<int>; we materialize each into a Task so they can be awaited
        // collectively via Task.WhenEach.
        var pending = new List<Task<int>>(shards.Count);
        foreach (List<SensorSnapshot> bucket in shards.Values)
            pending.Add(_database.WriteAsync(bucket, cancellationToken).AsTask());

        // Observe completions in the order they actually finish, not submission order.
        await foreach (Task<int> completed in Task.WhenEach(pending).ConfigureAwait(false))
        {
            int rows = await completed.ConfigureAwait(false); // already complete; just unwraps the result
            Interlocked.Add(ref _rowsFlushed, rows);
        }

        Interlocked.Increment(ref _flushCount);
    }

    /// <summary>
    /// Expose the aggregator's current snapshot as an async stream. Trivial here,
    /// but this is the seam where a real implementation would page results from a
    /// remote store without buffering them all in memory.
    /// </summary>
    private async IAsyncEnumerable<SensorSnapshot> StreamSnapshotsAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (SensorSnapshot snapshot in _aggregator.CreateSnapshot())
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return snapshot;
        }

        // Keeps the method a genuine async iterator (and a natural await point for a
        // real paged source) without adding measurable latency.
        await Task.CompletedTask.ConfigureAwait(false);
    }
}
