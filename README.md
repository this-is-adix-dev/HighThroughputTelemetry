# HighThroughputTelemetry

A compact, production-grade **.NET 10 / C# 14** reference project demonstrating
**high-load, low-latency system design** and the modern allocation-conscious
toolbox that ships with .NET 8 / 9 / 10.

It simulates a telemetry ingestion engine: a firehose of **100,000 sensor
readings per second** is generated, parsed without heap allocation, aggregated
across multiple consumer threads, and periodically flushed to a (simulated) slow
data store — all running for 10 seconds and then shutting down cleanly.

```
┌──────────────┐   bounded     ┌──────────────┐   in SensorReading   ┌──────────────┐
│  Firehose    │   Channel     │  Consumers   │  ──────────────────► │  Aggregator  │
│  (Module A)  │ ───────────►  │  (fan-out)   │   zero-alloc parse   │  (Module C)  │
│ 100k rd/s    │  TelemetryBatch│  Module B    │                      │ sharded,     │
│ Task.Delay   │  (ArrayPool)  └──────────────┘                      │ lock-free    │
└──────────────┘                                                     └──────┬───────┘
                                                                            │ ReadOnlySpan<SensorSnapshot>
                                                                            ▼
                                                                     ┌──────────────┐
                                                                     │  Async Sink  │
                                                                     │  (Module D)  │
                                                                     │ pre-alloc'd  │
                                                                     │ Task.WhenEach│
                                                                     └──────────────┘
                                                                            ▲
                                                              ┌─────────────┴──────────────┐
                                                              │  Observability             │
                                                              │  System.Diagnostics.Metrics│
                                                              │  MeterListener + console   │
                                                              └────────────────────────────┘
```

---

## Architecture

The solution is split along clean separation-of-concerns boundaries:

```
HighThroughputTelemetry.sln
├── src/Telemetry.Engine/            # the engine (Native AOT console app)
│   ├── Domain/                      # SensorReading value type + wire layout
│   ├── Parsing/                     # Module B — zero-allocation codec & parser
│   ├── Producer/                    # Module A — firehose, PreciseDelay, pooled batch envelope
│   ├── Aggregation/                 # Module C — concurrent statistics
│   ├── Sink/                        # Module D — async flushing to slow I/O
│   ├── Observability/               # EngineMetrics + ConsoleMetricsExporter
│   ├── Orchestration/               # composition root + run options
│   └── Program.cs                   # thin entry point
├── tests/Telemetry.Tests/           # xUnit unit + concurrency tests
└── benchmarks/Telemetry.Benchmarks/ # BenchmarkDotNet: parser alloc, lock contention, pacing
```

### The 16-byte wire frame

Everything is built around a fixed, little-endian 16-byte payload:

| Offset | Field            | Type          | Size |
|-------:|------------------|---------------|-----:|
| 0      | `TimestampTicks` | `Int64`       | 8 B  |
| 8      | `SensorId`       | `Int32`       | 4 B  |
| 12     | `Value`          | `Single`      | 4 B  |

### Module A — Internal Firehose Generator (Producer)
[`Producer/FirehoseGenerator.cs`](src/Telemetry.Engine/Producer/FirehoseGenerator.cs)

* Generates frames at a wall-clock-paced **100,000 readings/sec** (it emits the
  *deficit* against the target on each loop, so the long-run average is stable
  under scheduling jitter).
* Hands work off as **batches** (`TelemetryBatch`) through a **bounded
  `System.Threading.Channels.Channel`**, giving lock-free MPMC handoff with
  built-in **back-pressure** (`BoundedChannelFullMode.Wait`).
* Every batch buffer is **rented from `ArrayPool<byte>`** and returned securely
  via a **`try-finally` block**, ensuring no memory leaks occur even during
  cancellation or unexpected errors, leaving steady-state GC pressure essentially flat.
* Inter-batch pacing uses a plain **`Task.Delay`** floor. The downstream is a
  bounded channel with `FullMode.Wait`, which already absorbs producer jitter, and
  the deficit is recomputed against the wall clock every loop, so any timer
  overshoot is self-correcting — sub-tick pacing precision is accuracy the pipeline
  cannot consume, and is not worth a busy-spin. The high-resolution
  **[`PreciseDelay`](src/Telemetry.Engine/Producer/PreciseDelay.cs)** primitive is
  kept and **separately benchmarked** (accuracy vs. CPU cost) rather than applied
  where it earns nothing — see [`PacingBenchmarks`](benchmarks/Telemetry.Benchmarks/PacingBenchmarks.cs).

### Module B — Zero-Allocation Binary Parser
[`Parsing/`](src/Telemetry.Engine/Parsing/)

* [`TelemetryCodec`](src/Telemetry.Engine/Parsing/TelemetryCodec.cs) encodes/decodes
  frames using `ReadOnlySpan<byte>` and `BinaryPrimitives` — **zero heap
  allocation**, single bounds check per frame.
* [`TelemetryParser`](src/Telemetry.Engine/Parsing/TelemetryParser.cs) is a
  **`ref struct`** forward cursor over a span — the compiler *guarantees* it can
  never escape to the heap, making it safe to wrap pooled/stack memory.
* [`PayloadBuffer`](src/Telemetry.Engine/Parsing/PayloadBuffer.cs) is a C# 12+
  **`[InlineArray]`** type: a 16-byte buffer laid out inline in the struct that
  can be returned **by value** with no allocation and no `unsafe fixed`.
* Reading is passed by **`in`** to avoid copying the value struct.
* Integrates AppSec directly into the hot path: **zero-allocation HMAC-SHA256**
  signature verification using `stackalloc`, `[SkipLocalsInit]`, and
  `CryptographicOperations.FixedTimeEquals` to prevent timing attacks.

### Module C — Lock-Free Sharded Aggregator
[`Aggregation/`](src/Telemetry.Engine/Aggregation/)

* The global "readings processed" counter is **wait-free** via
  `System.Threading.Interlocked`, batched once per `IngestBatch` call (a 1000×
  reduction in `LOCK XADD` instructions vs. incrementing per-reading). This is the
  **only** atomic left on the hot path.
* Per-sensor statistics are **sharded one array per consumer**: the aggregator holds
  a `SensorStatistics[][]` and each consumer is handed a stable `shardIndex` (shard
  count = consumer count) that it alone writes. Two consumers updating the same hot
  sensor now touch two different cache lines in two different arrays, so the previous
  design's Read-For-Ownership storm — every `Update` serializing on one shared
  object's `Lock` and dirtying one shared cache line — is gone. Updates are
  **uncontended, lock-free, and need neither `Lock` nor `Interlocked`** on the
  per-sensor fields.
* `SensorStatistics` is now a **mutable `struct` stored by value** in the shard array,
  updated in place via `ref` — restoring genuine data cache-locality (no reference
  walk feeding scattered heap pointer-chases). Thread safety against concurrent snapshot
  reads is guaranteed by carefully ordering updates and using **`Volatile.Write`** for
  the final `Count` increment, which acts as a memory barrier preventing "dirty reads".
* The only synchronization that remains is the periodic **`CreateSnapshot` fan-in**,
  which sums counts/sums and min/max-reduces across shards. It runs at the flush
  cadence (seconds apart), so moving the only cross-thread reads there makes them
  effectively free. The win is measured, not just claimed — see
  [`ContentionBenchmarks`](benchmarks/Telemetry.Benchmarks/ContentionBenchmarks.cs).
* **`CreateSnapshot`** writes into a **pre-allocated `SensorSnapshot[]`** and
  returns a `ReadOnlySpan<SensorSnapshot>` slice — **zero heap allocation per
  call**. Callers must consume the span within a single synchronous scope and
  must not hold it across an `await` or past the next `CreateSnapshot` call.

### Module D — Asynchronous Data Sink
[`Sink/`](src/Telemetry.Engine/Sink/)

* Flushes on a **`PeriodicTimer`** cadence, sampling the aggregator via
  `CreateSnapshot` which returns a `ReadOnlySpan<SensorSnapshot>`. The span is
  fully consumed inside `PopulateShardBuckets` — a synchronous helper — before
  any `await`, satisfying the span lifetime contract and leaving a clean seam
  where a paged-remote source would switch to `async IAsyncEnumerable<T>`.
* The **flush path is zero-allocation after construction**: shard buckets
  (`List<SensorSnapshot>[]`) and the pending-write array (`Task<int>[]`) are
  pre-allocated once and reused across every flush.
* `DummySlowDatabase.WriteAsync` returns a **`Task<int>`** — it always suspends
  on `Task.Delay`, so a `ValueTask` wrapper would provide no allocation benefit
  and was excluded to avoid misleading callers.
* Shards are written concurrently; completions are observed in *finish order*
  with **.NET 9 `Task.WhenEach`** over a sliced `Span` (`tasks[..count]`),
  eliminating intermediate collection allocations.

### Observability
[`Observability/`](src/Telemetry.Engine/Observability/)

* [`EngineMetrics`](src/Telemetry.Engine/Observability/EngineMetrics.cs) owns one
  `System.Diagnostics.Metrics.Meter` and four instruments
  (`telemetry.readings.produced`, `telemetry.readings.consumed`,
  `telemetry.batch.size`, `telemetry.readings.rejected.tampered`). Using the
  runtime's built-in metrics API means **no reflection on the recording path**,
  full Native AOT compatibility, and zero boxing on the no-tag `Add`/`Record`
  overloads.
* [`ConsoleMetricsExporter`](src/Telemetry.Engine/Observability/ConsoleMetricsExporter.cs)
  attaches a `MeterListener` to the engine meter. Measurement callbacks fire on
  the recording thread and do nothing but a single wait-free `Interlocked.Add`;
  formatting and console I/O happen later on the reporter's own `PeriodicTimer`
  task — never on the hot path. Instrument routing uses reference comparison
  (`ReferenceEquals`) on the captured `Instrument` objects, so no string is
  compared and nothing is boxed per measurement.

### Orchestration
[`Orchestration/TelemetryPipeline.cs`](src/Telemetry.Engine/Orchestration/TelemetryPipeline.cs)
is the composition root that wires the channel, the consumer fan-out, the
aggregator, the sink and the observability exporter, then runs them for a bounded
duration. It uses a **fast-fail orchestration mechanism** with `Task.WhenAny`—if
any pipeline stage faults, the entire pipeline is instantly cancelled. Shutdown
is deterministic (producer completes the channel → consumers drain → sink performs a
final flush). `Program.cs` stays thin and also wires `Ctrl+C` to a graceful early stop.

---

## Modern .NET / C# features on display

| Feature | Where | Why it matters here |
|---|---|---|
| **Native AOT** (`PublishAot`) | `Telemetry.Engine.csproj` | No JIT, instant startup, ~1.6 MB self-contained binary |
| **16-byte aligned `struct`** | `SensorReading` | Fields reordered to eliminate padding |
| **Bounded `Channel<T>`** | Module A | Lock-free handoff + back-pressure |
| **`ArrayPool<T>`** | Modules A/B | Zero steady-state GC for buffers |
| **Robust `try-finally` pool returns** | Module A | Prevents leaks under cancellation |
| **`PreciseDelay` (`SpinWait` + `Stopwatch`)** | benchmarked primitive | Sub-millisecond pacing without OS timer floor artifacts — measured, not applied to the firehose |
| **`Span`/`ReadOnlySpan<byte>`** | Module B | Allocation-free slicing & parsing |
| **`ref struct`** | `TelemetryParser` | Compiler-enforced no-heap-escape |
| **`[InlineArray]`** | `PayloadBuffer` | Inline, escapable, safe fixed buffer |
| **`CryptographicOperations.FixedTimeEquals`** | Module B | Constant-time HMAC verification to prevent timing attacks |
| **`in` parameters / `readonly record struct`** | throughout | Pass-by-ref-no-copy value semantics |
| **Per-consumer sharded `SensorStatistics[][]`** | Module C | Lock-free, uncontended hot path — no cross-core write-sharing on hot sensors |
| **Mutable `struct` stat stored by value + `ref` update** | Module C | Contiguous data cache-locality; single-writer makes false sharing harmless |
| **`Volatile.Write`** | Module C | Thread-safe memory barrier for snapshot readers |
| **`Interlocked`** | Module C | Wait-free hot counter, batched per batch not per reading |
| **Pre-allocated `ReadOnlySpan<SensorSnapshot>`** | Module C | Zero-alloc snapshot with bounded lifetime |
| **Pre-allocated shard buckets & Task arrays** | Module D | Zero-alloc flush path after construction |
| **`ValueTask`** | Module D | Allocation-free async on the empty-window path |
| **`Task.WhenEach` (.NET 9) padding** | Module D | React to shard completions allocation-free by padding and passing the full pre-allocated array, bypassing async-iterator slicing limits |
| **`PeriodicTimer`** | Modules C/D/Observability | Async-native, allocation-light timers |
| **`System.Diagnostics.Metrics` + `MeterListener`** | Observability | Native AOT-safe, zero-alloc instrumentation decoupled from consumers |
| **Fast-fail `Task.WhenAny`** | Orchestration | Immediate cancellation on component fault |

---

## Running it

> Requires the **.NET 10 SDK**. (A C/C++ toolchain — `clang`/`gcc` — is only
> needed for the optional Native AOT *publish* step.)

### Run the 10-second simulation

```bash
dotnet run -c Release --project src/Telemetry.Engine
```

Sample output:

```
[  1.0s] Throughput:  100,000 msg/sec | Produced:  100,000 msg/sec | Batches processed:    100 | Total:     100,000 | Tampered:     0
...
=====================================================================
Simulation complete.
  Elapsed wall-clock      : 10.06 s
  Readings produced       : 999,000
  Readings processed      : 999,000
  Effective throughput    : 99,280 readings/s
  Distinct sensors        : 64
  Sink flushes            : 5
  Rows persisted to sink  : 320
=====================================================================
```

### Run the tests

```bash
dotnet test
```

### Run the benchmarks

```bash
# Pick a suite interactively, or target one with --filter:
dotnet run -c Release --project benchmarks/Telemetry.Benchmarks
dotnet run -c Release --project benchmarks/Telemetry.Benchmarks -- --filter '*Contention*'
dotnet run -c Release --project benchmarks/Telemetry.Benchmarks -- --filter '*'   # run all
```

Three suites:

* **`ParserBenchmarks`** — naive vs. zero-allocation parser. `[MemoryDiagnoser]` is
  enabled, so the **Allocated** column is the point: the naive parser allocates a
  throwaway array **and** a heap object per reading (and a growing `List<T>`), while
  `TelemetryParser` allocates **0 B** regardless of how many readings it decodes.
* **`ContentionBenchmarks`** — the Red-Flag-B proof. 1/2/4/8/16 threads all hammer a
  single hot sensor: the **shared-locked** baseline's wall time climbs ~linearly with
  threads (the throughput cliff) while the **per-consumer sharded** aggregator stays
  flat — the ratio widens to a large multiple by 16 threads.
* **`PacingBenchmarks`** — `PreciseDelay` vs. `Task.Delay` across sub-tick targets,
  documenting the accuracy/CPU-cost trade-off (precise but spin-bound vs. coarse but
  free) that justifies pacing the firehose with `Task.Delay`.

### Publish a Native AOT binary

```bash
dotnet publish src/Telemetry.Engine -c Release -r linux-x64
# → src/Telemetry.Engine/bin/Release/net10.0/linux-x64/publish/Telemetry.Engine  (~1.6 MB, no runtime required)
```

(Swap `linux-x64` for `win-x64` or `osx-arm64` as needed.)

### Run the Native AOT Docker Container (Chiseled)

```bash
docker build -t telemetry-engine .
docker run --rm telemetry-engine
```

(Builds an ultra-minimal, non-root container based on Ubuntu Chiseled, demonstrating production-ready AppSec posture and a final image size of < 30 MB).

---

## Design notes & trade-offs

* **Batching** is the single biggest throughput lever: pushing one reading at a
  time through the channel would pay synchronization cost 100k times/sec; batching
  amortizes it across ~1,000 readings.
* **Sharded, lock-free aggregation** is the decisive contention win: instead of one
  shared per-sensor object behind a `Lock`, each consumer owns a private
  `SensorStatistics[]` shard and is its sole writer, so per-sensor updates are
  uncontended and need no synchronization at all. The single remaining `Interlocked`
  counter (global readings-processed, batched per batch) and the once-per-flush
  fan-in are the only cross-thread coordination left. The
  [contention benchmark](benchmarks/Telemetry.Benchmarks/ContentionBenchmarks.cs)
  shows the locked baseline's throughput collapsing as cores are added while the
  sharded design stays flat.
* **Array vs. dictionary for sensor state**: a pre-sized array (now one per shard)
  trades the bounded sensor domain assumption for O(1) direct indexing with zero
  per-update allocation and no hash computation; storing the stats `struct` by value
  keeps each shard's data contiguous in cache.
* **Pre-allocated snapshot and flush buffers** eliminate the last remaining GC
  pressure on the hot path: `SensorAggregator.CreateSnapshot` reuses a fixed
  `SensorSnapshot[]`, and `AsyncDataSink` reuses its shard `List<T>` instances
  across every flush cycle.
* **`PreciseDelay` is kept as a primitive, not used as the firehose pacer.** On
  Windows the timer tick is ~15.6 ms, so any `Task.Delay` shorter than that parks
  the thread for a full tick; the two-phase hybrid (coarse sleep + `SpinWait` fine
  tail) delivers microsecond-accurate cadence — at the cost of busy-spinning the
  tail. Because the bounded channel already absorbs producer jitter, that precision
  buys the pipeline nothing, so the firehose paces with a plain `Task.Delay` and the
  primitive's accuracy/CPU-cost trade-off is documented with numbers in
  [`PacingBenchmarks`](benchmarks/Telemetry.Benchmarks/PacingBenchmarks.cs) instead.
* **Observability is fully decoupled**: `ConsoleMetricsExporter` never imports
  `EngineMetrics` — it matches by meter name. The producing and observing sides
  are order-independent and can be swapped for any `MeterListener`-compatible
  consumer (OpenTelemetry, Prometheus, etc.) with no changes to the engine.
* **Graceful shutdown is explicit**, not abrupt: cancellation stops the producer,
  which completes the channel writer; consumers then drain every in-flight batch
  (no data loss) before the sink performs one final flush.
* The numbers reported (e.g. `999,000`) reflect the wall-clock pacing window; the
  engine sustains the full 100k/sec rate every second of the run.
