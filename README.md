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
│ 100k rd/s    │  TelemetryBatch│  Module B    │                      │ min/max/avg  │
│ PreciseDelay │  (ArrayPool)  └──────────────┘                      └──────┬───────┘
└──────────────┘                                                            │ ReadOnlySpan<SensorSnapshot>
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
└── benchmarks/Telemetry.Benchmarks/ # BenchmarkDotNet: naive vs zero-alloc parser
```

### The 16-byte wire frame

Everything is built around a fixed, little-endian 16-byte payload:

| Offset | Field            | Type          | Size |
|-------:|------------------|---------------|-----:|
| 0      | `SensorId`       | `Int32`       | 4 B  |
| 4      | `TimestampTicks` | `Int64`       | 8 B  |
| 12     | `Value`          | `Single`      | 4 B  |

### Module A — Internal Firehose Generator (Producer)
[`Producer/FirehoseGenerator.cs`](src/Telemetry.Engine/Producer/FirehoseGenerator.cs)

* Generates frames at a wall-clock-paced **100,000 readings/sec** (it emits the
  *deficit* against the target on each loop, so the long-run average is stable
  under scheduling jitter).
* Hands work off as **batches** (`TelemetryBatch`) through a **bounded
  `System.Threading.Channels.Channel`**, giving lock-free MPMC handoff with
  built-in **back-pressure** (`BoundedChannelFullMode.Wait`).
* Every batch buffer is **rented from `ArrayPool<byte>`** and returned after
  consumption, so steady-state GC pressure is essentially flat.
* Inter-batch pacing uses **[`PreciseDelay`](src/Telemetry.Engine/Producer/PreciseDelay.cs)**
  — a two-phase hybrid that coarse-sleeps with `Task.Delay` for the bulk of
  the wait, then busy-spins with `SpinWait` measured against a `Stopwatch`
  timestamp for the sub-tick tail. This prevents the OS timer floor (~15.6 ms
  on Windows) from turning a smooth 10 ms cadence into bursty catch-up loops.

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

### Module C — Lock-Free / Low-Lock Aggregator
[`Aggregation/`](src/Telemetry.Engine/Aggregation/)

* The global "readings processed" counter is **wait-free** via
  `System.Threading.Interlocked`, batched once per `IngestBatch` call (a 1000×
  reduction in `LOCK XADD` instructions vs. incrementing per-reading).
* Per-sensor statistics live in a **pre-sized `SensorStatistics[]`**, indexed
  directly by `SensorId`. This replaces the former `ConcurrentDictionary`:
  direct array indexing eliminates hash computation, stripe-lock contention, and
  per-entry `Node<K,V>` heap allocations on every hot-path update.
* Each `SensorStatistics` entry guards its compound min/max/avg update with the
  new **.NET 9 `System.Threading.Lock`** (`using (_gate.EnterScope()) { … }`),
  so distinct sensors never contend.
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
  (`List<SensorSnapshot>[]`) and the pending-write list (`List<Task<int>>`) are
  pre-allocated once and reused across every flush — only `Clear()` is called,
  not a new allocation.
* `DummySlowDatabase.WriteAsync` returns a **`Task<int>`** — it always suspends
  on `Task.Delay`, so a `ValueTask` wrapper would provide no allocation benefit
  and was excluded to avoid misleading callers.
* Shards are written concurrently; completions are observed in *finish order*
  with **.NET 9 `Task.WhenEach`** instead of `WhenAll`.

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
duration with a deterministic shutdown sequence (producer completes the channel →
consumers drain → sink performs a final flush). `Program.cs` stays thin and also
wires `Ctrl+C` to a graceful early stop.

---

## Modern .NET / C# features on display

| Feature | Where | Why it matters here |
|---|---|---|
| **Native AOT** (`PublishAot`) | `Telemetry.Engine.csproj` | No JIT, instant startup, ~1.6 MB self-contained binary |
| **Bounded `Channel<T>`** | Module A | Lock-free handoff + back-pressure |
| **`ArrayPool<T>`** | Modules A/B | Zero steady-state GC for buffers |
| **`PreciseDelay` (`SpinWait` + `Stopwatch`)** | Module A | Sub-millisecond pacing without OS timer floor artifacts |
| **`Span`/`ReadOnlySpan<byte>`** | Module B | Allocation-free slicing & parsing |
| **`ref struct`** | `TelemetryParser` | Compiler-enforced no-heap-escape |
| **`[InlineArray]`** | `PayloadBuffer` | Inline, escapable, safe fixed buffer |
| **`in` parameters / `readonly record struct`** | throughout | Pass-by-ref-no-copy value semantics |
| **`SensorStatistics[]` direct array index** | Module C | O(1) lookup with no hashing, no stripe-lock, no node allocs |
| **`System.Threading.Lock` (.NET 9)** | Module C | Faster, scope-based mutual exclusion per sensor bucket |
| **`Interlocked`** | Module C | Wait-free hot counter, batched per batch not per reading |
| **Pre-allocated `ReadOnlySpan<SensorSnapshot>`** | Module C | Zero-alloc snapshot with bounded lifetime |
| **Pre-allocated shard buckets** | Module D | Zero-alloc flush path after construction |
| **`ValueTask`** | Module D | Allocation-free async on the empty-window path |
| **`Task.WhenEach` (.NET 9)** | Module D | React to shard completions as they land |
| **`PeriodicTimer`** | Modules C/D/Observability | Async-native, allocation-light timers |
| **`System.Diagnostics.Metrics` + `MeterListener`** | Observability | Native AOT-safe, zero-alloc instrumentation decoupled from consumers |

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

### Run the benchmarks (naive vs zero-allocation parser)

```bash
dotnet run -c Release --project benchmarks/Telemetry.Benchmarks
```

`[MemoryDiagnoser]` is enabled, so the report's **Allocated** column is the point
of interest: the naive parser allocates a throwaway array **and** a heap object
per reading (and a growing `List<T>`), while `TelemetryParser` allocates **0 B**
regardless of how many readings it decodes.

### Publish a Native AOT binary

```bash
dotnet publish src/Telemetry.Engine -c Release -r linux-x64
# → src/Telemetry.Engine/bin/Release/net10.0/linux-x64/publish/Telemetry.Engine  (~1.6 MB, no runtime required)
```

(Swap `linux-x64` for `win-x64` or `osx-arm64` as needed.)

---

## Design notes & trade-offs

* **Batching** is the single biggest throughput lever: pushing one reading at a
  time through the channel would pay synchronization cost 100k times/sec; batching
  amortizes it across ~1,000 readings.
* **Two-tier synchronization** matches each piece of state to its contention
  profile: a wait-free `Interlocked` counter for the single global hot integer,
  fine-grained per-bucket `Lock`s for compound per-sensor updates.
* **Array vs. dictionary for sensor state**: replacing `ConcurrentDictionary` with
  a pre-sized `SensorStatistics[]` trades the bounded sensor domain assumption for
  O(1) direct indexing with zero per-update allocation, no hash computation, and
  no stripe-lock contention across sensors.
* **Pre-allocated snapshot and flush buffers** eliminate the last remaining GC
  pressure on the hot path: `SensorAggregator.CreateSnapshot` reuses a fixed
  `SensorSnapshot[]`, and `AsyncDataSink` reuses its shard `List<T>` instances
  across every flush cycle.
* **`PreciseDelay`** prevents the OS timer floor from degrading pacing accuracy:
  on Windows the timer tick is ~15.6 ms, so any `Task.Delay` shorter than that
  parks the thread for a full tick. The two-phase hybrid (coarse sleep + `SpinWait`
  fine tail) delivers microsecond-accurate inter-batch cadence on every platform.
* **Observability is fully decoupled**: `ConsoleMetricsExporter` never imports
  `EngineMetrics` — it matches by meter name. The producing and observing sides
  are order-independent and can be swapped for any `MeterListener`-compatible
  consumer (OpenTelemetry, Prometheus, etc.) with no changes to the engine.
* **Graceful shutdown is explicit**, not abrupt: cancellation stops the producer,
  which completes the channel writer; consumers then drain every in-flight batch
  (no data loss) before the sink performs one final flush.
* The numbers reported (e.g. `999,000`) reflect the wall-clock pacing window; the
  engine sustains the full 100k/sec rate every second of the run.
