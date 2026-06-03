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
└──────────────┘  (ArrayPool)  └──────────────┘                      └──────┬───────┘
                                                                            │ snapshot
                                                                            ▼
                                                                     ┌──────────────┐
                                                                     │  Async Sink  │
                                                                     │  (Module D)  │
                                                                     │ IAsyncEnum + │
                                                                     │ Task.WhenEach│
                                                                     └──────────────┘
```

---

## Architecture

The solution is split along clean separation-of-concerns boundaries:

```
HighThroughputTelemetry.sln
├── src/Telemetry.Engine/            # the engine (Native AOT console app)
│   ├── Domain/                      # SensorReading value type + wire layout
│   ├── Parsing/                     # Module B — zero-allocation codec & parser
│   ├── Producer/                    # Module A — firehose + pooled batch envelope
│   ├── Aggregation/                 # Module C — concurrent statistics
│   ├── Sink/                        # Module D — async flushing to slow I/O
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
  `System.Threading.Interlocked`.
* Per-sensor statistics live in a **`ConcurrentDictionary`**; each bucket guards
  its compound min/max/avg update with the new **.NET 9 `System.Threading.Lock`**
  (`using (gate.EnterScope()) { … }`), so distinct sensors never contend.

### Module D — Asynchronous Data Sink
[`Sink/`](src/Telemetry.Engine/Sink/)

* Flushes on a **`PeriodicTimer`** cadence, sampling the aggregator as an
  **`IAsyncEnumerable<SensorSnapshot>`** stream.
* `DummySlowDatabase.WriteAsync` returns a **`ValueTask<int>`** (no `Task`
  allocation on the cheap path).
* Snapshots are sharded and written concurrently; completions are observed in
  *finish order* with **.NET 9 `Task.WhenEach`** instead of `WhenAll`.

### Orchestration
[`Orchestration/TelemetryPipeline.cs`](src/Telemetry.Engine/Orchestration/TelemetryPipeline.cs)
is the composition root that wires the channel, the consumer fan-out, the
aggregator, the sink and a live console reporter, then runs them for a bounded
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
| **`Span`/`ReadOnlySpan<byte>`** | Module B | Allocation-free slicing & parsing |
| **`ref struct`** | `TelemetryParser` | Compiler-enforced no-heap-escape |
| **`[InlineArray]`** | `PayloadBuffer` | Inline, escapable, safe fixed buffer |
| **`in` parameters / `readonly record struct`** | throughout | Pass-by-ref-no-copy value semantics |
| **`System.Threading.Lock` (.NET 9)** | Module C | Faster, scope-based mutual exclusion |
| **`Interlocked`** | Module C | Wait-free hot counter |
| **`ConcurrentDictionary`** | Module C | Sharded concurrent lookup |
| **`IAsyncEnumerable<T>`** | Module D | Async streaming snapshots |
| **`ValueTask`** | Module D | Allocation-free async on the common path |
| **`Task.WhenEach` (.NET 9)** | Module D | React to completions as they land |
| **`PeriodicTimer`** | Modules C/D | Async-native, allocation-light timers |

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
[  1.0s] throughput:  100,000 readings/s | total:    100,000 | sensors:  64
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
* **Graceful shutdown is explicit**, not abrupt: cancellation stops the producer,
  which completes the channel writer; consumers then drain every in-flight batch
  (no data loss) before the sink performs one final flush.
* The numbers reported (e.g. `999,000`) reflect the wall-clock pacing window; the
  engine sustains the full 100k/sec rate every second of the run.
