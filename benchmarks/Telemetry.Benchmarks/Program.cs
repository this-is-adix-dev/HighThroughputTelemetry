using BenchmarkDotNet.Running;

// Entry point for the benchmark harness. BenchmarkSwitcher exposes every suite and honours
// --filter / interactive selection, so a single launch can target one suite or run them all:
//   dotnet run -c Release --project benchmarks/Telemetry.Benchmarks                         (pick interactively)
//   dotnet run -c Release --project benchmarks/Telemetry.Benchmarks -- --filter *Contention* (one suite)
//   dotnet run -c Release --project benchmarks/Telemetry.Benchmarks -- --filter *            (run all)
//
// Suites:
//   ParserBenchmarks     — zero-allocation span parser vs. the allocation-heavy naive parser.
//   ContentionBenchmarks — shared-locked accumulator vs. per-consumer sharded (Red-Flag-B proof).
//   PacingBenchmarks     — PreciseDelay accuracy/CPU trade-off vs. plain Task.Delay.
BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);

// A named partial Program type so typeof(Program) resolves under top-level statements.
public partial class Program;
