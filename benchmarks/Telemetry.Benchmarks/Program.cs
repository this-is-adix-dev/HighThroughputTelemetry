using BenchmarkDotNet.Running;
using Telemetry.Benchmarks;

// Entry point for the benchmark harness. Run with:
//   dotnet run -c Release --project benchmarks/Telemetry.Benchmarks
BenchmarkRunner.Run<ParserBenchmarks>();
