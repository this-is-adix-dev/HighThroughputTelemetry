using System.Globalization;
using Telemetry.Engine.Orchestration;

// Top-level statements keep the entry point minimal: all real wiring lives in
// TelemetryPipeline. This file just configures the run, handles Ctrl+C, and prints
// the final report.

CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;

Console.WriteLine("HighThroughputTelemetry — high-load / low-latency ingestion simulation");
Console.WriteLine("=====================================================================");
Console.WriteLine("Target: 100,000 readings/sec for 10 seconds. Press Ctrl+C to stop early.");
Console.WriteLine();

// Allow a clean early exit via Ctrl+C without tearing the process down abruptly.
using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, eventArgs) =>
{
    eventArgs.Cancel = true; // keep the process alive; let the pipeline drain.
    cts.Cancel();
};

var pipeline = new TelemetryPipeline(new PipelineOptions());
PipelineReport report = await pipeline.RunAsync(cts.Token);

Console.WriteLine();
Console.WriteLine("=====================================================================");
Console.WriteLine("Simulation complete.");
Console.WriteLine($"  Elapsed wall-clock      : {report.Elapsed.TotalSeconds:F2} s");
Console.WriteLine($"  Readings produced       : {report.Produced:N0}");
Console.WriteLine($"  Readings processed      : {report.Processed:N0}");
Console.WriteLine($"  Effective throughput    : {report.Processed / Math.Max(report.Elapsed.TotalSeconds, 0.001):N0} readings/s");
Console.WriteLine($"  Distinct sensors        : {report.DistinctSensors:N0}");
Console.WriteLine($"  Sink flushes            : {report.Flushes:N0}");
Console.WriteLine($"  Rows persisted to sink  : {report.RowsPersisted:N0}");
Console.WriteLine("=====================================================================");

return 0;
