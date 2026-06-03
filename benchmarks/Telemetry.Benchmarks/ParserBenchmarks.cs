using BenchmarkDotNet.Attributes;
using Telemetry.Engine.Domain;
using Telemetry.Engine.Parsing;

namespace Telemetry.Benchmarks;

/// <summary>
/// Head-to-head comparison of the naive, allocation-heavy parser against the
/// zero-allocation <see cref="TelemetryParser"/> from Module B.
///
/// <see cref="MemoryDiagnoser"/> is the headline here: it reports bytes allocated
/// and GC collections per operation, which is where the two approaches diverge by
/// orders of magnitude even when wall-clock time looks "close enough".
/// </summary>
[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class ParserBenchmarks
{
    [Params(1_000, 10_000)]
    public int ReadingCount { get; set; }

    private byte[] _buffer = [];

    [GlobalSetup]
    public void Setup()
    {
        // Build one fixed buffer of serialized frames shared by both benchmarks so
        // they decode byte-for-byte identical input.
        _buffer = new byte[ReadingCount * SensorReading.Size];
        var random = new Random(1789);

        for (int i = 0; i < ReadingCount; i++)
        {
            var reading = new SensorReading(
                SensorId: random.Next(64),
                TimestampTicks: DateTime.UtcNow.Ticks,
                Value: 20f + (float)(random.NextDouble() * 60.0));

            TelemetryCodec.Encode(in reading, _buffer.AsSpan(i * SensorReading.Size, SensorReading.Size));
        }
    }

    /// <summary>Baseline: allocates a temp array + a heap object per reading.</summary>
    [Benchmark(Baseline = true)]
    public double Naive_AllocationHeavy()
    {
        List<NaiveTelemetryParser.HeapReading> readings = NaiveTelemetryParser.Parse(_buffer, ReadingCount);

        // Consume the result so the JIT can't elide the work.
        double sum = 0;
        foreach (NaiveTelemetryParser.HeapReading r in readings)
            sum += r.Value;
        return sum;
    }

    /// <summary>Optimized: a ref-struct cursor over spans — zero heap allocation.</summary>
    [Benchmark]
    public double ZeroAllocation_Span()
    {
        var parser = new TelemetryParser(_buffer);

        double sum = 0;
        while (parser.TryReadNext(out SensorReading reading))
            sum += reading.Value;
        return sum;
    }
}
