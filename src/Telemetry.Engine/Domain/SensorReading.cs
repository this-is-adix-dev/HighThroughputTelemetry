namespace Telemetry.Engine.Domain;

/// <summary>
/// A single telemetry reading in its decoded, in-memory form.
///
/// Modelled as a <c>readonly record struct</c> on purpose:
/// <list type="bullet">
///   <item>It is a <b>value type</b>, so it lives on the stack / inline inside
///   spans and never causes a heap allocation when we parse millions per second.</item>
///   <item><c>readonly</c> guarantees immutability — safe to share across the
///   producer / consumer threads without defensive copies.</item>
///   <item>The <c>record</c> modifier gives us value equality and a tidy
///   <c>ToString()</c> for free, which is handy in tests and diagnostics.</item>
/// </list>
///
/// The on-wire layout is a fixed 16 bytes (little-endian):
/// <code>
///   | 0          3 | 4                    11 | 12        15 |
///   |  SensorId    |     TimestampTicks      |    Value     |
///   |  Int32 (4B)  |       Int64 (8B)        | Single (4B)  |
/// </code>
/// </summary>
public readonly record struct SensorReading(int SensorId, long TimestampTicks, float Value)
{
    /// <summary>Size of a single serialized payload, in bytes. The whole system is built around this constant.</summary>
    public const int Size = 16;

    // Field offsets inside the 16-byte frame. Kept here so the codec, the parser
    // and any external tooling all agree on a single source of truth.
    public const int SensorIdOffset = 0;
    public const int TimestampOffset = 4;
    public const int ValueOffset = 12;

    /// <summary>Convenience view of the timestamp as a UTC <see cref="DateTime"/>.</summary>
    public DateTime TimestampUtc => new(TimestampTicks, DateTimeKind.Utc);
}
