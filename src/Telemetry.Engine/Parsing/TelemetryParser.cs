using System.Runtime.CompilerServices;
using Telemetry.Engine.Domain;

namespace Telemetry.Engine.Parsing;

/// <summary>
/// A forward-only, allocation-free cursor over a buffer containing many
/// back-to-back 16-byte frames.
///
/// Declared as a <c>ref struct</c> so the compiler <b>guarantees</b> it can never
/// escape to the heap (no boxing, no fields in classes, no async capture). That is
/// what makes it safe to wrap a <see cref="ReadOnlySpan{T}"/> that may point at a
/// pooled or stack buffer: the parser simply cannot outlive the memory it reads.
///
/// Usage:
/// <code>
///   var parser = new TelemetryParser(batchBytes);
///   while (parser.TryReadNext(out SensorReading r)) { /* ... */ }
/// </code>
/// </summary>
public ref struct TelemetryParser
{
    private readonly ReadOnlySpan<byte> _buffer;
    private int _position;

    public TelemetryParser(ReadOnlySpan<byte> buffer)
    {
        _buffer = buffer;
        _position = 0;
    }

    /// <summary>Number of whole frames remaining from the current position.</summary>
    public readonly int Remaining => (_buffer.Length - _position) / SensorReading.Size;

    /// <summary>
    /// Try to decode the next frame. Returns <c>false</c> once fewer than 16 bytes
    /// remain. No allocation occurs on either path.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryReadNext(out SensorReading reading)
    {
        // Slice the next frame without copying, then hand it to the codec.
        ReadOnlySpan<byte> remaining = _buffer[_position..];
        if (remaining.Length < SensorReading.Size)
        {
            reading = default;
            return false;
        }

        reading = TelemetryCodec.Decode(remaining);
        _position += SensorReading.Size;
        return true;
    }
}
