using System.Diagnostics.Metrics;
using Telemetry.Engine.Domain;

namespace Telemetry.Engine.Parsing;

/// <summary>
/// A forward-only, allocation-free cursor over a buffer containing many back-to-back
/// 32-byte signed frames (16 bytes of data + a 16-byte truncated HMAC).
///
/// Declared as a <c>ref struct</c> so the compiler <b>guarantees</b> it can never
/// escape to the heap (no boxing, no fields in classes, no async capture). That is
/// what makes it safe to wrap a <see cref="ReadOnlySpan{T}"/> that may point at a
/// pooled or stack buffer: the parser simply cannot outlive the memory it reads.
/// (A <c>ref struct</c> may still hold ordinary managed references such as the optional
/// metrics <see cref="Counter{T}"/> below — what it cannot do is live on the heap
/// itself.)
///
/// Every frame is integrity-checked as it is read. A frame whose HMAC does not verify
/// is silently dropped (and counted) so a single tampered packet cannot abort the
/// decode of the rest of the batch.
///
/// Usage:
/// <code>
///   var parser = new TelemetryParser(batchBytes, metrics.RejectedTampered);
///   while (parser.TryReadNext(out SensorReading r)) { /* ... */ }
/// </code>
/// </summary>
public ref struct TelemetryParser
{
    private readonly ReadOnlySpan<byte> _buffer;

    // Optional, tag-less counter incremented once per frame that fails verification.
    // Counter<long>.Add(long) is heap-free, and the null-conditional collapses to a
    // cheap branch when no counter is wired (e.g. in unit tests), so accounting for a
    // rejected frame still costs zero allocations on the hot path.
    private readonly Counter<long>? _rejectedTamperedCounter;

    private int _position;

    public TelemetryParser(ReadOnlySpan<byte> buffer, Counter<long>? rejectedTamperedCounter = null)
    {
        _buffer = buffer;
        _rejectedTamperedCounter = rejectedTamperedCounter;
        _position = 0;
    }

    /// <summary>Number of whole 32-byte frames remaining from the current position.</summary>
    public readonly int Remaining => (_buffer.Length - _position) / TelemetryCodec.FrameSize;

    /// <summary>
    /// Try to decode the next <b>authentic</b> frame. Tampered frames (failed HMAC) are
    /// skipped and counted, and decoding continues past them; the method only returns
    /// <c>false</c> once fewer than a whole frame's worth of bytes remain. No allocation
    /// occurs on any path.
    /// </summary>
    public bool TryReadNext(out SensorReading reading)
    {
        // Loop rather than returning false on a mismatch: a corrupt frame must not abort
        // the whole batch, so we drop it and advance to the next candidate.
        while (_buffer.Length - _position >= TelemetryCodec.FrameSize)
        {
            // Slice the next frame without copying, then split it into its data and
            // signature halves. Both slices are provably in range given the check above.
            ReadOnlySpan<byte> frame = _buffer.Slice(_position, TelemetryCodec.FrameSize);
            _position += TelemetryCodec.FrameSize;

            ReadOnlySpan<byte> data = frame[..SensorReading.Size];
            ReadOnlySpan<byte> signature = frame[SensorReading.Size..];

            // Re-derive the HMAC over the data and compare it to the on-wire signature in
            // constant time (see TelemetryCodec.Verify). A mismatch means the bytes were
            // altered after signing — drop and count the frame, then keep scanning.
            if (!TelemetryCodec.Verify(data, signature))
            {
                _rejectedTamperedCounter?.Add(1);
                continue;
            }

            reading = TelemetryCodec.Decode(data);
            return true;
        }

        reading = default;
        return false;
    }
}
