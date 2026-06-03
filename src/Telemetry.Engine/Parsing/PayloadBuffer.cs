using System.Runtime.CompilerServices;

namespace Telemetry.Engine.Parsing;

/// <summary>
/// A fixed-size, 16-byte inline buffer for staging a single serialized reading.
///
/// This is a C# 12+ <see cref="InlineArrayAttribute"/> type. The runtime lays the
/// 16 bytes out <b>inline inside the struct itself</b> (no separate array object on
/// the heap), and the language lets us treat the value directly as a
/// <see cref="Span{T}"/> / <see cref="ReadOnlySpan{T}"/>.
///
/// Why use it here instead of <c>stackalloc</c> or an <c>unsafe fixed byte[16]</c>?
/// <list type="bullet">
///   <item>It is a real <b>value type you can return by value</b> from a method —
///   so an encoder can hand back a fully populated 16-byte frame with zero heap
///   allocation, something <c>stackalloc</c> cannot do (a stackalloc span cannot
///   escape its method).</item>
///   <item>It is completely <b>safe</b> code — no <c>unsafe</c>, no pinning — which
///   keeps it Native-AOT and trim friendly.</item>
/// </list>
/// </summary>
[InlineArray(Domain.SensorReading.Size)]
public struct PayloadBuffer
{
    // A single element field is all an [InlineArray] needs; the attribute's length
    // argument tells the runtime to reserve room for N contiguous copies of it.
    private byte _element0;
}
