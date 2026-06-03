using System.Buffers.Binary;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using Telemetry.Engine.Domain;
using Telemetry.Engine.Parsing;

namespace Telemetry.Engine.Processing;

/// <summary>
/// A lightning-fast, allocation-free pre-screen that answers a single yes/no question
/// about a whole <c>TelemetryBatch</c>: <i>does ANY reading in here breach a critical
/// threshold?</i> It is meant to run <b>once per batch, before</b> the per-frame parse
/// + HMAC verification, as a cheap gate that flags alarming batches for special
/// attention without paying the full decode cost up front.
///
/// <para><b>The data-layout problem.</b> The batch is an <i>Array of Structs</i> (AoS):
/// each 32-byte frame is <c>[SensorId | Timestamp | Value | HMAC]</c>, so the only field
/// we care about — the 4-byte <see cref="SensorReading.Value"/> at byte offset
/// <see cref="SensorReading.ValueOffset"/> (12) — is buried inside every frame and
/// repeats every <see cref="TelemetryCodec.FrameSize"/> (32) bytes. The interesting
/// floats are therefore <i>strided</i>, never contiguous, which defeats a plain
/// <c>MemoryMarshal.Cast&lt;byte, float&gt;</c> + straight vector load.</para>
///
/// <para><b>The SIMD answer.</b> On AVX2 hardware we use the <c>vgatherdps</c> gather
/// instruction (<see cref="Avx2.GatherVector256(float*, Vector256{int}, byte)"/>) to pull
/// eight strided <c>Value</c> floats — one from each of eight consecutive frames — into a
/// single <see cref="Vector256{Single}"/> register in one shot, then compare all eight
/// against a broadcast threshold with one
/// <see cref="Vector256.GreaterThanAny{T}(Vector256{T}, Vector256{T})"/>. That is eight
/// readings examined per loop iteration instead of one.</para>
///
/// <para><b>Why this is vastly superior to a scalar scan.</b> A sequential loop issues a
/// load, a compare and a branch <i>per reading</i>; the per-element branch is also
/// unpredictable (an anomaly may appear anywhere), so the CPU mispredicts and stalls. The
/// SIMD path collapses eight loads into one gather micro-op stream, eight compares into one
/// instruction, and eight branches into a single <c>GreaterThanAny</c> test — turning a
/// memory- and branch-bound scan into a near branch-free, throughput-bound sweep. For the
/// large buffers this engine moves (1,000 frames/batch at 100k readings/s) that is the
/// difference between a noticeable per-batch tax and effectively free screening.</para>
///
/// <para><b>AOT &amp; allocation.</b> Hardware intrinsics and <c>Vector256</c> are
/// first-class in the Native AOT compiler (no JIT fallback, no reflection), and every path
/// here works over the caller's span with zero heap traffic — no arrays, no boxing.</para>
/// </summary>
public static class BatchAnomalyDetector
{
    // The Value field, expressed in float-element units rather than bytes, because the
    // gather index vector below indexes the buffer as an array of floats (scale = 4).
    // Both quantities are derived from the single-source-of-truth frame constants so this
    // detector can never drift out of sync with the wire layout.
    private const int FloatsPerFrame = TelemetryCodec.FrameSize / sizeof(float); // 32 / 4 = 8
    private const int ValueFloatIndex = SensorReading.ValueOffset / sizeof(float); // 12 / 4 = 3

    /// <summary>
    /// Scan <paramref name="batch"/> — a buffer of back-to-back 32-byte frames, exactly
    /// what <c>TelemetryBatch.Span</c> exposes — and return <c>true</c> if any frame's
    /// <see cref="SensorReading.Value"/> is strictly greater than
    /// <paramref name="criticalThreshold"/>.
    /// </summary>
    /// <remarks>
    /// This is a deliberately fast, pre-verification heuristic: it reads the raw
    /// <c>Value</c> bytes without first checking each frame's HMAC, so a (rare) tampered
    /// frame could in principle contribute a spurious reading. That trade-off is the whole
    /// point — it screens an entire batch for "is anything alarming in here at all?" far
    /// more cheaply than a full decode, and authenticity is still enforced per frame by the
    /// downstream <c>TelemetryParser</c> before any reading is aggregated.
    /// </remarks>
    public static bool HasCriticalAnomalies(ReadOnlySpan<byte> batch, float criticalThreshold)
    {
        int frameCount = batch.Length / TelemetryCodec.FrameSize;
        if (frameCount == 0)
            return false;

        // Take the hardware-gather fast path only when the runtime reports an accelerated
        // 256-bit vector unit AND AVX2 specifically is present (the gather intrinsic lives
        // in AVX2). Anything else — older x86, ARM/NEON, WASM — falls through to the
        // correct, portable scalar loop. We also need at least one full vector's worth of
        // frames for the gather to be worthwhile; smaller batches go straight to scalar.
        if (Vector256.IsHardwareAccelerated && Avx2.IsSupported && frameCount >= Vector256<float>.Count)
            return HasCriticalAnomaliesAvx2(batch, criticalThreshold, frameCount);

        return HasCriticalAnomaliesScalar(batch, criticalThreshold, startFrame: 0, frameCount);
    }

    /// <summary>
    /// AVX2 fast path: gather eight strided <c>Value</c> floats per iteration and test them
    /// against the threshold with a single SIMD comparison. Falls back to the scalar helper
    /// for the trailing &lt; 8 frames that don't fill a vector.
    /// </summary>
    private static unsafe bool HasCriticalAnomaliesAvx2(
        ReadOnlySpan<byte> batch,
        float criticalThreshold,
        int frameCount)
    {
        // The float-element index of each lane's Value, for eight consecutive frames:
        // frame i's Value is at float index i*FloatsPerFrame + ValueFloatIndex
        // = i*8 + 3  ->  { 3, 11, 19, 27, 35, 43, 51, 59 }.
        // The JIT lowers this constant Create into a single vector load from rodata.
        Vector256<int> valueIndices = Vector256.Create(
            0 * FloatsPerFrame + ValueFloatIndex,
            1 * FloatsPerFrame + ValueFloatIndex,
            2 * FloatsPerFrame + ValueFloatIndex,
            3 * FloatsPerFrame + ValueFloatIndex,
            4 * FloatsPerFrame + ValueFloatIndex,
            5 * FloatsPerFrame + ValueFloatIndex,
            6 * FloatsPerFrame + ValueFloatIndex,
            7 * FloatsPerFrame + ValueFloatIndex);

        // Broadcast the threshold into all eight lanes once, outside the loop.
        Vector256<float> threshold = Vector256.Create(criticalThreshold);

        // Number of whole 8-frame groups, and how far (in floats) to advance the base
        // pointer after each group: 8 frames × 8 floats/frame = 64 floats = 256 bytes.
        int vectorCount = Vector256<float>.Count;      // 8 lanes
        int groups = frameCount / vectorCount;         // full groups of 8 frames
        const int floatsPerGroup = 8 * FloatsPerFrame; // 64 floats advanced per group

        // Pin the span so the GC can't relocate it under the raw pointer the gather needs.
        // The gather path is only ever taken on x86 (little-endian), whose native float
        // layout matches our little-endian wire format, so reinterpreting the bytes as
        // floats requires no byte-swap — the scalar fallback handles endianness explicitly
        // for any other architecture.
        fixed (byte* basePtr = batch)
        {
            float* values = (float*)basePtr;

            for (int g = 0; g < groups; g++)
            {
                // ONE instruction, EIGHT loads: result[lane] = values[valueIndices[lane]].
                // scale = sizeof(float) (4) tells the CPU the indices are in float units,
                // so it computes basePtr + index*4 bytes for each of the eight lanes.
                Vector256<float> gathered = Avx2.GatherVector256(values, valueIndices, (byte)sizeof(float));

                // ONE instruction, EIGHT compares + a horizontal OR: did any lane breach?
                // Short-circuit the moment a breach appears — an alarming batch is detected
                // without scanning the rest of it.
                if (Vector256.GreaterThanAny(gathered, threshold))
                    return true;

                values += floatsPerGroup; // step to the next group of eight frames
            }
        }

        // Sweep the leftover frames (frameCount mod 8) that never filled a vector.
        return HasCriticalAnomaliesScalar(batch, criticalThreshold, startFrame: groups * vectorCount, frameCount);
    }

    /// <summary>
    /// Portable, branch-simple fallback: read each frame's <c>Value</c> with the
    /// endianness-correct <see cref="BinaryPrimitives"/> reader and compare. Used wholesale
    /// on non-AVX2 hardware, and for the &lt; 8-frame tail of the SIMD path.
    /// </summary>
    private static bool HasCriticalAnomaliesScalar(
        ReadOnlySpan<byte> batch,
        float criticalThreshold,
        int startFrame,
        int frameCount)
    {
        for (int i = startFrame; i < frameCount; i++)
        {
            int valueOffset = i * TelemetryCodec.FrameSize + SensorReading.ValueOffset;
            float value = BinaryPrimitives.ReadSingleLittleEndian(batch.Slice(valueOffset));
            if (value > criticalThreshold)
                return true;
        }

        return false;
    }
}
