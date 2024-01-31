using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Text;

namespace _1brc
{
    public readonly unsafe struct Utf8Span : IEquatable<Utf8Span>
    {
        internal readonly byte* Pointer;
        internal readonly nuint Length;

        public Utf8Span(byte* pointer, nuint length)
        {
            Pointer = pointer;
            Length = length;
        }

        /// <summary>
        /// Ref MUST be to a pinned or native memory
        /// </summary>
        public Utf8Span(ref byte pointer, nuint length)
        {
            Pointer = (byte*)Unsafe.AsPointer(ref pointer);
            Length = length;
        }

        public ReadOnlySpan<byte> Span
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => new(Pointer, (int)(uint)Length);
        }

        /// <summary>
        /// Slice without bound checks. Use only when the bounds are checked/ensured before the call.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Utf8Span SliceUnsafe(nuint start, nuint length) => new(Pointer + start, length);

        /// <summary>
        /// Slice without bound checks. Use only when the bounds are checked/ensured before the call.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Utf8Span SliceUnsafe(nuint start) => new(Pointer + start, Length - start);

        // Static data, no allocations. It's inlined in an assembly and has a fixed address.
        // ReSharper disable RedundantExplicitArraySize : it's very useful to ensure the right size.
        internal static ReadOnlySpan<byte> OnesAfterLength => new byte[64]
        {
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        };

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(Utf8Span other) //, bool isSimdSafe)
        {
            // With good hashing the values are almost always equal, but we need to prove that.
            // Span.SequenceEqual is the best general purpose API for comparing memory equality.
            // But it's not inlined and quite heavy for short values.

            // This solution is extremely fast for up to 32 bytes.
            // However, from inside the Utf8Span we cannot guarantee that dereferencing of a vector
            // will not segfault. To avoid such a problem we ensure that it's safe to touch 64 bytes
            // from every new line in a chunk.

            if (Length != other.Length)
                return false;

            const nuint vectorSize = 32;

            if (Vector256.IsHardwareAccelerated)
            {
                var equalsMask = Vector256.Equals(Vector256.Load(Pointer), Vector256.Load(other.Pointer))
                                 | Vector256.LessThanOrEqual(
                                     Vector256.Create((byte)Length),
                                     Vector256.Create((byte)0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31)
                                 );

                if (equalsMask.ExtractMostSignificantBits() != uint.MaxValue)
                    return false;

                return Length <= vectorSize || EqualsCont(this, other);
            }

            return Span.SequenceEqual(other.Span);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static bool EqualsCont(Utf8Span left, Utf8Span right)
            {
                var bytes = Vector256.Load(left.Pointer + vectorSize);
                var otherBytes = Vector256.Load(right.Pointer + vectorSize);
                var bytesAnd = Vector256.Equals(bytes, otherBytes);
                if (left.Length <= vectorSize * 2)
                {
                    bytesAnd |= Vector256.LoadUnsafe(in MemoryMarshal.GetReference(OnesAfterLength), vectorSize * 2 - left.Length);
                    return uint.MaxValue == bytesAnd.ExtractMostSignificantBits();
                }

                if (uint.MaxValue != bytesAnd.ExtractMostSignificantBits())
                    return false;

                bytes = Vector256.Load(left.Pointer + vectorSize * 2);
                otherBytes = Vector256.Load(right.Pointer + vectorSize * 2);
                bytesAnd = Vector256.Equals(bytes, otherBytes);
                if (left.Length <= vectorSize * 3)
                {
                    bytesAnd |= Vector256.LoadUnsafe(in MemoryMarshal.GetReference(OnesAfterLength), vectorSize * 3 - left.Length);
                    return uint.MaxValue == bytesAnd.ExtractMostSignificantBits();
                }

                if (uint.MaxValue != bytesAnd.ExtractMostSignificantBits())
                    return false;

                bytes = Vector256.Load(left.Pointer + vectorSize * 3);
                otherBytes = Vector256.Load(right.Pointer + vectorSize * 3);
                bytesAnd = Vector256.Equals(bytes, otherBytes);
                bytesAnd |= Vector256.LoadUnsafe(in MemoryMarshal.GetReference(OnesAfterLength), vectorSize * 4 - left.Length);
                return uint.MaxValue == bytesAnd.ExtractMostSignificantBits();
            }
        }

        public override bool Equals(object? obj) => obj is Utf8Span other && Equals(other);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override int GetHashCode()
        {
            // Here we use the first 4 chars (if ASCII) and the length for a hash.
            // The worst case would be a prefix such as Port/Saint and the same length,
            // which for human geo names is quite rare. 

            // .NET dictionary will obviously slow down with collisions but will still work.

            // Note that by construction we have `;` after the name, so hashing `abc;` or `a;` is fine.
            // Utf8Span points to a large blob that always has values beyond the length 

            const uint prime = 16777619u;

            return Length >= 3 // no, moving condition inside does not help
                ? (int)((*(uint*)Pointer * prime) ^ ((uint)Length))
                : (int)((*(ushort*)Pointer * prime) ^ ((uint)Length));

        }

        public override string ToString() => new((sbyte*)Pointer, 0, (int)Length, Encoding.UTF8);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nint ParseInt(byte* ptr, nuint start, out nuint lfIndex)
        {
            // I took it from artsiomkorzun, but he mentions merykitty, while noahfalk mentions RagnarGrootKoerkamp. The trace is lost

            const long DOT_BITS = 0x10101000;
            const long MAGIC_MULTIPLIER = (100 * 0x1000000 + 10 * 0x10000 + 1);

            long word = *(long*)(ptr + start + 1);
            long inverted = ~word;
            int dot = BitOperations.TrailingZeroCount(inverted & DOT_BITS);
            long signed = (inverted << 59) >> 63;
            long mask = ~(signed & 0xFF);
            long digits = ((word & mask) << (28 - dot)) & 0x0F000F0F00L;
            long abs = ((digits * MAGIC_MULTIPLIER) >>> 32) & 0x3FF;
            var value = ((abs ^ signed) - signed);
            lfIndex = start + (uint)(dot >> 3) + 4u;
            return (nint)value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public nint ParseInt(nuint start, out nuint lfIndex) => ParseInt(Pointer, start, out lfIndex);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal nuint IndexOfSemicolon() => Vector256.IsHardwareAccelerated ? IndexOfSemicolon(Pointer) : IndexOf(0, (byte)';');

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static nuint IndexOfSemicolon(byte* ptr)
        {
            var matches = Vector256.Equals(Unsafe.ReadUnaligned<Vector256<byte>>(ptr), Vector256.Create((byte)';'));
            var mask = matches.ExtractMostSignificantBits();

            nuint idx = mask == 0 ? IndexOfSemicolonLong(ptr) : (uint)BitOperations.TrailingZeroCount(mask);
            return idx;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static nuint IndexOfSemicolonLong(byte* ptr)
        {
            const nuint vectorSize = 32;

            var matches = Vector256.Equals(Unsafe.ReadUnaligned<Vector256<byte>>(ptr + vectorSize), Vector256.Create((byte)';'));
            var mask = matches.ExtractMostSignificantBits();
            var tzc = (uint)BitOperations.TrailingZeroCount(mask);
            if (mask != 0)
                return vectorSize + tzc;

            const nuint vectorSize2 = 2 * vectorSize;
            matches = Vector256.Equals(Unsafe.ReadUnaligned<Vector256<byte>>(ptr + vectorSize2), Vector256.Create((byte)';'));
            mask = matches.ExtractMostSignificantBits();
            tzc = (uint)BitOperations.TrailingZeroCount(mask);
            if (mask != 0)
                return vectorSize2 + tzc;

            const nuint vectorSize3 = 3 * vectorSize;
            matches = Vector256.Equals(Unsafe.ReadUnaligned<Vector256<byte>>(ptr + vectorSize3), Vector256.Create((byte)';'));
            mask = matches.ExtractMostSignificantBits();
            tzc = (uint)BitOperations.TrailingZeroCount(mask);
            return vectorSize3 + tzc;
        }

        /// <summary>
        /// Generic fallback
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private nuint IndexOf(nuint start, byte needle)
        {
            var indexOf = (nuint)SliceUnsafe(start).Span.IndexOf(needle);
            return (nint)indexOf < 0 ? Length : start + indexOf;
        }
    }
}