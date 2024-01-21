using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
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

            if (Vector256.IsHardwareAccelerated && Length <= (uint)Vector256<byte>.Count)
            {
                if (Length != other.Length)
                    return false;

                var mask = Vector256.LoadUnsafe(in MemoryMarshal.GetReference(OnesAfterLength), (uint)Vector256<byte>.Count - Length);
                var bytes = Vector256.Load(Pointer);
                var otherBytes = Vector256.Load(other.Pointer);
                var bytesAnd = Vector256.Equals(bytes, otherBytes) | mask;
                var msbMask = Vector256.ExtractMostSignificantBits(bytesAnd);
                var equals = msbMask == uint.MaxValue;
                return equals;
            }

            return Span.SequenceEqual(other.Span);
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

            if (Length >= 3)
                return (int)(((uint)Length * prime) ^ (*(uint*)(Pointer)));

            return (int)(uint)(*(ushort*)Pointer * prime);
        }

        public override string ToString() => new((sbyte*)Pointer, 0, (int)Length, Encoding.UTF8);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public nint ParseInt(nuint start, out nuint lfIndex)
        {
            var ptr = Pointer + start + 1;
            int sign;

            if (*ptr == (byte)'-')
            {
                ptr++;
                sign = -1;
                lfIndex = start + 6;
            }
            else
            {
                sign = 1;
                lfIndex = start + 5;
            }

            if (ptr[1] != '.')
            {
                lfIndex++;
                return (nint)(ptr[0] * 100u + ptr[1] * 10u + ptr[3] - '0' * 111u) * sign;
            }

            return (nint)(ptr[0] * 10u + ptr[2] - ('0' * 11u)) * sign;
        }

        /// <summary>
        /// Spec: Station name: non null UTF-8 string of min length 1 character and max length 100 bytes (i.e. this could be 100 one-byte characters, or 50 two-byte characters, etc.)
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal nuint IndexOfSemicolon()
        {
            const nuint vectorSize = 32;
            // nuint start = 0; // it's consistently faster with this useless variable (non constant)

            if (Vector256.IsHardwareAccelerated)
            {
                var sepVec = Vector256.Create((byte)';');

                var matches = Vector256.Equals(Unsafe.ReadUnaligned<Vector256<byte>>(Pointer), sepVec);
                var mask = (uint)Avx2.MoveMask(matches);
                var tzc = (uint)BitOperations.TrailingZeroCount(mask);

                if (mask == 0) // For non-taken branches prefer placing them in a "leaf" instead of mask != 0, somewhere on GH they explain why, it would be nice to find. 
                    return IndexOfSemicolonCont(this);

                return tzc;

                [MethodImpl(MethodImplOptions.NoInlining)]
                static nuint IndexOfSemicolonCont(Utf8Span span)
                {
                    // A nicer version would be just a recursive call, even not here but above instead of this function.
                    // It's as fast for the default case and very close for 10K. Yet, this manually unrolled continuation is faster for 10K.   
                    // return vectorSize + span.SliceUnsafe(vectorSize).IndexOfSemicolon();
                    
                    var sepVec = Vector256.Create((byte)';');
                    var matches = Vector256.Equals(Unsafe.ReadUnaligned<Vector256<byte>>(span.Pointer + vectorSize), sepVec);
                    var mask = (uint)Avx2.MoveMask(matches);
                    var tzc = (uint)BitOperations.TrailingZeroCount(mask);
                    if (mask != 0)
                        return vectorSize + tzc;

                    const nuint vectorSize2 = 2 * vectorSize;
                    matches = Vector256.Equals(Unsafe.ReadUnaligned<Vector256<byte>>(span.Pointer + vectorSize2), sepVec);
                    mask = (uint)Avx2.MoveMask(matches);
                    tzc = (uint)BitOperations.TrailingZeroCount(mask);
                    if (mask != 0)
                        return vectorSize2 + tzc;

                    const nuint vectorSize3 = 3 * vectorSize;
                    matches = Vector256.Equals(Unsafe.ReadUnaligned<Vector256<byte>>(span.Pointer + vectorSize3), sepVec);
                    mask = (uint)Avx2.MoveMask(matches);
                    tzc = (uint)BitOperations.TrailingZeroCount(mask);
                    return vectorSize3 + tzc;
                }
            }

            return IndexOf(0, (byte)';');
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