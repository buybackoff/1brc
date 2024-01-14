using System.Diagnostics;
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
        private static ReadOnlySpan<byte> StrcmpMask256 => new byte[64]
        {
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        };

        /// <summary>
        /// 
        /// </summary>
        /// <param name="other"></param>
        /// <param name="isSimdSafe"> True if `Pointer + 31` and `other.Pointer + 31` locations are in the process memory and loading a Vector256 from the pointers is safe. </param>
        /// <returns></returns>
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

                var mask = Vector256.LoadUnsafe(in MemoryMarshal.GetReference(StrcmpMask256), (uint)Vector256<byte>.Count - Length);
                var bytes = Vector256.Load(Pointer);
                var otherBytes = Vector256.Load(other.Pointer);

                return Avx2.And(bytes, mask) == Avx2.And(otherBytes, mask);
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

            if (Length >= 3)
                return (int)((Length * 820243u) ^ (uint)(*(uint*)(Pointer)));

            return (int)(uint)(*(ushort*)Pointer * 31);
        }

        public override string ToString() => new((sbyte*)Pointer, 0, (int)Length, Encoding.UTF8);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public nint ParseInt(nuint start, out nuint lfIndex)
        {
            var ptr = Pointer + start;
            nint sign;

            if (*ptr == (byte)'-')
            {
                ptr++;
                sign = -1;
                lfIndex = start + 5;
            }
            else
            {
                sign = 1;
                lfIndex = start + 4;
            }

            if (ptr[1] == '.')
                return (nint)(ptr[0] * 10u + ptr[2] - ('0' * 11u)) * sign;

            lfIndex++;
            return (nint)(ptr[0] * 100u + ptr[1] * 10 + ptr[3] - '0' * 111u) * sign;
        }

        /// <summary>
        /// Sprec: Station name: non null UTF-8 string of min length 1 character and max length 100 bytes (i.e. this could be 100 one-byte characters, or 50 two-byte characters, etc.)
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal nuint IndexOfSemicolon()
        {
            const nuint vectorSize = 32;
            const nuint stride = 4;
            nuint start = 0;
            if (Vector256.IsHardwareAccelerated)
            {
                Debug.Assert(Length > vectorSize * stride);

                var sepVec = Vector256.Create((byte)';');

                for (int i = 0; i < 4; i++)
                {
                    var matches = Vector256.Equals(Unsafe.ReadUnaligned<Vector256<byte>>(Pointer + start), sepVec);
                    var mask = (uint)Avx2.MoveMask(matches);
                    if (mask != 0)
                    {
                        var tzc = (uint)BitOperations.TrailingZeroCount(mask);
                        return start + tzc;
                    }

                    start += vectorSize;
                }
            }

            return IndexOf(start, (byte)';');
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