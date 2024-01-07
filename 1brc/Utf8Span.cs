using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using System.Text;

namespace _1brc
{
    public unsafe struct Utf8Span : IEquatable<Utf8Span>
    {
        internal byte* Pointer;
        internal int Length;

        public Utf8Span(byte* pointer, int length)
        {
            Debug.Assert(length >= 0);
            Pointer = pointer;
            Length = length;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal byte GetAtUnsafe(int idx) => Pointer[idx];

        public ReadOnlySpan<byte> Span
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => new(Pointer, Length);
        }

        /// <summary>
        /// Slice without bound checks. Use only when the bounds are checked/ensured before the call.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Utf8Span SliceUnsafe(int start, int length) => new(Pointer + start, length);

        /// <summary>
        /// Slice without bound checks. Use only when the bounds are checked/ensured before the call.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Utf8Span SliceUnsafe(int start) => new(Pointer + start, Length - start);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(Utf8Span other) => Span.SequenceEqual(other.Span);

        public override bool Equals(object? obj)
        {
            return obj is Utf8Span other && Equals(other);
        }

        public override int GetHashCode()
        {
            // Here we use the first 4 chars (if ASCII) and the length for a hash.
            // The worst case would be a prefix such as Port/Saint and the same length,
            // which for human geo names is quite rare. 

            // .NET dictionary will obviously slow down with collisions but will still work.
            // If we keep only `*_pointer` the run time is still reasonable ~9 secs.
            // Just using `if (_len > 0) return (_len * 820243) ^ (*_pointer);` gives 5.8 secs.
            // By just returning 0 - the worst possible hash function and linear search - the run time is 12x slower at 56 seconds. 

            // The magic number 820243 is the largest happy prime that contains 2024 from https://prime-numbers.info/list/happy-primes-page-9

            // Avoid zero-extension when casting to int, go via uint first.

            if (Length > 3)
                return (Length * 820243) ^ (int)(*(uint*)Pointer);

            if (Length > 1)
                return (int)(uint)(*(ushort*)Pointer);

            return (int)(uint)*Pointer;
        }

        public override string ToString() => new((sbyte*)Pointer, 0, Length, Encoding.UTF8);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int ParseInt(int start, int length)
        {
            int sign = 1;
            uint value = 0;
            var end = start + length;
            
            for (; start < end; start++)
            {
                var c = (uint)GetAtUnsafe(start);

                if (c == '-')
                    sign = -1;
                else
                    value = value * 10u + (c - '0');
            }

            var fractional = (uint)GetAtUnsafe(start + 1) - '0';
            return sign * (int)(value * 10 + fractional);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int IndexOf(int start, byte value)
        {
            if (Avx2.IsSupported)
            {
                Vector<byte> vec;

                while (true)
                {
                    if (start + Vector<byte>.Count >= Length)
                        goto FALLBACK;
                    var data = Unsafe.ReadUnaligned<Vector<byte>>(Pointer + start);
                    vec = Vector.Equals(data, new Vector<byte>(value));
                    if (!vec.Equals(Vector<byte>.Zero))
                        break;
                    start += Vector<byte>.Count;
                }

                var matches = vec.AsVector256();
                var mask = Avx2.MoveMask(matches);
                int tzc = BitOperations.TrailingZeroCount((uint)mask);
                return start + tzc;
            }

            FALLBACK:

            int indexOf = SliceUnsafe(start).Span.IndexOf(value);
            return indexOf < 0 ? Length : start + indexOf;
        }
    }
}