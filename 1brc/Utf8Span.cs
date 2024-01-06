using System.Buffers.Text;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;

namespace _1brc
{
    [SkipLocalsInit]
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

        // public Utf8SpanLineEnumerator EnumerateLines() => new Utf8SpanLineEnumerator(this);

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

            if (Length > 3)
                return (Length * 820243) ^ (*(int*)Pointer);

            if (Length > 1)
                return *(short*)Pointer;

            return *Pointer;
        }

        public override string ToString() => new((sbyte*)Pointer, 0, Length, Encoding.UTF8);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int ConsumeNumberWithNewLine(int start, out int consumed)
        {
            int sign = 1;
            int value = 0;

            int i = start;
            for (; i < Length; i++)
            {
                var c = (int)GetAtUnsafe(i);

                if (c > '9')
                    break;

                if (c >= '0')
                {
                    value = value * 10 + (c - '0');
                }
                else
                {
                    if (c == '-')
                    {
                        sign = -1;
                    }
                    else if (c == '.' || c < 14)
                    {
                        // Skip
                    }
                    else
                    {
                        break;
                    }
                }

            }

            consumed = i;
            return sign * value;
        }
    }
}