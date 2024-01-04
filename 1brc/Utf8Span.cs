using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.InteropServices.JavaScript;
using System.Text;

namespace _1brc
{
    public readonly unsafe struct Utf8Span : IEquatable<Utf8Span>
    {
        private readonly byte* _pointer;
        private readonly int _len;
        
        public Utf8Span(byte* pointer, int len)
        {
            _pointer = pointer;
            _len = len;
        }

        public ReadOnlySpan<byte> Span
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => new(_pointer, _len);
        }

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
            
            if (_len > 3)
                return (_len * 820243) ^ (*(int*)_pointer); 
            
            if (_len > 1)
                return *(short*)_pointer;
            
            if (_len > 0)
                return *_pointer;
        
            return 0;
        }

        public override string ToString() => new((sbyte*)_pointer, 0, _len, Encoding.UTF8);
    }
}