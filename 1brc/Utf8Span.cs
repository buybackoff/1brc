using System.Runtime.CompilerServices;
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
            if (_len > 3)
                return *(int*)_pointer;

            if (_len > 1)
                return *(short*)_pointer;

            if (_len > 0)
                return *_pointer;

            return 0;
        }

        public override string ToString() => new((sbyte*)_pointer, 0, _len, Encoding.UTF8);
    }
}