using System.Runtime.CompilerServices;

namespace _1brc
{
    public struct Summary : ISpanFormattable
    {
        public nint Sum;
        public nuint Count;
        public nint Min;
        public nint Max;

        public double Average => (double)Sum / Count;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Apply(nint value)
        {
            Sum += value;
            if (Count++ > 0)
            {
                // For normal real data (not an artificial sequence such as `0,-1,2,-3,4,-5,...`) the branches are not taken and we have less assignments. 
                if (value < Min)
                    Min = value;
                if (value > Max)
                    Max = value;
            }
            else
            {
                Min = value;
                Max = value;
            }
        }

        public void Merge(Summary other)
        {
            if (other.Min < Min || Count == 0)
                Min = other.Min;
            if (other.Max > Max || Count == 0)
                Max = other.Max;
            Sum += other.Sum;
            Count += other.Count;
        }

        public override string ToString() => $"{Min / 10.0:N1}/{Average / 10.0:N1}/{Max / 10.0:N1}";

        // No change in performance for 1BRC, but this is the right approach to use ISpanFormattable
        // https://github.com/buybackoff/1brc/issues/8

        public bool TryFormat(Span<char> destination, out int charsWritten, ReadOnlySpan<char> format, IFormatProvider? provider)
            => destination.TryWrite($"{Min / 10.0:N1}/{Average / 10.0:N1}/{Max / 10.0:N1}", out charsWritten);

        public string ToString(string? format, IFormatProvider? formatProvider) => ToString();
    }
}