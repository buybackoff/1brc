using System.Runtime.CompilerServices;

namespace _1brc
{
    public struct Summary
    {
        public double Min;
        public double Max;
        public double Sum;
        public long Count;
        public double Average => Sum / Count;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Init(double value)
        {
            Min = value;
            Max = value;
            Sum += value;
            Count++;
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Apply(double value)
        {
            if (value < Min)
                Min = value;
            else if (value > Max)
                Max = value;
            Sum += value;
            Count++;
        }

        public void Apply(Summary other)
        {
            if (other.Min < Min)
                Min = other.Min;
            if (other.Max > Max)
                Max = other.Max;
            Sum += other.Sum;
            Count += other.Count;
        }
        
        public override string ToString() => $"{Min:N1}/{Average:N1}/{Max:N1}";
    }
}