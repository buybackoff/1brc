using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace _1brc
{
    [StructLayout(LayoutKind.Sequential, Size = 16)]
    public struct Summary
    {
        public long Sum;
        public int Count;
        public short Min;
        public short Max;
        
        public double Average => (double)Sum / Count;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Apply(int value, bool existing)
        {
            if (existing)
                Apply(value);
            else
                Init(value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Init(int value)
        {
            Sum = value;
            Count = 1;
            Min = (short)value;
            Max = (short)value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Apply(int value)
        {
            Sum += value;
            Count++;
            Min = (short)GetMin(Min, value);
            Max = (short)GetMax(Max, value);
            
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static int GetMin(int a, int b)
            {
                int delta = a - b;
                return b + (delta & (delta >> 31));
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static int GetMax(int a, int b)
            {
                int delta = a - b;
                return a - (delta & (delta >> 31));
            }
        }

        public void Merge(Summary other)
        {
            if (other.Min < Min)
                Min = other.Min;
            if (other.Max > Max)
                Max = other.Max;
            Sum += other.Sum;
            Count += other.Count;
        }

        public override string ToString() => $"{Min / 10.0:N1}/{Average / 10.0:N1}/{Max / 10.0:N1}";
    }
}