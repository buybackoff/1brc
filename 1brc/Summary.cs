namespace _1brc
{
    public struct Summary
    {
        public double Min;
        public double Max;
        public double Sum;
        public long Count;
        public double Average => Sum / Count;

        public void Apply(double value, bool isFirst)
        {
            if (value < Min || isFirst)
                Min = value;
            if (value > Max || isFirst)
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