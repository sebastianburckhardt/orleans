namespace OrleansStatAnalyzer
{
    internal class BasicStatisticResult
    {
        public string StatName { get; set; }

        public double Average { get; set; }

        public double Min { get; set; }

        public double Max { get; set; }

        public double Percentage { get; set; }

        public BasicStatisticResult(string name)
        {
            StatName = name;
            Average = 0.0;
            Min = 0.0;
            Max = 0.0;
            Percentage = 0.0;
        }
    }
}
