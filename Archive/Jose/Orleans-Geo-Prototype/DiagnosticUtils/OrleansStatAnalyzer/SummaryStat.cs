using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OrleansStatAnalyzer
{
    public class SummaryStat
    {
        public double Median { get; set; }
        public double Sdev { get; set; }
        public double Avg { get; set; }
        public double Range { get; set; }
        public double Max { get; set; }
        public double Min { get; set; }
    }

    public class CounterSummaryStat
    {
        public string CounterName { get; set; }
        public bool IsReference { get; set; }
        public SummaryStat GlobalStat { get; set; }
        public Dictionary<string, SummaryStat> SiloSummaryStat { get; set; }
        public Dictionary<int, SummaryStat> TimeSummaryStat { get; set; }

        public CounterSummaryStat() 
        {
            GlobalStat = new SummaryStat();
            SiloSummaryStat = new Dictionary<string, SummaryStat>();
            TimeSummaryStat = new Dictionary<int, SummaryStat>();
        }
    }
}
