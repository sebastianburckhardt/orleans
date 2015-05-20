using System;
using System.Collections.Generic;
using System.Linq;

namespace OrleansStatAnalyzer
{
    public class OrleanStatistic
    {
        public string Name { get; private set; }
        
        // This is use when the stat is inside a Silo
        public Dictionary<DateTime, double> TimeVals { get; set;}
        
        // This is used in the point of view of time
        public Dictionary<int, List<Tuple<string, double > > > SiloVals { get; set;}

        // This is used for stats in a silo
        public Dictionary<string, double> SiloSummaryValuses; 

        
        public List<StatRule> Rules { get; set; } 
 

        public OrleanStatistic(string name)
        {
            Name = name;
            TimeVals = new Dictionary<DateTime, double>();
            SiloVals = new Dictionary<int, List<Tuple<string, double> > >();
            SiloSummaryValuses = new Dictionary<string, double>();
            Rules = new List<StatRule>();
        }

        public void SummerizeSiloValues()
        {
            double Average = NumericalAnalyzer.Average(TimeVals.Values.ToList());
            double Max = NumericalAnalyzer.Max(TimeVals.Values.ToList());
            double Min = NumericalAnalyzer.Min(TimeVals.Values.ToList());
            double Median = NumericalAnalyzer.Median(TimeVals.Values.ToList());
            double Sdev = NumericalAnalyzer.Sdev(TimeVals.Values.ToList());

            SiloSummaryValuses.Add("Avg", Average);
            SiloSummaryValuses.Add("Max", Max);
            SiloSummaryValuses.Add("Min", Min);
            SiloSummaryValuses.Add("Median", Median);
            SiloSummaryValuses.Add("Sdev", Sdev);
        }

        public DateTime GetEarliestTime()
        {
            return TimeVals.Keys.Min();
        }

        public DateTime GetLatestTime()
        {
            return TimeVals.Keys.Max();
        }

        public void EvaluateRules(PerfCounterData counterData)
        {
            foreach (StatRule r in Rules)
            {
                r.Evaluate(counterData, Name);
                Console.WriteLine();
                Console.WriteLine("Violations For Rule : {0} in Counter {1}", r.Name, Name);
                Console.WriteLine("=======================");
                Console.WriteLine();

                if (r.ViolatedInfo.Count() == 0)
                {
                    System.Console.WriteLine("No Anamolies in the counter for the Rule");
                }
                else
                {
                    foreach (var violation in r.ViolatedInfo)
                    {
                        Console.WriteLine("Silo: {0} Time: {1} Val: {2}", violation.Item1, violation.Item2,
                                          violation.Item3);        
                    }
                }
            }
        }
    }
}
