using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OrleansStatAnalyzer
{
    public abstract class StatRule
    {
        public string Name { get; set; }
        public string Statistic { get; set; }
        public double ExpectedVal { get; set; }
        public RuleOperator op { get; set; }
        public List<Tuple<string, DateTime, double>> ViolatedInfo { get; set; }
        public bool IsPercentageRule { get; set; }

        public double GetStatValue(List<double> data)
        {
            if(Statistic == "Average")
            {
                return NumericalAnalyzer.Average(data);
            }
            
            if(Statistic == "Median")
            {
                return NumericalAnalyzer.Median(data);
            }
            
            if(Statistic == "Max")
            {
                return NumericalAnalyzer.Max(data);
            }
            
            if(Statistic == "Min")
            {
                return NumericalAnalyzer.Min(data);
            }
            
            if(Statistic == "Sdev")
            {
                return NumericalAnalyzer.Sdev(data);
            }

            if (Statistic == "Variance")
            {
                return NumericalAnalyzer.Variance(data);
            }

            Console.WriteLine("Don't understand the specified stat name");
            return 0.0;
        }


        public void EvaluateAny(PerfCounterData counterData, string counterName)
        {
            foreach (var pair in counterData.SiloData)
            {
                try
                {
                    foreach (var counterVal in pair.Value.SiloStatistics[counterName].TimeVals)
                    {
                        if (!op.operate(counterVal.Value, ExpectedVal))
                        {
                            ViolatedInfo.Add(new Tuple<string, DateTime, double>(pair.Key, counterVal.Key,
                                                                                counterVal.Value));
                        }
                    }
                }
                catch (KeyNotFoundException)
                {

                }
            }
        }
        
        
        public abstract void Evaluate(PerfCounterData counterData, string counterName);
    }
}
