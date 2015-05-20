using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OrleansStatAnalyzer
{
    public class ThresholdAnalyzer : PerformanceCounterAnalyzer
    {
        public string RuleFile { get; set; }


        public override void Analyze(PerfCounterData counterData, HashSet<string> counters)
        {
            ThresholdAnalysisConfig threshConfig = new ThresholdAnalysisConfig();
            threshConfig.ProcessConfig(RuleFile, counterData.CounterTimeData);

            AnalyzeEachStat(counterData, counters);
        }

        private void AnalyzeEachStat(PerfCounterData counterData, HashSet<string> counters)
        {
           foreach (var counterName in counters)
           {
               try
               {
                   if (counterData.CounterTimeData[counterName].Rules.Count() != 0)
                   {
                       Console.WriteLine();
                       Console.WriteLine("Analyzing {0}", counterName);
                       Console.WriteLine("===========================================");
                       counterData.CounterTimeData[counterName].EvaluateRules(counterData);
                       Console.WriteLine();
                   }
               }
               catch (KeyNotFoundException)
               {
                   System.Console.WriteLine("Cannot find the counter for analysis {0}", counterName);     
               }
                
           }
       }
    }
}
