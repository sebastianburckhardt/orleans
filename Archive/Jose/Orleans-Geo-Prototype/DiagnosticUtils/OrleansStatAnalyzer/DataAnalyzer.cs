using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OrleansStatAnalyzer
{
    public class DataAnalyzer
    {
        public HashSet<string> SuspiciousCounters { get; set; }
        public List<PerformanceCounterAnalyzer> Analyzers { get; set; }

        public DataAnalyzer()
        {
            SuspiciousCounters = new HashSet<string>();
            Analyzers = new List<PerformanceCounterAnalyzer>();
        }

        public void DetectAndAnalyze(PerfCounterData counterData)
        {
            FindSuspiciousCounters(counterData);
            AnalyzeSuspiciousCounters(counterData);
        }

        public void AnalyzeSuspiciousCounters(PerfCounterData counterData)
        {
            foreach (var analyzer in Analyzers)
            {
                analyzer.Analyze(counterData, SuspiciousCounters);    
            }    
        }
        
        public void FindSuspiciousCounters(PerfCounterData counterData)
        {
            string line;

            do
            {
                Console.WriteLine();
                Console.WriteLine("Enter the thhreshold for Anamoly test or type quit to exit : ");
                line = Console.ReadLine();

                if (line == "quit")
                {
                    break;
                }

                double threshold = Convert.ToDouble(line);

                foreach (var counter in counterData.AllCounters)
                {
                    try
                    {
                        double globalMedian = counterData.SummaryStatistics[counter].GlobalStat.Median;
                        double globalSdev = counterData.SummaryStatistics[counter].GlobalStat.Sdev;

                        foreach (var spair in counterData.SummaryStatistics[counter].SiloSummaryStat)
                        {
                            double siloMedian = counterData.SummaryStatistics[counter].SiloSummaryStat[spair.Key].Median;
                            double x = (Math.Abs(siloMedian - globalMedian)) / globalSdev;

                            if (x > threshold)
                            {
                                Console.WriteLine("{0} : {1}  Global_Median={2}    Silo_Median={3},    Global_Sdev={4}",
                                    spair.Key, counter, globalMedian, siloMedian, globalSdev);
                                SuspiciousCounters.Add(counter);
                            }
                        }
                    }
                    catch (KeyNotFoundException)
                    {

                    }
                }

            } while (line != null);
        }
    }
}
