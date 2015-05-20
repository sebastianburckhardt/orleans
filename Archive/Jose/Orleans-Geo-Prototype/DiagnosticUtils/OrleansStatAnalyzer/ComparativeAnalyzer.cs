using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OrleansStatAnalyzer
{
    public class AnamoulasCounter
    {
        public string Name { get; set; }
        public double Median1 { get; set; }
        public double Median2 { get; set; }
        public double Range { get; set; }
    }
    
    public class ComparisonResults
    {
        public List<AnamoulasCounter> GlobalCounters { get; set; }
        public Dictionary<string, List<AnamoulasCounter>> SiloCounters { get; set; }
        public Dictionary<int, List<AnamoulasCounter>> TimeCounters { get; set; }
        public double Threshold { get; set; }
        
        public ComparisonResults()
        {
            GlobalCounters = new List<AnamoulasCounter>();
            SiloCounters = new Dictionary<string, List<AnamoulasCounter>>();
            TimeCounters = new Dictionary<int, List<AnamoulasCounter>>();
            Threshold = 0.0;
        }

        public void Print(bool isSilo, bool isTime)
        {
            Console.WriteLine();
            Console.WriteLine("Threshold : {0}", Threshold);
            Console.WriteLine("============================");

            Console.WriteLine("Global Comparisons");
            Console.WriteLine("--------------------");

            foreach (var counter in GlobalCounters)
            {
                Console.WriteLine("{0} : M1 = {1},  M2 = {2}, R = {3}", counter.Name, counter.Median1,counter.Median2, counter.Range);
            }

            if (isSilo)
            {
                Console.WriteLine();

                Console.WriteLine("Silo Comparisons");
                Console.WriteLine("--------------------");

                foreach (var pair in SiloCounters)
                {
                    Console.WriteLine("{0} :", pair.Key);
                    Console.WriteLine("===============");
                    foreach (var counter in pair.Value)
                    {
                        Console.WriteLine("{0} : M1 = {1},  M2 = {2}, R = {3}", counter.Name, counter.Median1, counter.Median2, counter.Range);
                    }
                    Console.WriteLine();
                }

                Console.WriteLine();     
            }

            if (isTime)
            {
                Console.WriteLine("Time Comparisons");
                Console.WriteLine("--------------------");

                foreach (var pair in TimeCounters)
                {
                    Console.WriteLine("Time point {0} :", pair.Key);
                    Console.WriteLine("===========================");
                    foreach (var counter in pair.Value)
                    {
                        Console.WriteLine("{0} : M1 = {1},  M2 = {2}, R = {3}", counter.Name, counter.Median1, counter.Median2, counter.Range);
                    }
                    Console.WriteLine();
                }    
            }
        }
    }

    
    public class ComparativeAnalyzer
    {
        public HashSet<string> SuspiciousCounters { get; set; }
        public List<PerformanceCounterAnalyzer> Analyzers { get; set; }
        public bool IsSilo { get; set; }
        public bool IsTime { get; set; }
 
        public ComparativeAnalyzer()
        {
            SuspiciousCounters = new HashSet<string>();
            Analyzers = new List<PerformanceCounterAnalyzer>();
            IsSilo = false;
            IsTime = false;
        }

        public void Compare (List<PerfCounterData> cData)
        {
           
            DoMedianTest(cData);
            
            foreach(var counterData in cData)
            {
                foreach (var analyzer in Analyzers)
                {
                    analyzer.Analyze(counterData, SuspiciousCounters);
                }  
            }
        }

        void DoMedianTest(List<PerfCounterData> cData)
        {
            PerfCounterData refData = GetReferenceDataset(cData);

            if (refData == null)
            {
                Console.WriteLine("The reference dataset is not specified");
                return;
            }

            string line;
            do
            {
                Console.WriteLine();
                Console.WriteLine("Enter the thhreshold for median test : ");
                line = Console.ReadLine();

                if (line == "quit")
                {
                    break;
                }

                ComparisonResults compResults = new ComparisonResults();

                foreach (var perfCounterData in cData)
                {
                    if (!perfCounterData.IsRef)
                    {
                        Console.WriteLine();
                        Console.WriteLine("Anamoulas Counters in {0} dataset", perfCounterData.Name);
                        Console.WriteLine("=========================================================");
                        compResults.Threshold = Convert.ToDouble(line);

                        foreach (var counterSummary in perfCounterData.SummaryStatistics)
                        {
                            try
                            {
                                bool IsGloballyAnomalous = CompareGlobalMedianWithRef(refData.SummaryStatistics[counterSummary.Key],
                                                                        counterSummary.Value, Convert.ToDouble(line));
                                if (IsGloballyAnomalous)
                                {
                                    //double m1 = refData.SummaryStatistics[counterSummary.Key].GlobalStat.Median;
                                    //double m2 = counterSummary.Value.GlobalStat.Median;
                                    //double r = refData.SummaryStatistics[counterSummary.Key].GlobalStat.Range;
                                    //Console.WriteLine("{0} : M1 = {1},  M2 = {2}, R = {3}", counterSummary.Key, m1, m2, r);
                                    AnamoulasCounter ac = new AnamoulasCounter();
                                    ac.Name = counterSummary.Key;
                                    ac.Median1 = refData.SummaryStatistics[counterSummary.Key].GlobalStat.Median;
                                    ac.Median2 = counterSummary.Value.GlobalStat.Median;
                                    ac.Range = refData.SummaryStatistics[counterSummary.Key].GlobalStat.Range;

                                    compResults.GlobalCounters.Add(ac);
                                    SuspiciousCounters.Add(counterSummary.Key);
                                    SuspiciousCounters.Add(counterSummary.Key);
                                }

                                if (IsSilo)
                                {
                                    foreach (var spair in counterSummary.Value.SiloSummaryStat)
                                    {
                                        bool IsSiloAnomalous =
                                            CompareSiloMedianWithRef(refData.SummaryStatistics[counterSummary.Key],
                                                                     counterSummary.Value, Convert.ToDouble(line), spair.Key);

                                        if (IsSiloAnomalous)
                                        {
                                            AnamoulasCounter ac = new AnamoulasCounter();
                                            ac.Name = counterSummary.Key;
                                            ac.Median1 = refData.SummaryStatistics[counterSummary.Key].SiloSummaryStat[spair.Key].Median;
                                            ac.Median2 = counterSummary.Value.SiloSummaryStat[spair.Key].Median;
                                            ac.Range = refData.SummaryStatistics[counterSummary.Key].SiloSummaryStat[spair.Key].Range;

                                            if (!compResults.SiloCounters.ContainsKey(spair.Key))
                                            {
                                                List<AnamoulasCounter> val = new List<AnamoulasCounter>();
                                                compResults.SiloCounters.Add(spair.Key, val);
                                            }

                                            compResults.SiloCounters[spair.Key].Add(ac);
                                            SuspiciousCounters.Add(counterSummary.Key);
                                            SuspiciousCounters.Add(counterSummary.Key);
                                        }
                                    }    
                                }

                                if (IsTime)
                                {
                                    foreach (var tpair in counterSummary.Value.TimeSummaryStat)
                                    {
                                        bool IsTimeAnomalous =
                                            CompareTimeMedianWithRef(refData.SummaryStatistics[counterSummary.Key],
                                                                     counterSummary.Value, Convert.ToDouble(line), tpair.Key);

                                        if (IsTimeAnomalous)
                                        {
                                            AnamoulasCounter ac = new AnamoulasCounter();
                                            ac.Name = counterSummary.Key;
                                            ac.Median1 = refData.SummaryStatistics[counterSummary.Key].TimeSummaryStat[tpair.Key].Median;
                                            ac.Median2 = counterSummary.Value.TimeSummaryStat[tpair.Key].Median;
                                            ac.Range = refData.SummaryStatistics[counterSummary.Key].TimeSummaryStat[tpair.Key].Range;

                                            if (!compResults.TimeCounters.ContainsKey(tpair.Key))
                                            {
                                                List<AnamoulasCounter> val = new List<AnamoulasCounter>();
                                                compResults.TimeCounters.Add(tpair.Key, val);
                                            }

                                            compResults.TimeCounters[tpair.Key].Add(ac);
                                            SuspiciousCounters.Add(counterSummary.Key);
                                        }
                                    }        
                                }
                            
                            }
                            
                            catch (KeyNotFoundException)
                            {

                            }
                        }
                        compResults.Print(IsSilo, IsTime);
                    }
                }
            
            } while (line != null);
        }

        
        PerfCounterData GetReferenceDataset(List<PerfCounterData> cData)
        {
            foreach (var data in cData)
            {
                if (data.IsRef)
                {
                    return data;   
                }
            }
            return null;
        }

        bool CompareGlobalMedianWithRef(CounterSummaryStat cStatRef, CounterSummaryStat cStat, double threshold)
        {
            double globalDiff = Math.Abs(cStatRef.GlobalStat.Median - cStat.GlobalStat.Median);

            double diffPercentage = 0.0;
            
            if (cStatRef.GlobalStat.Range != 0.0)
            {
                diffPercentage = globalDiff / cStatRef.GlobalStat.Range;
            }
            
            if (diffPercentage > threshold)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        bool CompareSiloMedianWithRef(CounterSummaryStat cStatRef, CounterSummaryStat cStat, double threshold, string silo)
        {
            double siloDiff = Math.Abs(cStatRef.SiloSummaryStat[silo].Median - cStat.SiloSummaryStat[silo].Median);

            double diffPercentage = 0.0;
            if (cStatRef.SiloSummaryStat[silo].Range != 0.0)
            {
                diffPercentage = siloDiff / cStatRef.SiloSummaryStat[silo].Range;    
            }

            if (diffPercentage > threshold)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        bool CompareTimeMedianWithRef(CounterSummaryStat cStatRef, CounterSummaryStat cStat, double threshold, int timePoint)
        {
            double siloDiff = Math.Abs(cStatRef.TimeSummaryStat[timePoint].Median - cStat.TimeSummaryStat[timePoint].Median);

            double diffPercentage = 0.0;
            
            if (cStatRef.TimeSummaryStat[timePoint].Range != 0.0)
            {
                diffPercentage = siloDiff / cStatRef.TimeSummaryStat[timePoint].Range;    
            }

            if (diffPercentage > threshold)
            {
                return true;
            }
            else
            {
                return false;
            }
        }
    }
}
