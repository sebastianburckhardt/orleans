using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OrleansStatAnalyzer
{
    public class VarianceBasedAnalyzer : PerformanceCounterAnalyzer
    {
        public override void Analyze(PerfCounterData counterData, HashSet<string> counters)
        {
            Console.WriteLine();
            Console.WriteLine("Global Variance of normalized data: Only the values which are greather than 0.3 is shown");
            Console.WriteLine("=================================================================================");
            AnalyzeVarianceGlobally(counters, counterData.SiloData);
            
            Console.WriteLine();
            Console.WriteLine("Silo Variance of normalized data: Only the values which are greather than 0.3 is shown");
            Console.WriteLine("=================================================================================");

            AnalyzeVarianceInSilos(counters, counterData.SiloData);

            Console.WriteLine();
            Console.WriteLine("Time point Variance of normalized data: Only the values which are greather than 0.3 is shown");
            Console.WriteLine("=================================================================================");

            AnalyzeVarianceInTme(counterData.CounterTimeData);
        }

        public void AnalyzeVarianceGlobally(HashSet<string> counters, Dictionary<string, SiloInstance> silos)
        {
            Console.WriteLine();
            foreach (string counter in counters)
            {
                List<Double> counterVals = new List<double>();
                foreach (var pair in silos)
                {
                    try
                    {
                        foreach (var counterVal in pair.Value.SiloStatistics[counter].TimeVals)
                        {
                            counterVals.Add(counterVal.Value);
                        }
                    }
                    catch (KeyNotFoundException)
                    {
                            
                    }
                }
                List<double> normalizedVals = NumericalAnalyzer.Normalize(counterVals);
                double normalizedSdev = NumericalAnalyzer.Sdev(normalizedVals);

                if (normalizedSdev > 0.3)
                {
                    Console.WriteLine("{0} : {1}", counter, normalizedSdev);    
                }
            }
            Console.WriteLine();
        }

        public void AnalyzeVarianceInSilos(HashSet<string> counters, Dictionary<string, SiloInstance> silos)
        {
            foreach (string counter in counters)
            {
                foreach (var pair in silos)
                {
                    List<Double> counterVals = new List<double>();
                    try
                    {
                        foreach (var counterVal in pair.Value.SiloStatistics[counter].TimeVals)
                        {
                            counterVals.Add(counterVal.Value);
                        }
                    }
                    catch (KeyNotFoundException)
                    {
                        continue;
                    }
                    List<double> normalizedVals = NumericalAnalyzer.Normalize(counterVals);
                    double normalizedSdev = NumericalAnalyzer.Sdev(normalizedVals);

                    if (normalizedSdev > 0.3)
                    {
                        Console.WriteLine("{0} : {1} : {2}", pair.Key, counter, normalizedSdev);
                    }
                }
            }
            Console.WriteLine();
        }

        public void AnalyzeVarianceInTme(Dictionary<string, OrleanStatistic> pcs)
        {
            foreach (var pc in pcs)
            {
                foreach (var pair in pc.Value.SiloVals)
                {
                    List<double> data = new List<double>();
                    foreach (var t in pair.Value)
                    {
                        data.Add(t.Item2);
                    }
                
                    List<double> normalizedVals = NumericalAnalyzer.Normalize(data);
                    double normalizedSdev = NumericalAnalyzer.Sdev(normalizedVals);

                    if (normalizedSdev > 0.3)
                    {
                        Console.WriteLine("{0} : {1} : {2}", pc.Key, pair.Key, normalizedSdev);
                    }
                }
            }
        }
    }
}
