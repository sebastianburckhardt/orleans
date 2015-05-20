using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OrleansStatAnalyzer
{
    public class PerfCounterData
    {
        public string Name;
        public Dictionary<string, SiloInstance> SiloData { get; set; }
        public Dictionary<string, OrleanStatistic> CounterTimeData { get; set; }
        public Dictionary<string, CounterSummaryStat> SummaryStatistics { get; set; }

        public StatCollector DataSource { get; set; }
        public HashSet<string> AllCounters { get; set; }
        
        public bool IsRef { get; set; }

        public DataAnalyzer DAnalyzer { get; set; }
        public UserDrivenAnalyzer UAnalyzer { get; set; }

        public PerfCounterData()
        {
            AllCounters = new HashSet<string>();
            IsRef = false;
            DAnalyzer = null;
            UAnalyzer = null;
        }

        public void Populate()
        {
            // Collect data

            Console.WriteLine();
            Console.WriteLine("Processing logs of {0} .......", Name);
            Console.WriteLine();

            SiloData = DataSource.RetreiveData(AllCounters);

            if (DataSource.NewCounterList.Count() > 0)
            {
                AllCounters = DataSource.NewCounterList;
            }

            if (SiloData.Count == 0)
            {
                System.Console.WriteLine("No data extracted. Please see the file prefix specified in the configuration");
                return;
            }

            // Get the data in time view.

            PopulateCounterView();
            SummaryStatistics = new Dictionary<string, CounterSummaryStat>();
        }

        public void Analyze()
        {
            // This will calculate and store all the summary statistics.
            CalculateSummaryStatistics();
            
            if (UAnalyzer != null)
            {
                UAnalyzer.Analyze(this);
            }

            if (DAnalyzer != null)
            {
                DAnalyzer.DetectAndAnalyze(this);
            }
        }

        private void PopulateCounterView()
        {
            CounterTimeData = new Dictionary<string, OrleanStatistic>();
            
            int iterations = SetStartingPoints();

            foreach (var counterName in AllCounters)
            {
                OrleanStatistic perfCounter = new OrleanStatistic(counterName);
                Console.WriteLine("processing {0}", counterName);
                
                foreach (KeyValuePair<string, SiloInstance> spair in SiloData)
                {
                    try
                    {
                        List<double> vals = spair.Value.SiloStatistics[counterName].TimeVals.Values.ToList();
                        int i = 0;

                        foreach (var dt in spair.Value.TracePoints)
                        {
                            if (dt < spair.Value.RelativeStartingPoint)
                            {
                                continue;
                            }

                            if (i == iterations)
                            {
                                break;
                            }

                            if (!perfCounter.SiloVals.ContainsKey(i))
                            {
                                List<Tuple<String, double>> siloVals = new List<Tuple<string, double>>();
                                perfCounter.SiloVals.Add(i, siloVals);
                            }
                            
                            double val = 0.0;
                            try
                            {
                                val = spair.Value.SiloStatistics[counterName].TimeVals[dt];
                            }
                            catch (KeyNotFoundException)
                            {
                                val = 0.0;
                            }
                            Tuple<string, double> tuple = new Tuple<string, double>(spair.Key, val);
                            perfCounter.SiloVals[i].Add(tuple);
                            i++;
                         }
                    }
                    catch (KeyNotFoundException)
                    {
                    }
                }

                if (perfCounter.SiloVals.Count() > 0)
                {
                    CounterTimeData.Add(counterName, perfCounter);    
                }
            }
        }

        private DateTime GetEarliestTime()
        {
            List<DateTime> temp = new List<DateTime>();
            foreach (KeyValuePair<string, SiloInstance> pair in SiloData)
            {
                temp.Add(pair.Value.EarliestTime);
            }
            return temp.Min();
        }

        private DateTime GetLatestTime()
        {
            List<DateTime> temp = new List<DateTime>();
            foreach (KeyValuePair<string, SiloInstance> pair in SiloData)
            {
                temp.Add(pair.Value.LatestTime);
            }
            return temp.Max();
        }

        private DateTime GetLatestOfEarliestTime()
        {
            List<DateTime> temp = new List<DateTime>();
            foreach (KeyValuePair<string, SiloInstance> pair in SiloData)
            {
                temp.Add(pair.Value.EarliestTime);
            }
            return temp.Max();
        }

        private DateTime GetEarliestOfLatestTime()
        {
            List<DateTime> temp = new List<DateTime>();
            foreach (KeyValuePair<string, SiloInstance> pair in SiloData)
            {
                temp.Add(pair.Value.LatestTime);
            }
            return temp.Min();
        }

        int SetStartingPoints()
        {
            DateTime t1 = GetLatestOfEarliestTime();
            DateTime t2 = GetEarliestOfLatestTime();

            foreach (var siloEntry in SiloData)
            {
                siloEntry.Value.SetRelativeStartingPoint(t1);
            }

            TimeSpan diff = t2 - t1;
            double secs = diff.TotalSeconds;

            double times = secs / DataSource.TimeGap;
            times = times + 1;

            return (int)Math.Floor(times);
        }

        public void CalculateSummaryStatistics()
        {
            foreach (var counter in AllCounters)
            {
                List<Double> global = GetGlobalData(counter);
                
                CounterSummaryStat counterSummaryStat = new CounterSummaryStat();

                counterSummaryStat.CounterName = counter;
                counterSummaryStat.IsReference = IsRef;

                counterSummaryStat.GlobalStat.Median = NumericalAnalyzer.Median(global);;
                counterSummaryStat.GlobalStat.Avg = NumericalAnalyzer.Average(global);
                counterSummaryStat.GlobalStat.Sdev = NumericalAnalyzer.Sdev(global);
                counterSummaryStat.GlobalStat.Range = NumericalAnalyzer.Range(global);
                counterSummaryStat.GlobalStat.Max = NumericalAnalyzer.Max(global);
                counterSummaryStat.GlobalStat.Min = NumericalAnalyzer.Min(global);

                foreach (var s in SiloData)
                {
                    List<double> siloValues = GetSiloData(s.Value, counter);
                    if (siloValues.Count() > 0)
                    {
                        SummaryStat sumStat = new SummaryStat();
                        sumStat.Median = NumericalAnalyzer.Median(siloValues); ;
                        sumStat.Avg = NumericalAnalyzer.Average(siloValues);
                        sumStat.Sdev = NumericalAnalyzer.Sdev(siloValues);
                        sumStat.Range = NumericalAnalyzer.Range(siloValues);
                        sumStat.Max = NumericalAnalyzer.Max(siloValues);
                        sumStat.Min = NumericalAnalyzer.Min(siloValues);

                        counterSummaryStat.SiloSummaryStat.Add(s.Key, sumStat);

                    }
                    else
                    {
                        //System.Console.WriteLine("No values for the counter {0} in Silo {1}", counter, s.Key);   
                    }
                }

                foreach (var pair in CounterTimeData[counter].SiloVals)
                {
                    List<double> timeVals = new List<double>();
                    foreach (var t in pair.Value)
                    {
                        timeVals.Add(t.Item2);
                    }
                    
                    SummaryStat summStat = new SummaryStat();
                    summStat.Median = NumericalAnalyzer.Median(timeVals);
                    summStat.Avg = NumericalAnalyzer.Average(timeVals);
                    summStat.Sdev = NumericalAnalyzer.Sdev(timeVals);
                    summStat.Range = NumericalAnalyzer.Range(timeVals);
                    summStat.Max = NumericalAnalyzer.Max(timeVals);
                    summStat.Min = NumericalAnalyzer.Min(timeVals);

                    counterSummaryStat.TimeSummaryStat.Add(pair.Key, summStat);
                }

                SummaryStatistics.Add(counter, counterSummaryStat);
            }
        }


        public List<double> GetGlobalData(string counterName)
        {
            List<Double> counterVals = new List<double>();
            foreach (var pair in SiloData)
            {
                try
                {
                    foreach (var counterVal in pair.Value.SiloStatistics[counterName].TimeVals)
                    {
                        counterVals.Add(counterVal.Value);
                    }
                }
                catch (KeyNotFoundException)
                {

                }
            }
            return counterVals;
        }

        public List<double> GetSiloData(SiloInstance silo, string counterName)
        {
            List<Double> counterVals = new List<double>();
            try
            {
                foreach (var counterVal in silo.SiloStatistics[counterName].TimeVals)
                {
                    counterVals.Add(counterVal.Value);
                }
            }
            catch (KeyNotFoundException)
            {

            }
            return counterVals;
        }
    }
}
