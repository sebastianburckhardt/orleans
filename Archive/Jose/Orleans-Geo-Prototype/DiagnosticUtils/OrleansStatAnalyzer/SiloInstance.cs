using System;
using System.Collections.Generic;
using System.Linq;

namespace OrleansStatAnalyzer
{
    public class SiloInstance
    {
        public string Name { get; set;}
        public Dictionary<string, OrleanStatistic> SiloStatistics { get; set; }
        public DateTime LatestTime { get; set; }
        public DateTime EarliestTime { get; set; }
        public DateTime RelativeStartingPoint { get; set; }
        public List<DateTime> TracePoints { get; set; }
        
        public SiloInstance(string name)
        {
            Name = name;
            SiloStatistics = new Dictionary<string, OrleanStatistic>();
            TracePoints = new List<DateTime>();
        }

        public void CalculateSiloStatistics(string counter)
        {
            try
            {
                SiloStatistics[counter].SummerizeSiloValues();
            }
            catch (KeyNotFoundException)
            {
                Console.WriteLine("Silo {0} does not track the counter {1}", Name, counter);
                Console.WriteLine();
            }
            catch (InvalidOperationException)
            {
                Console.WriteLine("No value for statistic {0} can be found for this Silo {1}", counter, Name);
                Console.WriteLine();
            }
        }

        public void SetEarliestTime()
        {
            List<DateTime> temp = new List<DateTime>();
            foreach (KeyValuePair<string, OrleanStatistic> pair in SiloStatistics)
            { 
                temp.Add(pair.Value.GetEarliestTime());
            }
            if (temp.Count > 0)
            {
                EarliestTime = temp.Min();    
            }
        }

        public void SetLatestTime()
        {
            List<DateTime> temp = new List<DateTime>();
            foreach (KeyValuePair<string, OrleanStatistic> pair in SiloStatistics)
            {
                temp.Add(pair.Value.GetLatestTime());
            }
            if (temp.Count > 0)
            {
                LatestTime = temp.Max();    
            }
        }

        public void SetRelativeStartingPoint(DateTime dt)
        {
            List<double> diff = new List<double>();

            int size = TracePoints.Count();

            for (int i = 0; i < size; i++)
            {
                TimeSpan ts = TracePoints.ElementAt(i) - dt;
                double secs = ts.TotalSeconds;

                diff.Add(Math.Abs(secs));

                if (i > 0)
                {
                    if (diff.ElementAt(i) > diff.ElementAt(i - 1))
                    {
                        RelativeStartingPoint = TracePoints.ElementAt(i - 1);
                        break;
                    }
                }
            }
        }
    }
}
