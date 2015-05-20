using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OrleansStatAnalyzer
{
    public class UserDrivenAnalyzer
    {
        public HashSet<string> UserCounters { get; set; }
        public List<PerformanceCounterAnalyzer> Analyzers { get; set; }

        public UserDrivenAnalyzer()
        {
            UserCounters = new HashSet<string>();
            Analyzers = new List<PerformanceCounterAnalyzer>();
        }

        public void Analyze(PerfCounterData counterData)
        {
            foreach (var analyzer in Analyzers)
            {
                if (UserCounters.Count() == 0)
                {
                    analyzer.Analyze(counterData, counterData.AllCounters);   
                }
                else
                {
                    HashSet<string> newCounters = new HashSet<string>();
                    
                    foreach (string counter in counterData.AllCounters)
                    {
                        if (FindCounter(counter))
                        {
                            newCounters.Add(counter);
                        }
                    }
                    
                    analyzer.Analyze(counterData, newCounters);
                }
            }
        }

        private bool FindCounter(string name)
        {
            if (UserCounters.Contains(name))
            {
                return true;
            }

            foreach (var counter in UserCounters)
            {
                if (name.StartsWith(counter))
                {
                    if (!name.EndsWith(".Delta"))
                    {
                        return true;
                    }
                }
            }
            return false;
        }

    }
}
