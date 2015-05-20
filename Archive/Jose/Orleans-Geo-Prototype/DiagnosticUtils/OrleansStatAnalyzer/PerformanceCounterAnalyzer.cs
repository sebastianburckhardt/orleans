using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OrleansStatAnalyzer
{
    public abstract class PerformanceCounterAnalyzer
    {
        public abstract void Analyze(PerfCounterData counterData, HashSet<string> counters);
    }
}
