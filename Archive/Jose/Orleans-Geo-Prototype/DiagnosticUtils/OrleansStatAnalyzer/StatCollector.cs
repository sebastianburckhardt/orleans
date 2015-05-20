using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OrleansStatAnalyzer
{
    public abstract class StatCollector
    {
        public double TimeGap { get; set; }
        public HashSet<string> NewCounterList { get; set; }
        public abstract Dictionary<string, SiloInstance> RetreiveData(HashSet<string> counters);
    }
}
