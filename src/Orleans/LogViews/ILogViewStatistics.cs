using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.LogViews
{
    /// <summary>
    /// A collection of statistics for log view grains
    /// </summary>
    public class LogViewStatistics
    {
        /// <summary>
        /// A map from event names to a count
        /// </summary>
        public Dictionary<String, long> eventCounters;
        /// <summary>
        /// A list of all measured stabilization latencies
        /// </summary>
        public List<int> stabilizationLatenciesInMsecs;
    }

}
