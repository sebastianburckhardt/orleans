using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.LogViews
{
    /// <summary>
    /// A collection of statistics for log view grains. See <see cref="ILogViewGrain"/>
    /// </summary>
    public class LogViewStatistics
    {
        /// <summary>
        /// A map from event names to event counts
        /// </summary>
        public Dictionary<String, long> EventCounters;
        /// <summary>
        /// A list of all measured stabilization latencies
        /// </summary>
        public List<int> StabilizationLatenciesInMsecs;
    }

}
