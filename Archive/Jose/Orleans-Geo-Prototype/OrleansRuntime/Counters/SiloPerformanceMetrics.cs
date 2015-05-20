#define LOG_MEMORY_PERF_COUNTERS 
using System;
using System.Diagnostics;
using System.Threading;

using Orleans.Scheduler;
using Orleans.Counters;

namespace Orleans.Runtime.Counters
{
    [Serializable]
    internal class SiloPerformanceMetrics : MarshalByRefObject, ISiloPerformanceMetrics, IDisposable
    {
        internal OrleansTaskScheduler Scheduler { get; set; }
        internal TargetDirectory ActivationDirectory { get; set; }
        internal IMessageCenter MC { get; set; }
        internal ISiloMetricsDataPublisher MetricsDataPublisher { get; set; }
        private TimeSpan reportFrequency;
        private bool overloadLatched;
        private bool overloadValue;

        private RuntimeStatisticsGroup runtimeStats;
        private AsyncSafeTimer tableReportTimer;
        private static readonly Logger logger = Logger.GetLogger("SiloPerformanceMetrics", Logger.LoggerType.Runtime);
        internal NodeConfiguration NodeConfig { get; set; }
        private float? cpuUsageLatch = null;

        internal SiloPerformanceMetrics(RuntimeStatisticsGroup runtime, NodeConfiguration cfg = null)
        {
            runtimeStats = runtime;
            reportFrequency = TimeSpan.Zero;
            overloadLatched = false;
            overloadValue = false;
            NodeConfig = cfg ?? new NodeConfiguration();
            StringValueStatistic.FindOrCreate(StatNames.STAT_RUNTIME_IS_OVERLOADED, () => IsOverloaded.ToString());
        }

        // For testing only
        public void LatchIsOverload(bool overloaded)
        {
            overloadLatched = true;
            overloadValue = overloaded;
        }

        // For testing only
        public void UnlatchIsOverloaded()
        {
            overloadLatched = false;
        }

        public void LatchCpuUsage(float value)
        {
            cpuUsageLatch = value;
        }

        public void UnlatchCpuUsage()
        {
            cpuUsageLatch = null;
        }

        #region ISiloPerformanceMetrics Members

        public float CpuUsage 
        { 
            get { return cpuUsageLatch.HasValue ? cpuUsageLatch.Value : runtimeStats.CpuUsage; } 
        }

        public long MemoryUsage 
        {
            get { return runtimeStats.MemoryUsage; } 
        }

        public bool IsOverloaded
        {
            get { return overloadLatched ? overloadValue : (NodeConfig.LoadSheddingEnabled && (CpuUsage > NodeConfig.LoadSheddingLimit)); }
        }

        public long RequestQueueLength
        {
            get
            {
                return MC.ReceiveQueueLength;
                //return MessagingProcessingStatisticsGroup.RequestQueueLength;
            }
        }

        public int ActivationCount
        {
            get
            {
                return ActivationDirectory.Count;
            }
        }

        public int SendQueueLength
        {
            get { return MC.SendQueueLength; }
        }

        public int ReceiveQueueLength
        {
            get { return MC.ReceiveQueueLength; }
        }

        public long SentMessages
        {
            get { return MessagingStatisticsGroup.MessagesSentTotal.GetCurrentValue(); }
        }

        public long ReceivedMessages
        {
            get { return MessagingStatisticsGroup.MessagesReceived.GetCurrentValue(); }
        }

        public long ClientCount
        {
            get { return MessagingStatisticsGroup.ConnectedClientCount.GetCurrentValue(); }
        }

        public TimeSpan MetricsTableWriteInterval
        {
            get { return reportFrequency; }
            set
            {
                if (value <= TimeSpan.Zero)
                {
                    if (reportFrequency > TimeSpan.Zero)
                    {
                        logger.Info(ErrorCode.PerfMetricsStoppingTimer, "Stopping Silo Table metrics reporter with reportFrequency={0}", reportFrequency);
                        if (tableReportTimer != null)
                        {
                            tableReportTimer.Dispose();
                            tableReportTimer = null;
                        }
                    }
                    reportFrequency = TimeSpan.Zero;
                }
                else
                {
                    reportFrequency = value;
                    logger.Info(ErrorCode.PerfMetricsStartingTimer, "Starting Silo Table metrics reporter with reportFrequency={0}", reportFrequency);
                    if (tableReportTimer != null)
                    {
                        tableReportTimer.Dispose();
                    }
                    tableReportTimer = new AsyncSafeTimer(this.Reporter, null, reportFrequency, reportFrequency); // Start a new fresh timer. 
                }
            }
        }

        private AsyncCompletion Reporter(object context)
        {
            try
            {
                if (MetricsDataPublisher != null)
                {
                    return AsyncCompletion.FromTask(MetricsDataPublisher.ReportMetrics(this));
                }
            }
            catch (Exception exc)
            {
                var e = exc.GetBaseException();
                logger.Error(ErrorCode.Runtime_Error_100101, "Exception occurred during Silo Table metrics reporter: " + e.Message, exc);
            }
            return AsyncCompletion.Done;
        }

        #endregion

        public void Dispose()
        {
            if (this.tableReportTimer != null)
                tableReportTimer.Dispose();
            tableReportTimer = null;
        }
    }
}
