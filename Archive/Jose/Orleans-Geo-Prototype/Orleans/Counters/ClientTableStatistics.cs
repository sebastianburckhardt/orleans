#define LOG_MEMORY_PERF_COUNTERS 
using System;


namespace Orleans.Counters
{
    [Serializable]
    internal class ClientTableStatistics : MarshalByRefObject, IClientPerformanceMetrics
    {
        private IMessageCenter MC;
        private IClientMetricsDataPublisher MetricsDataPublisher;
        private TimeSpan reportFrequency;

        private readonly IntValueStatistic connectedGWCount;
        private RuntimeStatisticsGroup runtimeStats;

        private AsyncSafeTimer reportTimer;
        private static readonly Logger logger = Logger.GetLogger("ClientTableStatistics", Logger.LoggerType.Runtime);

        internal ClientTableStatistics(IMessageCenter mc, IClientMetricsDataPublisher metricsDataPublisher, RuntimeStatisticsGroup runtime)
        {
            this.MC = mc;
            this.MetricsDataPublisher = metricsDataPublisher;
            runtimeStats = runtime;
            reportFrequency = TimeSpan.Zero;
            connectedGWCount = IntValueStatistic.Find(StatNames.STAT_CLIENT_CONNECTED_GW_COUNT);
        }

        #region IClientPerformanceMetrics Members

        public float CpuUsage 
        {
            get { return runtimeStats.CpuUsage; } 
        }

        public long MemoryUsage 
        {
            get { return runtimeStats.MemoryUsage; } 
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

        public long ConnectedGWCount
        {
            get { return connectedGWCount.GetCurrentValue(); }
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
                        logger.Info(ErrorCode.PerfMetricsStoppingTimer, "Stopping performance metrics reporting with reportFrequency={0}", reportFrequency);
                        if (reportTimer != null)
                        {
                            reportTimer.Dispose();
                            reportTimer = null;
                        }
                    }
                    reportFrequency = TimeSpan.Zero;
                }
                else
                {
                    reportFrequency = value;
                    logger.Info(ErrorCode.PerfMetricsStartingTimer, "Starting performance metrics reporting with reportFrequency={0}", reportFrequency);
                    if (reportTimer != null)
                    {
                        reportTimer.Dispose();
                    }
                    reportTimer = new AsyncSafeTimer(this.Reporter, null, reportFrequency, reportFrequency); // Start a new fresh timer. 
                }
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private AsyncCompletion Reporter(object context)
        {
            try
            {
                if(MetricsDataPublisher != null)
                {
                    return AsyncCompletion.FromTask(MetricsDataPublisher.ReportMetrics(this));
                }
            }
            catch (Exception exc)
            {
                var e = exc.GetBaseException();
                logger.Error(ErrorCode.Runtime_Error_100101, String.Format("Exception occurred during metrics reporter."), exc);
            }
            return AsyncCompletion.Done;
        }

        #endregion

        public void Dispose()
        {
            if (this.reportTimer != null)
                reportTimer.Dispose();
            reportTimer = null;
        }
    }
}
