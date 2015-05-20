using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Orleans.AzureUtils;

using Orleans.Serialization;

namespace Orleans.Counters
{
    internal class LogStatistics
    {
        internal const string StatsLogPrefix = "Statistics: ^^^";
        internal const string StatsLogPostfix = "^^^";

        private readonly TimeSpan reportFrequency;
        private AsyncSafeTimer reportTimer;

        private readonly Logger logger;
        public StatsTableDataManager StatsTablePublisher;

        internal LogStatistics(TimeSpan writeInterval, bool isSilo)
        {
            reportFrequency = writeInterval;
            logger = Logger.GetLogger(isSilo ? "SiloLogStatistics" : "ClientLogStatistics", Logger.LoggerType.Runtime);
        }

        internal void Start()
        {
            reportTimer = new AsyncSafeTimer(this.Reporter, null, reportFrequency, reportFrequency); // Start a new fresh timer. 
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private AsyncCompletion Reporter(object context)
        {
            try
            {
                return DumpCounters();
            }
            catch (Exception exc)
            {
                var e = exc.GetBaseException();
                logger.Error(ErrorCode.Runtime_Error_100101, "Exception occurred during LogStatistics reporter.", exc);
                return AsyncCompletion.Done;
            }
        }

        public void Stop()
        {
            if (reportTimer != null)
            {
                reportTimer.Dispose();
            }
            reportTimer = null;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        internal AsyncCompletion DumpCounters()
        {
            List<IOrleansCounter> allCounters = new List<IOrleansCounter>();
            CounterStatistic.AddCounters(allCounters, cs => cs.Storage != CounterStorage.DontStore);
            IntValueStatistic.AddCounters(allCounters, cs => cs.Storage != CounterStorage.DontStore);
            StringValueStatistic.AddCounters(allCounters, cs => cs.Storage != CounterStorage.DontStore);
            FloatValueStatistic.AddCounters(allCounters, cs => cs.Storage != CounterStorage.DontStore);
            AverageTimeSpanStatistic.AddCounters(allCounters, cs => cs.Storage != CounterStorage.DontStore);

            foreach (var stat in allCounters.Where(cs => cs.Storage != CounterStorage.DontStore).OrderBy(cs => cs.Name))
            {
                WriteStatsLogEntry(stat.GetDisplayString());
            }
            // NOTE: For now, we don't want to bother logging these counters -- AG 11/20/2012
            foreach (var stat in GenerateAdditionalCounters().OrderBy(cs => cs.Name))
            {
                WriteStatsLogEntry(stat.GetDisplayString());
            }
            WriteStatsLogEntry(null); // Write any remaining log data

            try
            {
                if (StatsTablePublisher != null)
                {
                    return AsyncCompletion.FromTask(StatsTablePublisher.ReportStats(allCounters));
                }
            }
            catch (Exception exc)
            {
                var e = exc.GetBaseException();
                logger.Error(ErrorCode.AzureTable_35, "Exception occurred during Stats reporter.", exc);
            }
            return AsyncCompletion.Done;
        }

        private readonly StringBuilder logMsgBuilder = new StringBuilder();

        private void WriteStatsLogEntry(string counterData)
        {
            if (counterData == null)
            {
                // Flush remaining data
                logger.Info(ErrorCode.PerfCounterDumpAll, logMsgBuilder.ToString());
                logMsgBuilder.Clear();
                return;
            }

            int newSize = logMsgBuilder.Length + Environment.NewLine.Length + counterData.Length;
            int newSizeWithPostfix = newSize + StatsLogPostfix.Length + Environment.NewLine.Length;

            if (newSizeWithPostfix >= Logger.MAX_LOG_MESSAGE_SIZE)
            {
                // Flush pending data and start over
                logMsgBuilder.AppendLine(StatsLogPostfix);
                logger.Info(ErrorCode.PerfCounterDumpAll, logMsgBuilder.ToString());
                logMsgBuilder.Clear();
            }

            if (logMsgBuilder.Length == 0)
            {
                logMsgBuilder.AppendLine(StatsLogPrefix);
            }

            logMsgBuilder.AppendLine(counterData);
        }

        private static List<IOrleansCounter> GenerateAdditionalCounters()
        {
            if (StatisticsCollector.CollectSerializationStats)
            {
                long NumHeaders = SerializationManager.HeaderSersNumHeaders.GetCurrentValue();// CounterStatistic.FindOrCreate("Serialization.HeaderSerialization.NumHeaders").GetCurrentValue();
                long HeaderBytes = MessagingStatisticsGroup.HeaderBytesSent.GetCurrentValue();
                long TotalBytes = MessagingStatisticsGroup.TotalBytesSent.GetCurrentValue();
                float NumMessages = MessagingStatisticsGroup.MessagesSentTotal.GetCurrentValue();

                FloatValueStatistic NumHeadersPerMsg = FloatValueStatistic.CreateDoNotRegister("AutoGenerated.Serialization.Header.Serialization.NumHeadersPerMsg", () => NumHeaders / NumMessages);
                FloatValueStatistic NumHeaderBytesPerMsg = FloatValueStatistic.CreateDoNotRegister("AutoGenerated.Serialization.Header.Serialization.NumHeaderBytesPerMsg", () => HeaderBytes / NumMessages);
                FloatValueStatistic NumBodyBytesPerMsg = FloatValueStatistic.CreateDoNotRegister("AutoGenerated.Serialization.Body.Serialization.NumBodyBytesPerMsg", () => (TotalBytes - HeaderBytes) / NumMessages);

                long HeaderSerialization_ticks = SerializationManager.HeaderSerTime.GetCurrentValue(); // CounterStatistic.FindOrCreate("Serialization.HeaderSerialization.Milliseconds").GetCurrentValue();
                long HeaderSerialization_Millis = TicksToMilliSeconds(HeaderSerialization_ticks);
                long HeaderDeserialization_Millis = TicksToMilliSeconds(SerializationManager.HeaderDeserTime.GetCurrentValue());
                FloatValueStatistic HeaderSerMillisPerMessage = FloatValueStatistic.CreateDoNotRegister("AutoGenerated.Serialization.Header.Serialization.MillisPerMessage", () => HeaderSerialization_Millis / NumMessages);
                FloatValueStatistic HeaderDeserMillisPerMessage = FloatValueStatistic.CreateDoNotRegister("AutoGenerated.Serialization.Header.Deserialization.MillisPerMessage", () => HeaderDeserialization_Millis / NumMessages);

                long BodySerialization_Millis = TicksToMilliSeconds(SerializationManager.SerTimeStatistic.GetCurrentValue());
                long BodyDeserialization_Millis = TicksToMilliSeconds(SerializationManager.DeserTimeStatistic.GetCurrentValue());
                long BodyCopy_Millis = TicksToMilliSeconds(SerializationManager.CopyTimeStatistic.GetCurrentValue());
                FloatValueStatistic BodySerMillisPerMessage = FloatValueStatistic.CreateDoNotRegister("AutoGenerated.Serialization.Body.Serialization.MillisPerMessage", () => BodySerialization_Millis / NumMessages);
                FloatValueStatistic BodyDeserMillisPerMessage = FloatValueStatistic.CreateDoNotRegister("AutoGenerated.Serialization.Body.Deserialization.MillisPerMessage", () => BodyDeserialization_Millis / NumMessages);
                FloatValueStatistic BodyCopyMillisPerMessage = FloatValueStatistic.CreateDoNotRegister("AutoGenerated.Serialization.Body.DeepCopy.MillisPerMessage", () => BodyCopy_Millis / NumMessages);

                return new List<IOrleansCounter>(new FloatValueStatistic[] { NumHeadersPerMsg, NumHeaderBytesPerMsg, NumBodyBytesPerMsg,
                                    HeaderSerMillisPerMessage, HeaderDeserMillisPerMessage, BodySerMillisPerMessage, BodyDeserMillisPerMessage, BodyCopyMillisPerMessage });
            }
            else
            {
                return new List<IOrleansCounter>(new FloatValueStatistic[] { });
            }
        }

        private static long TicksToMilliSeconds(long ticks)
        {
            return (long)TimeSpan.FromTicks(ticks).TotalMilliseconds;
        }
    }
}
