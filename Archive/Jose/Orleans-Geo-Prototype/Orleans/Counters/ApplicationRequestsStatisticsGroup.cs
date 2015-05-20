using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;


namespace Orleans.Counters
{
    internal class ApplicationRequestsStatisticsGroup
    {
        private static HistogramValueStatistic appRequestsLatencyHistogram;
        private static readonly int numAppRequestsExpLatencyHistogramCategories = 31;
        private static readonly int numAppRequestsLinearLatencyHistogramCategories = 30000;
        
        private static CounterStatistic TimedOutRequests;
        private static CounterStatistic TotalAppRequests;
        private static bool PRINT_EXP_HISTOGRAM = true;

        internal static void Init(TimeSpan responseTimeout)
        {
            if (StatisticsCollector.CollectApplicationRequestsStats)
            {
                if (PRINT_EXP_HISTOGRAM)
                {
                    appRequestsLatencyHistogram = ExponentialHistogramValueStatistic.Create_ExponentialHistogram_ForTiming(
                        StatNames.STAT_APP_REQUESTS_LATENCY_HISTOGRAM, numAppRequestsExpLatencyHistogramCategories);
                }
                else
                {
                    appRequestsLatencyHistogram = LinearHistogramValueStatistic.Create_LinearHistogram_ForTiming(StatNames.STAT_APP_REQUESTS_LATENCY_HISTOGRAM,
                        numAppRequestsLinearLatencyHistogramCategories, responseTimeout);
                }
                TimedOutRequests = CounterStatistic.FindOrCreate(StatNames.STAT_APP_REQUESTS_TIMED_OUT);
                TotalAppRequests = CounterStatistic.FindOrCreate(StatNames.STAT_APP_REQUESTS_TOTAL_NUMBER_OF_REQUESTS);
            }
        }

        internal static void OnAppRequestsEnd(TimeSpan timeSpan)
        {
            appRequestsLatencyHistogram.AddData(timeSpan);
            TotalAppRequests.Increment();
        }

        internal static void OnAppRequestsTimedOut()
        {
            TimedOutRequests.Increment();
        }
    }
}

