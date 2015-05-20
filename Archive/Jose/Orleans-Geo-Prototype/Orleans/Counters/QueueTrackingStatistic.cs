using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;


namespace Orleans.Counters
{
    internal class QueueTrackingStatistic
    {
        private AverageValueStatistic AverageQueueSizeCounter;
        private CounterStatistic NumEnqueuedRequestsCounter;
        private ITimeInterval totalExecutionTime;                  // total time this time is being tracked
        private FloatValueStatistic AverageArrivalRate;

        public QueueTrackingStatistic(string queueName, CounterStorage storage = CounterStorage.LogOnly)
        {
            if (StatisticsCollector.CollectQueueStats)
            {
                AverageQueueSizeCounter = AverageValueStatistic.FindOrCreate(new StatName(StatNames.STAT_QUEUES_QUEUE_SIZE_AVERAGE_PER_QUEUE, queueName), storage);
                NumEnqueuedRequestsCounter = CounterStatistic.FindOrCreate(new StatName(StatNames.STAT_QUEUES_ENQUEUED_PER_QUEUE, queueName), storage);

                totalExecutionTime = TimeIntervalFactory.CreateTimeInterval(StatisticsCollector.MeasureFineGrainedTime);
                AverageArrivalRate = FloatValueStatistic.FindOrCreate(new StatName(StatNames.STAT_QUEUES_AVERAGE_ARRIVAL_RATE_PER_QUEUE, queueName),
                   () =>
                   {
                       TimeSpan totalTime = totalExecutionTime.Elapsed;
                       if (totalTime.Ticks == 0) return 0;
                       long numReqs = NumEnqueuedRequestsCounter.GetCurrentValue();
                       return (float)((((double)numReqs * (double)TimeSpan.TicksPerSecond)) / (double)totalTime.Ticks);
                   }, storage);
            } 
        }

        public void OnEnQueueRequest(int numEnqueuedRequests, int queueLenght)
        {
            NumEnqueuedRequestsCounter.IncrementBy(numEnqueuedRequests);
            AverageQueueSizeCounter.AddValue(queueLenght);
        }

        public float AverageQueueLength { get { return AverageQueueSizeCounter.GetAverageValue(); } }
        public long NumEnqueuedRequests { get { return NumEnqueuedRequestsCounter.GetCurrentValue(); } }
        public float ArrivalRate { get { return AverageArrivalRate.GetCurrentValue(); } }

        public void OnStartExecution()
        {
            totalExecutionTime.Start();
        }

        public void OnStopExecution()
        {
            totalExecutionTime.Stop();
        }
    }
}
