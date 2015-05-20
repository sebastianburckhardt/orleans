using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Orleans.Counters;

namespace Orleans
{
    internal abstract class AsynchQueueAgent<T> : AsynchAgent, IDisposable where T : IOutgoingMessage
    {
        private readonly IMessagingConfiguration config;
        private OrleansRuntimeQueue<T> requestQueue;
        private QueueTrackingStatistic queueTracking;

        public AsynchQueueAgent(string nameSuffix, IMessagingConfiguration cfg)
            : base(nameSuffix)
        {
            config = cfg;
            requestQueue = new OrleansRuntimeQueue<T>();
            if (StatisticsCollector.CollectQueueStats)
            {
                queueTracking = new QueueTrackingStatistic(base.Name);
            }
        }

        public void QueueRequest(T request)
        {
#if TRACK_DETAILED_STATS
            if (StatisticsCollector.CollectQueueStats)
            {
                queueTracking.OnEnQueueRequest(1, requestQueue.Count);
            }
#endif
            requestQueue.Add(request);
        }

        protected abstract void Process(T request);
        protected abstract void ProcessBatch(List<T> requests);

        protected override void Run()
        {
#if TRACK_DETAILED_STATS
            if (StatisticsCollector.CollectThreadTimeTrackingStats)
            {
                threadTracking.OnStartExecution();
                queueTracking.OnStartExecution();
            }
#endif
            try
            {
                if (config.UseMessageBatching)
                {
                    RunBatching();
                }
                else
                {
                    RunNonBatching();
                }
            }
            finally
            {
#if TRACK_DETAILED_STATS
                if (StatisticsCollector.CollectThreadTimeTrackingStats)
                {
                    threadTracking.OnStopExecution();
                    queueTracking.OnStopExecution();
                }
#endif
            }
        }


        protected void RunNonBatching()
        {            
            while (true)
            {
                if (cts.IsCancellationRequested)
                {
                    return;
                }
                T request;
                try
                {
                    request = requestQueue.Take();
                }
                catch (InvalidOperationException)
                {
                    log.Info(ErrorCode.Runtime_Error_100312, "Stop request processed");
                    break;
                }
#if TRACK_DETAILED_STATS
                if (StatisticsCollector.CollectThreadTimeTrackingStats)
                {
                    threadTracking.OnStartProcessing();
                }
#endif
                Process(request);
#if TRACK_DETAILED_STATS
                if (StatisticsCollector.CollectThreadTimeTrackingStats)
                {
                    threadTracking.OnStopProcessing();
                    threadTracking.IncrementNumberOfProcessed();
                }
#endif
            }
        }

        protected void RunBatching()
        {
            int maxBatchingSize = config.MaxMessageBatchingSize;

            while (true)
            {
                if (cts.IsCancellationRequested)
                {
                    return;
                }

                List<T> mlist = new List<T>();
                T firstRequest;
                try
                {
                    firstRequest = requestQueue.Take();
                    mlist.Add(firstRequest);

                    while (requestQueue.Count != 0 && mlist.Count < maxBatchingSize &&
                        requestQueue.First().IsSameDestination(firstRequest))
                    {
                        mlist.Add(requestQueue.Take());
                    }
                }
                catch (InvalidOperationException)
                {
                    log.Info(ErrorCode.Runtime_Error_100312, "Stop request processed");
                    break;
                }

#if TRACK_DETAILED_STATS
                if (StatisticsCollector.CollectThreadTimeTrackingStats)
                {
                    threadTracking.OnStartProcessing();
                }
#endif
                ProcessBatch(mlist);
#if TRACK_DETAILED_STATS
                if (StatisticsCollector.CollectThreadTimeTrackingStats)
                {
                    threadTracking.OnStopProcessing();
                    threadTracking.IncrementNumberOfProcessed(mlist.Count);
                }
#endif
            }
        }

        public override void Stop()
        {
#if TRACK_DETAILED_STATS
            if (StatisticsCollector.CollectThreadTimeTrackingStats)
            {
                threadTracking.OnStopExecution();
            }
#endif
            requestQueue.CompleteAdding();
            base.Stop();
        }

        public virtual int Count
        {
            get
            {
                return requestQueue.Count;
            }
        }

        #region IDisposable Members

        protected override void Dispose(bool disposing)
        {
            if (!disposing) return;
            
#if TRACK_DETAILED_STATS
            if (StatisticsCollector.CollectThreadTimeTrackingStats)
            {
                threadTracking.OnStopExecution();
            }
#endif
            base.Dispose(disposing);

            if (requestQueue != null)
            {
                requestQueue.Dispose();
                requestQueue = null;
            }
        }

        #endregion

    }
}
