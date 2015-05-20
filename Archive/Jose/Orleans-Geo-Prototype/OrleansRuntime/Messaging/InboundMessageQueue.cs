using System;
using System.Collections.Concurrent;
using Orleans.Counters;


namespace Orleans.Runtime.Messaging
{
    /// <summary>
    /// For internal use only
    /// </summary>
    internal interface IInboundMessageQueue
    {
        /// <summary>
        /// For internal use only
        /// </summary>
        int Count { get; }

        /// <summary>
        /// For internal use only
        /// </summary>
        void Stop();

        /// <summary>
        /// For internal use only
        /// </summary>
        void PostMessage(Message message);

        /// <summary>
        /// For internal use only
        /// </summary>
        Message WaitMessage(Message.Categories type);
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1711:IdentifiersShouldNotHaveIncorrectSuffix")]
    internal class InboundMessageQueue : IInboundMessageQueue
    {
        private readonly OrleansRuntimeQueue<Message>[] messageQueues;
        private readonly Logger log;
        private readonly QueueTrackingStatistic[] queueTracking;

        public int Count
        {
            get
            {
                int n = 0;
                foreach (var queue in messageQueues)
                {
                    n += queue.Count;
                }
                return n;
            }
        }

        internal InboundMessageQueue()
        {
            int n = Enum.GetValues(typeof(Message.Categories)).Length;
            messageQueues = new OrleansRuntimeQueue<Message>[n];
            queueTracking = new QueueTrackingStatistic[n];
            int i = 0;
            foreach (var category in Enum.GetValues(typeof(Message.Categories)))
            {
                messageQueues[i] = new OrleansRuntimeQueue<Message>();
                if (StatisticsCollector.CollectQueueStats)
                {
                    string queueName = "IncomingMessageAgent." + category;
                    queueTracking[i] = new QueueTrackingStatistic(queueName);
                    queueTracking[i].OnStartExecution();
                }
                i++;
            }
            log = Logger.GetLogger("Orleans.Messaging.InboundMessageQueue");
        }

        public void Stop()
        {
            if (messageQueues == null) return;
            foreach (var q in messageQueues)
            {
                q.CompleteAdding();
            }
            if (StatisticsCollector.CollectQueueStats)
            {
                foreach (var q in queueTracking)
                {
                    q.OnStopExecution();
                }
            }
        }

        public void PostMessage(Message wmsg)
        {
            wmsg.AddTimestamp(Message.LifecycleTag.EnqueueIncoming);
#if TRACK_DETAILED_STATS
            if (StatisticsCollector.CollectQueueStats)
            {
                queueTracking[(int)wmsg.Category].OnEnQueueRequest(1, messageQueues[(int)wmsg.Category].Count);
            }
#endif
            messageQueues[(int)wmsg.Category].Add(wmsg);
           
            if (log.IsVerbose3) log.Verbose3("Queued incoming {0} message", wmsg.Category.ToString());
        }

        public Message WaitMessage(Message.Categories type)
        {
            try
            {
                Message msg = messageQueues[(int)type].Take();
                return msg;
            }
            catch (InvalidOperationException)
            {
                return null;
            }
        }
    }
}
