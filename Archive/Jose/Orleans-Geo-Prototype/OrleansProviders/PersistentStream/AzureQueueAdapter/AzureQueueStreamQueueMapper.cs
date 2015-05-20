#if !DISABLE_STREAMS 
using System;
using System.Collections.Generic;
using Orleans.Streams;

namespace Orleans.Providers.Streams.Persistent.AzureQueueAdapter
{
    public class AzureQueueStreamQueueMapper : IStreamQueueMapper
    {
        private static readonly uint NUM_QUEUES = 8; // keep as power of 2.
        private readonly HashRing<QueueId> hashRing;

        internal AzureQueueStreamQueueMapper()
        {
            List<QueueId> queueIds = new List<QueueId>((int)NUM_QUEUES);
            uint portion = HashRing<QueueId>.RING_SIZE / NUM_QUEUES + 1;
            for (uint i = 0; i < NUM_QUEUES; i++)
            {
                uint uniformHashCode = portion * i;
                queueIds.Add(QueueId.GetQueueId(i, uniformHashCode));
            }
            hashRing = new HashRing<QueueId>(queueIds);
        }

        public IEnumerable<QueueId> GetQueuesForRange(IRingRange range)
        {
            foreach (QueueId queueId in hashRing.GetAllRingMembers())
            {
                if (range.InRange(queueId.GetUniformHashCode()))
                {
                    yield return queueId;
                }
            }
        }

        public IEnumerable<QueueId> GetAllQueues()
        {
            return hashRing.GetAllRingMembers();
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1011:ConsiderPassingBaseTypesAsParameters")]
        internal QueueId GetQueueForStream(StreamId streamId)
        {
            return hashRing.CalculateResponsible(streamId);
        }

        public override string ToString()
        {
            return hashRing.ToString();
        }
    }

}

#endif