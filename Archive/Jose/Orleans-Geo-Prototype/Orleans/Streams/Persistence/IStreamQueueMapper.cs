#if !DISABLE_STREAMS 
using System;
using System.Collections.Generic;
using Orleans.Streams;

namespace Orleans.Streams
{
    public interface IStreamQueueMapper
    {
        IEnumerable<QueueId> GetQueuesForRange(IRingRange range);

        IEnumerable<QueueId> GetAllQueues();
    }

}

#endif