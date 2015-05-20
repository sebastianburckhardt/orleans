#if !DISABLE_STREAMS

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Orleans.Streams
{
    /// <summary>
    /// Receives batches of messages from a single partition of a message queue.  
    /// </summary>
    public interface IQueueAdapterReceiver
    {
        QueueId Id { get; }

        /// <summary>
        /// Retrieves up to 'count' batches from a message queue in the allotted time
        /// </summary>
        /// <param name="count"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        // TODO: replace timeout with cancellation source
        Task<IEnumerable<IBatchContainer>> GetQueueMessagesAsync();

        /// <summary>
        /// Call once messages are delivered
        /// </summary>
        /// <param name="msgs"></param>
        /// <returns></returns>
        Task OnDeliveryComplete(IEnumerable<IBatchContainer> msgs);

        /// <summary>
        /// Rewind this queue receiver to an earlier point in the queue.
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        Task Rewind(StreamSequenceToken token);
    }
}
#endif