#if !DISABLE_STREAMS

using System.Collections.Generic;
using System.Threading.Tasks;

namespace Orleans.Streams
{
    /// <summary>
    /// Stream queue storage adapter.  This is an abstraction layer that hides the implementation details of the underlying queuing system.
    /// </summary>
    public interface IQueueAdapter
    {
        /// <summary>
        /// Name of the adapter. Primarily for logging purposes
        /// </summary>
        string Name { get; }

        /// <summary>
        /// Creates a quere receiver for the specificed queueId
        /// </summary>
        /// <param name="queueId"></param>
        /// <returns></returns>
        Task<IQueueAdapterReceiver> CreateReceiver(QueueId queueId);

        /// <summary>
        /// Writes a set of events to the queue as a single batch associated with the provided streamId.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="streamId"></param>
        /// <param name="events"></param>
        /// <returns></returns>
        Task QueueMessageAsync<T>(StreamId streamId, T events);

        /// <summary>
        /// Writes a set of events to the queue as a single batch associated with the provided streamId.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="streamId"></param>
        /// <param name="events"></param>
        /// <returns></returns>
        Task QueueMessageBatchAsync<T>(StreamId streamId, IEnumerable<T> events);

        /// <summary>
        /// Returns IStreamQueueMapper.
        /// </summary>
        /// <returns></returns>
        IStreamQueueMapper GetStreamQueueMapper();

        /// <summary>
        /// Determines whether this is a rewindable stream adapter - supports subscribing from previous point in time.
        /// </summary>
        /// <returns>True if this is a rewindable stream adapter, false otherwise.</returns>
        bool IsRewindable { get; }
    }
}
#endif