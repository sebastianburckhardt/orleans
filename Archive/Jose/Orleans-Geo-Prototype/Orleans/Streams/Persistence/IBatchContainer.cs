#if !DISABLE_STREAMS

using System.Collections.Generic;

namespace Orleans.Streams
{
    /// <summary>
    /// Each queue message is allowed to be a heterogeneous  ordered set of events.  IBatchContainer contains these events and allows users to query the batch for a specific type of event.
    /// </summary>
    public interface IBatchContainer
    {
        /// <summary>
        /// Stream identifier for the stream this batch is part of
        /// </summary>
        StreamId StreamId { get; }

        /// <summary>
        /// Gets events of a specific type from the batch.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        IEnumerable<T> GetEvents<T>();

        /// <summary>
        /// Sequence Token for that batch of events.
        /// </summary>
        StreamSequenceToken Token { get; }
    }
}
#endif