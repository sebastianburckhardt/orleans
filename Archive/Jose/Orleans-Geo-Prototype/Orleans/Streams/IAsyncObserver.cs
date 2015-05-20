#if !DISABLE_STREAMS

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Orleans.Streams
{
    /// <summary>
    /// This interface generalizes the standard .NET IObserver interface to allow asynchronous production of items.
    /// <para>
    /// Note that this interface is implemented by item consumers and invoked (used) by item producers.
    /// This means that the producer endpoint of a stream implements this interface.
    /// </para>
    /// </summary>
    /// <typeparam name="T">The type of object consumed by the observer.</typeparam>
    public interface IAsyncObserver<T>
    {
        /// <summary>
        /// Passes the next item to the consumer.
        /// <para>
        /// The Task returned from this method should be completed when the item's processing has been
        /// sufficiently processed by the consumer to meet any behavioral guarantees.
        /// </para>
        /// <para>
        /// When the consumer is the (producer endpoint of) a stream, the Task is completed when the stream implementation
        /// has accepted responsibility for the item and is assured of meeting its delivery guarantees.
        /// For instance, a stream based on a durable queue would complete the Task when the item has been durably saved.
        /// A stream that provides best-effort at most once delivery would return a Task that is already complete.
        /// </para>
        /// <para>
        /// When the producer is the (consumer endpoint of) a stream, the Task should be completed by the consumer code
        /// when it has accepted responsibility for the item. 
        /// In particular, if the stream provider guarantees at-least-once delivery, then the item should not be considered
        /// delivered until the Task returned by the consumer has been completed.
        /// </para>
        /// </summary>
        /// <param name="item">The item to be passed.</param>
        /// <param name="token">The stream sequence token of this item.</param>
        /// <returns>A Task that is completed when the item has been accepted.</returns>
        Task OnNextAsync(T item, StreamSequenceToken token = null);

        /// <summary>
        /// Notifies the consumer that the stream is closed, and no more items will be delivered.
        /// <para>
        /// The Task returned from this method should be completed when the consumer is done processing the stream closure.
        /// </para>
        /// </summary>
        /// <returns>A Task that is completed when the close has been accepted.</returns>
        Task OnCompletedAsync();

        /// <summary>
        /// Notifies the consumer that the stream has been closed due to an error, and no more items will be delivered.
        /// <para>
        /// The Task returned from this method should be completed when the consumer is done processing the stream closure.
        /// </para>
        /// </summary>
        /// <param name="ex">An Exception that describes the error that forced the stream to close.</param>
        /// <returns>A Task that is completed when the close has been accepted.</returns>
        Task OnErrorAsync(Exception ex);
    }

    /// <summary>
    /// This interface generalizes the IAsyncObserver interface to allow production and consumption of batches of items.
    /// <para>
    /// Note that this interface is implemented by item consumers and invoked (used) by item producers.
    /// This means that the producer endpoint of a stream implements this interface.
    /// </para>
    /// TODO: Do we really need/want this interface? We can implement batch sending without it.
    /// </summary>
    /// <typeparam name="T">The type of object consumed by the observer.</typeparam>
    public interface IAsyncBatchObserver<T> : IAsyncObserver<T>
    {
        /// <summary>
        /// Passes the next batch of items to the consumer.
        /// <para>
        /// The Task returned from this method should be completed when all items in the batch have been
        /// sufficiently processed by the consumer to meet any behavioral guarantees.
        /// </para>
        /// <para>
        /// That is, the semantics of the returned Task is the same as for <code>OnNextAsync</code>,
        /// extended for all items in the batch.
        /// </para>
        /// </summary>
        /// <param name="batch">The items to be passed.</param>
        /// <param name="token">The stream sequence token of this batch of items.</param>
        /// <returns>A Task that is completed when the batch has been accepted.</returns>
        Task OnNextBatchAsync(IEnumerable<T> batch, StreamSequenceToken token = null);
    }

    /// <summary>
    /// This interface generalizes the standard .NET IObserveable interface to allow asynchronous consumption of items.
    /// Asynchronous here means that the consumer can process items asynchronously and signal item completion to the 
    /// producer by completing the returned Task.
    /// <para>
    /// Note that this interface is invoked (used) by item consumers and implemented by item producers.
    /// This means that the producer endpoint of a stream implements this interface.
    /// </para>
    /// </summary>
    /// <typeparam name="T">The type of object produced by the observable.</typeparam>
    public interface IAsyncObservable<T>
    {
        /// <summary>
        /// Subscribe a consumer to this observable.
        /// </summary>
        /// <param name="observer">The asynchronous observer to subscribe.</param>
        /// <returns>A promise for a StreamSubscriptionHandle that represents the subscription.
        /// The consumer may unsubscribe by using this handle.
        /// The subscription remains active for as long as it is not explicitely unsubscribed.
        /// </returns>
        Task<StreamSubscriptionHandle> SubscribeAsync(IAsyncObserver<T> observer);

        /// <summary>
        /// Subscribe a consumer to this observable.
        /// </summary>
        /// <param name="observer">The asynchronous observer to subscribe.</param>
        /// <param name="token">The stream sequence to be used as an offset to start the subscription from.</param>
        /// <returns>A promise for a StreamSubscriptionHandle that represents the subscription.
        /// The consumer may unsubscribe by using this handle.
        /// The subscription remains active for as long as it is not explicitely unsubscribed.
        /// </returns>
        Task<StreamSubscriptionHandle> SubscribeAsync(IAsyncObserver<T> observer, StreamSequenceToken token);

        /// <summary>
        /// Unsubscribe a stream consumer from this observable.
        /// </summary>
        /// <param name="handle">The stream handle to unsubscribe.</param>
        /// <returns>A promise to unsubscription action.
        /// </returns>
        Task UnsubscribeAsync(StreamSubscriptionHandle handle);

        /// <summary>
        /// Unsubscribe all stream consumers from this observable.
        /// <para>
        /// Note that this unsubscribe call applies to all stream subscriptons done in a certain processing context, 
        /// and not globally all subscriptons done in a whole distributed system.
        /// In Orleans that means all subsriptions made by a grain.
        /// </para>
        /// </summary>
        /// <returns>A promise to unsubscription action.
        /// </returns>
        Task UnsubscribeAllAsync();
    }

    /// <summary>
    /// Handle representing this subsription.
    /// Consumer may serialize and store the handle in order to unsubsribe later, for example
    /// in another activation on this grain.
    /// </summary>
    [Serializable]
    public abstract class StreamSubscriptionHandle : IEquatable<StreamSubscriptionHandle>
    {
        public abstract StreamId StreamId { get; }

        #region IEquatable<StreamSubscriptionHandle> Members

        public abstract bool Equals(StreamSubscriptionHandle other);

        #endregion
    }

    /// <summary>
    /// Handle representing stream sequence number/token.
    /// Consumer may subsribe to the stream while specifying the start of the subsription sequence token.
    /// That means that the stream infarstructure will deliver stream events starting from this sequence token.
    /// </summary>
    [Serializable]
    public abstract class StreamSequenceToken : IEquatable<StreamSequenceToken>, IComparable<StreamSequenceToken>
    {
        #region IEquatable<StreamSequenceToken> Members

        public abstract bool Equals(StreamSequenceToken other);

        #endregion

        #region IComparable<StreamSequenceToken> Members

        public abstract int CompareTo(StreamSequenceToken other);

        #endregion
    }
}

#endif