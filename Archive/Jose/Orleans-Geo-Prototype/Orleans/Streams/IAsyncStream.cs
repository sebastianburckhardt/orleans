#if !DISABLE_STREAMS

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Orleans.Streams
{
    /// <summary>
    /// This interface represents an object that serves as a distributed rendevous between producers and consumers.
    /// It is similar to a Reactive Framework <code>Subject</code>, but different in that it implements
    /// neither <code>IObserver</code> nor <code>IObservable</code> itself; rather, it provides a way to obtain
    /// implementations of these interfaces.
    /// This allows the creator of a stream to pass a single endpoint (consumer or producer) to a component, rather
    /// than the full (double-sided) stream itself, providing some level of usage control.
    /// </summary>
    /// <typeparam name="T">The type of object that flows through the stream.</typeparam>
    public interface IAsyncStream<T>
    {
        /// <summary>
        /// Gets an interface to the stream suitable for a producer to use.
        /// TODO: Should this just return an IAsyncObserver, either if we don't support batching or if some providers don't?
        /// </summary>
        /// <returns>The producer-side interface.</returns>
        IAsyncBatchObserver<T> GetProducerInterface();

        /// <summary>
        /// Gets an interface to the stream suitable for a consumer to use.
        /// </summary>
        /// <returns>The consumer-side interface.</returns>
        IAsyncObservable<T> GetConsumerInterface();

        /// <summary>
        /// Closes the stream.
        /// </summary>
        /// <param name="ex">If not null, an Exception that describes the error that is forcing the stream to be closed.</param>
        /// <returns>A Task that completes when the stream closure has been accepted by the runtime.</returns>
        Task CloseAsync(Exception ex = null);

        /// <summary>
        /// Determines whether this is a rewindable stream - supports subscribing from previous point in time.
        /// </summary>
        /// <returns>True if this is a rewindable stream, false otherwise.</returns>
        bool IsRewindable { get; }
    }
}

#endif