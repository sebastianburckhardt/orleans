#if !DISABLE_STREAMS
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.Providers;

namespace Orleans.Streams
{
    internal class PersistentStreamProducer<T> : IAsyncBatchObserver<T>
    {
        private readonly StreamId      _streamId;
        private readonly IQueueAdapter _queueAdapter;

        internal bool IsRewindable { get; private set; }

        internal PersistentStreamProducer(StreamId id, IStreamProviderRuntime providerUtilities, IQueueAdapter queueAdapter, bool isRewindable)
        {
            this._streamId = id;
            this._queueAdapter = queueAdapter;
            this.IsRewindable = isRewindable;
            providerUtilities.GetLogger(this.GetType().Name, Logger.LoggerType.Application)
                .Info("Created PersistentStreamProducer for stream {0}, of type {1}, and with Adapter: {2}.",
                      id.ToString(), typeof(T), _queueAdapter.Name);
        }

        public Task OnNextAsync(T item, StreamSequenceToken token)
        {
            if (token != null && !IsRewindable)
            {
                throw new ArgumentNullException("token", "Passing a non-null token to a non-rewindable IAsyncBatchObserver.");
            }
            return _queueAdapter.QueueMessageAsync(_streamId, item);
        }

        public Task OnNextBatchAsync(IEnumerable<T> batch, StreamSequenceToken token)
        {
            if (token != null && !IsRewindable)
            {
                throw new ArgumentNullException("token", "Passing a non-null token to a non-rewindable IAsyncBatchObserver.");
            }
            return _queueAdapter.QueueMessageBatchAsync(_streamId, batch);
        }

        public Task OnCompletedAsync()
        {
            // GKTODO: Send a close message to the rendezvous
            throw new NotImplementedException("OnCompletedAsync is not implemented for now.");
        }

        public Task OnErrorAsync(Exception ex)
        {
            // TODO: Send a close message to the rendezvous
            throw new NotImplementedException("OnErrorAsync is not implemented for now.");
        }
    }
}

#endif