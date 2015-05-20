#if !DISABLE_STREAMS

using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Orleans.Streams
{
    internal class SimpleMessageStreamProducer<T> : IAsyncBatchObserver<T>
    {
        private readonly StreamId                               _streamId;
        [NonSerialized]
        private readonly IStreamProviderRuntime                 _providerRuntime;
        private SimpleMessageStreamProducerExtension            _myExtension;
        private IStreamProducerExtension                        _myGrainReference;
        private bool                                            _connectedToRendezvous;
        private readonly bool                                   _fireAndForgetDelivery;
        [NonSerialized]
        private bool                                            _isDisposed;
        [NonSerialized]
        private readonly OrleansLogger                          _logger;
        [NonSerialized]
        private readonly object                                 _schedulingContext;
        [NonSerialized]
        private readonly AsyncLock                              _initLock;
        internal bool IsRewindable { get; private set; }

        internal SimpleMessageStreamProducer(StreamId id, IStreamProviderRuntime providerUtilities, bool fireAndForgetDelivery, bool isRewindable)
        {
            this._streamId = id;
            this._providerRuntime = providerUtilities;
            this._connectedToRendezvous = false;
            this._fireAndForgetDelivery = fireAndForgetDelivery;
            this.IsRewindable = isRewindable;
            this._isDisposed = false;
            this._logger = _providerRuntime.GetLogger(GetType().Name, Logger.LoggerType.Application);
            this._schedulingContext = providerUtilities.GetCurrentSchedulingContext();
            this._initLock = new AsyncLock();

            ConnectToRendezvous().Ignore();
        }

        private async Task<ISet<PubSubSubscriptionState>> RegisterProducer()
        {
            var tup =
                await this._providerRuntime.BindExtension<SimpleMessageStreamProducerExtension, IStreamProducerExtension>(
                    () =>
                        new SimpleMessageStreamProducerExtension(_providerRuntime, _fireAndForgetDelivery));
            _myExtension = tup.Item1;
            _myGrainReference = tup.Item2;

            _myExtension.AddStream(_streamId);

            // Notify streamRendezvous about new stream streamProducer. Retreave the list of RemoteSubscribers.
            return await _providerRuntime.PubSub(StreamPubSubType.GRAINBASED).RegisterProducer(_streamId, _myGrainReference);
        }

        private async Task ConnectToRendezvous()
        {
            if (_isDisposed)
                throw new ObjectDisposedException(GetType().Name);

            // the caller should check _connectedToRendezvous before calling this method.
            using (await _initLock.LockAsync())
            {
                if (!_connectedToRendezvous) // need to re-check again.
                {
                    var remoteSubscribers = await RegisterProducer();
                    _myExtension.AddSubscribers(_streamId, remoteSubscribers);
                    _connectedToRendezvous = true;
                }
            }
        }

        ~SimpleMessageStreamProducer()
        {
            try
            {
                _logger.Info("~SimpleMessageStreamProducer() called");
                // right now, we're running on the finalization thread, which can't make calls to Orleans grain methods.
                // we use InvokeWithinSchedulingContextAsync() to remedy this.
                _myExtension.RemoveStream(_streamId);
                _providerRuntime.InvokeWithinSchedulingContextAsync(() => DisposeAsync(), _schedulingContext).Ignore();
            }
            catch (Exception e)
            {
                // note: _logger must be readonly for this to be guaranteed to be safe.
                _logger.Error((int)ErrorCode.StreamProvider_FailedToDispose, "Unhandled exception while disposing SimpleMessageStreamProducer", e);
            }
        }

        public async Task OnNextAsync(T item, StreamSequenceToken token)
        {
            if (token != null && !IsRewindable)
            {
                throw new ArgumentNullException("token", "Passing a non-null token to a non-rewindable IAsyncBatchObserver.");
            }

            if (_isDisposed)
                throw new ObjectDisposedException(GetType().Name);

            if (!_connectedToRendezvous)
                await ConnectToRendezvous();
            await _myExtension.DeliverItem(_streamId, item);
        }

        public Task OnNextBatchAsync(IEnumerable<T> batch, StreamSequenceToken token)
        {
            if (token != null && !IsRewindable)
            {
                throw new ArgumentNullException("token", "Passing a non-null token to a non-rewindable IAsyncBatchObserver.");
            }

            throw new NotImplementedException("We still don't support OnNextBatchAsync()");
        }

        public Task OnCompletedAsync()
        {
            throw new NotImplementedException("OnCompletedAsync is not implemented for now.");
        }

        public Task OnErrorAsync(Exception ex)
        {
            throw new NotImplementedException("OnErrorAsync is not implemented for now.");
        }

        internal Action OnDisposeTestHook { get; set; }

        private async Task DisposeAsync()
        {
            _logger.Info("DisposeAsync() called");

            // todo: this class needs to be tuned for thread-safety because it can be used from the grain client environment.
            if (_isDisposed)
                return;

            if (_connectedToRendezvous)
            {
                try
                {
                    await _providerRuntime.PubSub(StreamPubSubType.GRAINBASED).UnregisterProducer(_streamId, _myGrainReference);
                }
                catch (Exception exc)
                {
                    _logger.Warn((int)ErrorCode.StreamProvider_ProducerFailedToUnregister, "Unhandled exception while PubSub(StreamPubSubType.SMS).UnregisterProducer", exc);
                }
            }
            _isDisposed = true;
            Action onDisposeTestHook = OnDisposeTestHook; // capture
            if (onDisposeTestHook != null)
                onDisposeTestHook();
        }
    }
}

#endif