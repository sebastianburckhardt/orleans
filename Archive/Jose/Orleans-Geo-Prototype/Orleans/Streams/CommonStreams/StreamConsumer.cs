#if !DISABLE_STREAMS 

using System;
using System.Threading.Tasks;

namespace Orleans.Streams
{
    internal class StreamConsumer<T> : IAsyncObservable<T>
    {
        internal bool                               IsRewindable { get; private set; }

        private readonly StreamId                   _streamId;
        [NonSerialized]
        private readonly IStreamProviderRuntime     _providerRuntime;
        [NonSerialized]
        private readonly IStreamPubSub              _pubSub;
        private StreamConsumerExtension             _myExtension;
        private IStreamConsumerExtension            _myGrainReference;
        private bool                                _connectedToRendezvous;
        [NonSerialized]
        private readonly AsyncLock                  _initLock;
        [NonSerialized]
        private readonly Logger                     _logger;

        public StreamConsumer(StreamId id, IStreamProviderRuntime providerUtilities, IStreamPubSub pubSub, bool isRewindable)
        {
            if (id == null)
                throw new ArgumentNullException("id");
            if (providerUtilities == null)
                throw new ArgumentNullException("providerUtilities");
            if (pubSub == null)
                throw new ArgumentNullException("pubSub");

            this._logger = Logger.GetLogger(string.Format("StreamConsumer<{0}>-{1}", typeof(T).Name, id),Logger.LoggerType.Runtime); 
            this._streamId = id;
            this._providerRuntime = providerUtilities;
            this._pubSub = pubSub;
            this.IsRewindable = isRewindable;
            this._myExtension = null;
            this._myGrainReference = null;
            this._connectedToRendezvous = false;
            this._initLock = new AsyncLock();
        }

        public Task<StreamSubscriptionHandle> SubscribeAsync(IAsyncObserver<T> observer)
        {
            return SubscribeAsync(observer, null);
        }

        public async Task<StreamSubscriptionHandle> SubscribeAsync(IAsyncObserver<T> observer, StreamSequenceToken token)
        {
            if (token != null && !IsRewindable)
            {
                throw new ArgumentNullException("token", "Passing a non-null token to a non-rewindable IAsyncObservable.");
            }
            using (await _initLock.LockAsync())
            {
                if (_logger.IsVerbose)
                    _logger.Verbose("Subscribe Observer={0} Token={1}", observer, token);
                if (_myExtension == null)
                {
                    if (_logger.IsVerbose2)
                        _logger.Verbose2("Subscribe - Binding local extension to stream runtime={0}", _providerRuntime);
                    var tup =
                        await this._providerRuntime.BindExtension<StreamConsumerExtension, IStreamConsumerExtension>(
                            () =>
                                new StreamConsumerExtension(_providerRuntime));
                    this._myExtension = tup.Item1;
                    this._myGrainReference = tup.Item2;
                    if (_logger.IsVerbose2)
                        _logger.Verbose2("Subscribe - Connected Extension={0} GrainRef={1}", _myExtension, _myGrainReference);
                }

                if (!_connectedToRendezvous)
                {
                    if (_logger.IsVerbose)
                        _logger.Verbose("Subscribe - Connecting to Rendezvous {0}", _pubSub);
                    // Send a message to the Stream Rendezvous grain to let it know that this grain is now a subscriber
                    await _pubSub.RegisterConsumer(_streamId, _myGrainReference, token);
                    _connectedToRendezvous = true;
                }

                return _myExtension.AddObserver(_streamId, observer);
            }
        }

        public async Task UnsubscribeAsync(StreamSubscriptionHandle handle)
        {
            if (_myExtension == null || !_connectedToRendezvous)
            {
                throw new InvalidOperationException(String.Format("Called UnsubscribeAsync with StreamSubscriptionHandle {0} which was not properly Subscribed Async.", handle));
            }
            using (await _initLock.LockAsync())
            {
                if (_logger.IsVerbose)
                    _logger.Verbose("Unsubscribe StreamSubscriptionHandle={0}", handle);
                bool shouldUnsubscribe = _myExtension.RemoveObserver<T>(handle);
                if (!shouldUnsubscribe)
                {
                    return;
                }

                try
                {
                    if (_logger.IsVerbose)
                        _logger.Verbose("Subscribe - Disconnecting from Rendezvous {0} My GrainRef={1}", _pubSub, _myGrainReference);
                    await _pubSub.UnregisterConsumer(_streamId, _myGrainReference);
                }
                finally
                {
                    _connectedToRendezvous = false;
                }
            }
        }

        public Task UnsubscribeAllAsync()
        {
            throw new NotImplementedException("UnsubscribeAllAsync not implemented yet.");
        }

        internal Task<int> DiagGetConsumerObserversCount()
        {
            return Task.FromResult(_myExtension.DiagCountStreamObservers<T>(_streamId));
        }
    }
}

#endif