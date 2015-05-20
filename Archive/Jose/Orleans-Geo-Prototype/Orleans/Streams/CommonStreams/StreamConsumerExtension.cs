#if !DISABLE_STREAMS
using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace Orleans.Streams
{
    /// <summary>
    /// The extesion multiplexes all stream related messages to this grain between different streams and their stream observers.
    /// 
    /// On the silo, we have one extension object per activation and this extesion multiplexes all streams on this activation 
    ///     (streams of all types and ids: different stream ids and different stream providers).
    /// On the client, we have one extension per stream (we bind an extesion for every StreamConsumer, therefore every stream has its own extension).
    /// </summary>
    [Serializable]
    internal class StreamConsumerExtension : IStreamConsumerExtension
    {
        private interface IStreamObservers
        {
            Task DeliverItem(object item, StreamSequenceToken token);
        }

        [Serializable]
        private class ObserversCollection<T> : IStreamObservers
        {
            private readonly ConcurrentDictionary<Guid, ObserverWrapper<T>> _localObservers;

            internal ObserversCollection()
            {
                _localObservers = new ConcurrentDictionary<Guid, ObserverWrapper<T>>();
            }

            internal void AddObserver(ObserverWrapper<T> observer)
            {
                _localObservers.TryAdd(observer.ObserverGuid, observer);
            }

            internal void RemoveObserver(ObserverWrapper<T> observer)
            {
                ObserverWrapper<T> ignore;
                _localObservers.TryRemove(observer.ObserverGuid, out ignore);
            }

            internal bool IsEmpty
            {
                get { return _localObservers.IsEmpty; }
            }

            public Task DeliverItem(object item, StreamSequenceToken token)
            {
                var batch = item as IBatchContainer;
                return batch != null
                    ? DeliverBatchItem(batch, token)
                    : DeliverTypedItem(item, token);
            }

            internal int Count
            {
                get { return _localObservers.Count; }
            }

            private async Task DeliverTypedItem(object item, StreamSequenceToken token)
            {
                T typedItem;
                try
                {
                    typedItem = (T)item;
                }
                catch (InvalidCastException)
                {
                    // We got an illegal item on the stream -- close it with a Cast exception
                    throw new InvalidCastException("Received an item of type " + item.GetType().Name + ", expected "
                                                 + typeof(T).FullName);
                }
                foreach (var kvPair in _localObservers)
                {
                    ObserverWrapper<T> observer = kvPair.Value;
                    // AGTODO: flow control to not pass in this item if the streamConsumer is still processing another item
                    // (that is, don't ignore the Task, use it for synchronization, keep an internal queue, etc.).
                    await observer.OnNextAsync(typedItem, token);
                }
            }

            private async Task DeliverBatchItem(IBatchContainer batch, StreamSequenceToken token)
            {
                foreach (var item in batch.GetEvents<T>())
                {
                    await DeliverTypedItem(item, token);
                }
            }
        }

        private readonly IStreamProviderRuntime providerRuntime;
        private readonly ConcurrentDictionary<StreamId, IStreamObservers> allStreamObservers; // map to different ObserversCollection<T> of different Ts.
        private readonly OrleansLogger logger;
        private readonly object lockable;

        internal StreamConsumerExtension(IStreamProviderRuntime providerRt)
        {
            this.providerRuntime = providerRt;
            this.allStreamObservers = new ConcurrentDictionary<StreamId, IStreamObservers>();
            this.logger = providerRuntime.GetLogger(this.GetType().Name, Logger.LoggerType.Grain);
            this.lockable = new object();
        }

        internal StreamSubscriptionHandle AddObserver<T>(StreamId streamId, IAsyncObserver<T> observer)
        {
            if (null == streamId)
                throw new ArgumentNullException("streamId");
            if (null == observer)
                throw new ArgumentNullException("observer");

            try
            {
                if (logger.IsVerbose)
                    logger.Verbose("{0} AddObserver for stream {1}", providerRuntime.ExecutingEntityIdentity(), streamId);
                IStreamObservers obs;
                lock (lockable)
                {
                    if (!allStreamObservers.TryGetValue(streamId, out obs))
                    {
                        obs = new ObserversCollection<T>();
                        allStreamObservers.TryAdd(streamId, obs);
                    }
                }
                ObserverWrapper<T> wrapper = new ObserverWrapper<T>(observer, streamId);
                ((ObserversCollection<T>)obs).AddObserver(wrapper);
                return wrapper;
            }
            catch (Exception exc)
            {
                logger.Error((int)ErrorCode.StreamProvider_AddObserverException, 
                    String.Format("{0} StreamConsumerExtension.AddObserver({1}) caugth exception.", providerRuntime.ExecutingEntityIdentity(), streamId), 
                    exc);
                throw;
            }
        }

        internal bool RemoveObserver<T>(StreamSubscriptionHandle handle)
        {
            ObserverWrapper<T> observerWrapper = (ObserverWrapper<T>)handle;
            IStreamObservers obs;
            lock (lockable)
            {
                if (!allStreamObservers.TryGetValue(observerWrapper.StreamId, out obs))
                {
                    throw new InvalidOperationException("Trying to remove observer from a stream that is not registered in the StreamConsumerExtension.");
                }
                ObserversCollection<T> observersCollection = (ObserversCollection<T>)obs;

                observersCollection.RemoveObserver(observerWrapper);
                observerWrapper.Clear();
                if (observersCollection.IsEmpty)
                {
                    IStreamObservers ignore;
                    allStreamObservers.TryRemove(observerWrapper.StreamId, out ignore);
                    // if we don't have any more subsribed streams, unsubsribe the extension.
                    return true;
                }
                return false;
            }
        }

        public Task DeliverItem(StreamId streamId, object item, StreamSequenceToken token)
        {
#if DEBUG
            if (logger.IsVerbose3)
                logger.Verbose3("DeliverItem {0} for stream {1}", item, streamId);
#endif
            IStreamObservers observers;
            if (allStreamObservers.TryGetValue(streamId, out observers))
            {
                return observers.DeliverItem(item, token);
            }
            else
            {
                logger.Warn((int)(ErrorCode.StreamProvider_NoStreamForItem), 
                    "{0} got an item for stream {1}, but I don't have any subscriber for that stream. Dropping on the floor.", 
                    providerRuntime.ExecutingEntityIdentity(), streamId);
                // AGTODO: we got an item when we don't think we're the subscriber. This is a normal race condition.
                // We can drop the item on the floor, or pass it to the rendezvous, or ...
            }
            return TaskDone.Done;
        }

        public Task Complete(StreamId streamId, Exception ex)
        {
            throw new NotImplementedException();
        }

        internal int DiagCountStreamObservers<T>(StreamId streamId)
        {
            return ((ObserversCollection<T>) allStreamObservers[streamId]).Count;
        }
    }

    /// <summary>
    /// Wraps a single application observer object, mainly to add Dispose fuctionality.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    [Serializable]
    internal class ObserverWrapper<T> : StreamSubscriptionHandle, IAsyncObserver<T>
    {
        public override StreamId StreamId { get { return _streamId; } }

        [NonSerialized]
        private IAsyncObserver<T>           _observer;
        private readonly StreamId           _streamId;
        internal readonly Guid              ObserverGuid;

        public ObserverWrapper(IAsyncObserver<T> observer, StreamId streamId)
        {
            this._observer = observer;
            this.ObserverGuid = Guid.NewGuid();
            this._streamId = streamId;
        }

        public Task OnNextAsync(T item, StreamSequenceToken token)
        {
            // This method could potentially be invoked after Dispose() has been called, 
            // so we have to ignore the request or we risk breaking unit tests AQ_01 - AQ_04.
            if (_observer == null)
                return TaskDone.Done;

            return _observer.OnNextAsync(item, token);
        }

        public Task OnCompletedAsync()
        {
            if (_observer == null)
                return TaskDone.Done;

            return _observer.OnCompletedAsync();
        }

        public Task OnErrorAsync(Exception ex)
        {
            if (_observer == null)
                return TaskDone.Done;
            
            return _observer.OnErrorAsync(ex);
        }

        #region IEquatable<StreamId> Members

        public override bool Equals(StreamSubscriptionHandle other)
        {
            var o = other as ObserverWrapper<T>;
            return o != null && ObserverGuid == o.ObserverGuid;
        }

        #endregion

        public override bool Equals(object obj)
        {
            return Equals(obj as ObserverWrapper<T>);
        }

        public override int GetHashCode()
        {
            return ObserverGuid.GetHashCode();
        }

        internal void Clear()
        {
            _observer = null;
        }

        public override string ToString()
        {
            return String.Format("StreamSubscriptionHandle:Stream={0},ObserverId={1}", StreamId, ObserverGuid);
        }
    }
}

#endif
