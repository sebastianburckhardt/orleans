#if !DISABLE_STREAMS
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Orleans.Streams
{
    [Serializable]
    internal class StreamConsumerExtensionCollection
    {
        private readonly ConcurrentDictionary<Guid, IStreamConsumerExtension> _consumers;

        internal StreamConsumerExtensionCollection()
        {
            this._consumers = new ConcurrentDictionary<Guid, IStreamConsumerExtension>();
        }

        internal void AddRemoteSubscriber(IStreamConsumerExtension streamConsumer)
        {
            _consumers.TryAdd(streamConsumer.GetPrimaryKey(), streamConsumer);
        }

        internal void RemoveRemoteSubscriber(IStreamConsumerExtension streamConsumer)
        {
            IStreamConsumerExtension ignore;
            _consumers.TryRemove(streamConsumer.GetPrimaryKey(), out ignore);
            if (_consumers.Count == 0)
            {
                // TODO: Unsubscribe from PubSub?
            }
        }

        internal Task DeliverItem(StreamId streamId, object item, bool fireAndForgetDelivery)
        {
            List<Task> tasks = null;
            if (!fireAndForgetDelivery)
            {
                tasks = new List<Task>();
            }
            foreach (var kvPair in _consumers)
            {
                IStreamConsumerExtension remoteConsumer = kvPair.Value;
                Task task = remoteConsumer.DeliverItem(streamId, item, null);
                if (fireAndForgetDelivery)
                {
                    task.Ignore(); 
                }
                else
                {
                    tasks.Add(task);
                }
            }
            // If there's no subscriber, presumably we just drop the item on the floor
            if (fireAndForgetDelivery)
            {
                return TaskDone.Done;
                
            }
            else
            {
                return Task.WhenAll(tasks);
            }
        }
    }

    /// <summary>
    /// Multiplexes messages to mutiple different producers in the same grain over one grain-extension interface.
    /// 
    /// On the silo, we have one extension per activation and this extesion multiplexes all streams on this activation 
    ///     (different stream ids and different stream providers).
    /// On the client, we have one extension per stream (we bind an extesion for every StreamProducer, therefore every stream has its own extension).
    /// </summary>
    [Serializable]
    internal class SimpleMessageStreamProducerExtension : IStreamProducerExtension
    {
        private readonly Dictionary<StreamId, StreamConsumerExtensionCollection> _remoteConsumers;
        private readonly IStreamProviderRuntime     _providerRuntime;
        private readonly bool                       _fireAndForgetDelivery;
        private readonly OrleansLogger              _logger;

        internal SimpleMessageStreamProducerExtension(IStreamProviderRuntime providerRt, bool fireAndForget)
        {
            _providerRuntime = providerRt;
            _fireAndForgetDelivery = fireAndForget;
            _remoteConsumers = new Dictionary<StreamId, StreamConsumerExtensionCollection>();
            _logger = _providerRuntime.GetLogger(this.GetType().Name, Logger.LoggerType.Grain);
        }

        internal void AddStream(StreamId streamId)
        {
            StreamConsumerExtensionCollection obs;
            // no need to lock on _remoteConsumers, since on the client we have one extension per stream (per StreamProducer)
            // so this call is only made once, when StreamProducer is created.
            if (!_remoteConsumers.TryGetValue(streamId, out obs))
            {
                obs = new StreamConsumerExtensionCollection();
                _remoteConsumers.Add(streamId, obs);
            }
        }

        internal void RemoveStream(StreamId streamId)
        {
            _remoteConsumers.Remove(streamId);
        }

        internal void AddSubscribers(StreamId streamId, ICollection<PubSubSubscriptionState> newSubscribers)
        {
            if (_logger.IsVerbose)
            {
                _logger.Verbose("{0} AddSubscribers {1} for stream {2}", _providerRuntime.ExecutingEntityIdentity(), OrleansUtils.IEnumerableToString(newSubscribers), streamId);
            }
            
            StreamConsumerExtensionCollection consumers;
            if (_remoteConsumers.TryGetValue(streamId, out consumers))
            {
                foreach (var newSubscriber in newSubscribers)
                {
                    consumers.AddRemoteSubscriber(newSubscriber.Consumer);
                }
            }
            else
            {
                // AGTODO: we got an item when we don't think we're the subscriber. This is a normal race condition.
                // We can drop the item on the floor, or pass it to the rendezvous, or ...
                // GKTODO: log warning.
            }
        }

        internal Task DeliverItem(StreamId streamId, object item)
        {
            StreamConsumerExtensionCollection consumers;
            if (_remoteConsumers.TryGetValue(streamId, out consumers))
            {
                return consumers.DeliverItem(streamId, item, _fireAndForgetDelivery);
            }
            else
            {
                // AGTODO: we got an item when we don't think we're the subscriber. This is a normal race condition.
                // We can drop the item on the floor, or pass it to the rendezvous, or ...
                // GKTODO: log warning.
            }
            return TaskDone.Done;
        }

        // Called by rendezvous when new remote subsriber subscribes to this stream.
        public Task AddSubscriber(StreamId streamId, IStreamConsumerExtension streamConsumer, StreamSequenceToken token)
        {
            if (_logger.IsVerbose)
            {
                _logger.Verbose("{0} AddSubscriber {1} for stream {2}", _providerRuntime.ExecutingEntityIdentity(), streamConsumer, streamId);
            }

            StreamConsumerExtensionCollection consumers;
            if (_remoteConsumers.TryGetValue(streamId, out consumers))
            {
                consumers.AddRemoteSubscriber(streamConsumer);
            }
            else
            {
                // AGTODO: we got an item when we don't think we're the subscriber. This is a normal race condition.
                // We can drop the item on the floor, or pass it to the rendezvous, or ...
                // GKTODO: log warning.
            }
            return TaskDone.Done;
        }

        public Task RemoveSubscriber(StreamId streamId, IStreamConsumerExtension streamConsumer)
        {
            _logger.Info("{0} RemoveSubscriber {1} for stream {2}", _providerRuntime.ExecutingEntityIdentity(), streamConsumer, streamId);

            StreamConsumerExtensionCollection consumers;
            if (_remoteConsumers.TryGetValue(streamId, out consumers))
            {
                consumers.RemoveRemoteSubscriber(streamConsumer);
            }
            else
            {
                // AGTODO: we got an item when we don't think we're the subscriber. This is a normal race condition.
                // We can drop the item on the floor, or pass it to the rendezvous, or ...
                // GKTODO: log warning.
            }
            return TaskDone.Done;
        }
    }
}

#endif