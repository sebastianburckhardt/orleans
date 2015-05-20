#if !DISABLE_STREAMS 

using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Orleans.Streams
{
    //[Reentrant]
    [StorageProvider(ProviderName = "PubSubStore")]
    internal class PubSubRendezvousGrain : GrainBase<IPubSubGrainState>, IPubSubRendezvousGrain
    {
        private OrleansLogger logger;
        private const bool DEBUG_PUB_SUB = false;

        public override Task ActivateAsync()
        {
            logger = base.GetLogger(this.GetType().Name + "-" + base.RuntimeIdentity + "-" + base.IdentityString);
            LogPubSubCounts("ActivateAsync");

            RemoveDeadProducers();
            
            if (State.Consumers == null)
            {
                State.Consumers = new HashSet<PubSubSubscriptionState>();
            }
            if (State.Producers == null)
            {
                State.Producers = new HashSet<PubSubPublisherState>();
            }
            if (DEBUG_PUB_SUB || logger.IsVerbose)
            {
                logger.Info("ActivateAsync-Done");
            }
            return TaskDone.Done;
        }

        private void RemoveDeadProducers()
        {
            // Remove only those we know for sure are Dead.
            int numRemoved = State.Producers.RemoveWhere(producerState =>
                {
                    return IsDeadProducer(producerState.Producer);
                });
            if (numRemoved > 0)
            {
                LogPubSubCounts(String.Format("RemoveDeadProducers: removed {0} outdated producers", numRemoved));
            }
        }

        /// accept and notify only Active producers.
        private bool IsActiveProducer(IStreamProducerExtension producer)
        {
            GrainReference grainRef = producer.AsReference();
            if (grainRef.GrainId.IsSystemTarget && grainRef.IsInitializedSystemTarget)
            {
                return GrainClient.InternalCurrent.GetSiloStatus(grainRef.SystemTargetSilo).Equals(SiloStatus.Active);
            }
            return true;
        }

        private bool IsDeadProducer(IStreamProducerExtension producer)
        {
            GrainReference grainRef = producer.AsReference();
            if (grainRef.GrainId.IsSystemTarget && grainRef.IsInitializedSystemTarget)
            {
                return GrainClient.InternalCurrent.GetSiloStatus(grainRef.SystemTargetSilo).Equals(SiloStatus.Dead);
            }
            return true;
        }

        public async Task<ISet<PubSubSubscriptionState>> RegisterProducer(StreamId streamId, IStreamProducerExtension streamProducer)
        {
            if (!IsActiveProducer(streamProducer))
            {
                throw new ArgumentException(String.Format("Trying to register noon active IStreamProducerExtension: {}", streamProducer.ToString()), "streamProducer");
            }
            var publisherState = new PubSubPublisherState(streamId, streamProducer);
            State.Producers.Add(publisherState);
            LogPubSubCounts("RegisterProducer {0}", streamProducer);
            await State.WriteStateAsync();
            return State.Consumers;
        }

        public async Task UnregisterProducer(StreamId streamId, IStreamProducerExtension streamProducer)
        {
            int numRemoved = State.Producers.RemoveWhere(s => s.Equals(streamId, streamProducer));
            LogPubSubCounts("UnregisterProducer {0} NumRemoved={1}", streamProducer, numRemoved);
            await State.WriteStateAsync();
        }

        public async Task RegisterConsumer(StreamId streamId, IStreamConsumerExtension streamConsumer, StreamSequenceToken token)
        {
            var pubSubState = new PubSubSubscriptionState(streamId, streamConsumer, token);
            State.Consumers.Add(pubSubState);
            LogPubSubCounts("RegisterConsumer {0}", streamConsumer);
            await State.WriteStateAsync();

            int numProducers = State.Producers.Count;
            if (numProducers > 0)
            {
                if (DEBUG_PUB_SUB || logger.IsVerbose)
                {
                    logger.Info("Notifying {0} existing producer(s) about new consumer {1}. Producers={2}", 
                        numProducers, streamConsumer, Utils.IEnumerableToString(State.Producers));
                }
                // Notify producers about a new streamConsumer.
                List<Task> promises = new List<Task>();
                foreach (var producerState in State.Producers.Where(producerState => IsActiveProducer(producerState.Producer)))
                {
                    promises.Add(producerState.Producer.AddSubscriber(streamId, streamConsumer, token));
                }
                await Task.WhenAll(promises);
            }
        }

        public async Task UnregisterConsumer(StreamId streamId, IStreamConsumerExtension streamConsumer)
        {
            int numRemoved = State.Consumers.RemoveWhere(c => c.Equals(streamId, streamConsumer));
            LogPubSubCounts("UnregisterConsumer {0} NumRemoved={1}", streamConsumer, numRemoved);
            await State.WriteStateAsync();

            int numProducers = State.Producers.Count;
            if (numProducers > 0)
            {
                if (DEBUG_PUB_SUB || logger.IsVerbose)
                {
                    logger.Info("Notifying {0} existing producers about unregistered consumer.", numProducers);
                }
                // Notify producers about unregistered consumer.
                List<Task> promises = new List<Task>();
                foreach (var producerState in State.Producers.Where(producerState => IsActiveProducer(producerState.Producer)))
                {
                    promises.Add(producerState.Producer.RemoveSubscriber(streamId, streamConsumer));
                }
                await Task.WhenAll(promises);
            }
        }

        public Task<int> ProducerCount(StreamId streamId)
        {
            return Task.FromResult(State.Producers.Count);
        }

        public Task<int> ConsumerCount(StreamId streamId)
        {
            return Task.FromResult(GetConsumersForStream(streamId).Length);
        }

        public Task<PubSubSubscriptionState[]> DiagGetConsumers(StreamId streamId)
        {
            return Task.FromResult(GetConsumersForStream(streamId));
        }

        // ---------- Utility functions ----------

        private PubSubSubscriptionState[] GetConsumersForStream(StreamId streamId)
        {
            return State.Consumers.Where(c => c.Stream.Equals(streamId)).ToArray();
        }

        private void LogPubSubCounts(string fmt, params object[] args)
        {
            int numProducers = State.Producers.Count;
            int numConsumers = State.Consumers.Count;

            if (DEBUG_PUB_SUB || logger.IsVerbose)
            {
                string when = args != null && args.Length != 0 ? String.Format(fmt, args) : fmt;
                logger.Info("{0}. Now have total of {1} producers and {2} consumers. All Consumers = {3}, All Producers = {4}",
                    when, numProducers, numConsumers, Utils.IEnumerableToString(State.Consumers), Utils.IEnumerableToString(State.Producers));
            }
        }

        // Check that what we have cached locally matches what is in the persistent table.
        public async Task Validate()
        {
            var captureProducers = State.Producers;
            var captureConsumers = State.Consumers;

            await State.ReadStateAsync();
            
            if (captureProducers.Count != State.Producers.Count)
            {
                throw new Exception(String.Format("State mismatch between PubSubRendezvousGrain and its persistent state. captureProducers.Count={0}, State.Producers.Count={1}",
                    captureProducers.Count, State.Producers.Count));
            }
            foreach (var producer in captureProducers)
            {
                if (!State.Producers.Contains(producer))
                {
                    throw new Exception(String.Format("State mismatch between PubSubRendezvousGrain and its persistent state. captureProducers={0}, State.Producers={1}",
                        Utils.IEnumerableToString(captureProducers), Utils.IEnumerableToString(State.Producers)));
                }
            }

            if (captureConsumers.Count != State.Consumers.Count)
            {
                LogPubSubCounts("");
                throw new Exception(String.Format("State mismatch between PubSubRendezvousGrain and its persistent state. captureConsumers.Count={0}, State.Consumers.Count={1}",
                        captureConsumers.Count, State.Consumers.Count));
            }
            foreach (PubSubSubscriptionState consumer in captureConsumers)
            {
                if (!State.Consumers.Contains(consumer))
                {
                    throw new Exception(String.Format("State mismatch between PubSubRendezvousGrain and its persistent state. captureConsumers={0}, State.Consumers={1}",
                        Utils.IEnumerableToString(captureConsumers), Utils.IEnumerableToString(State.Consumers)));
                }
            }
        }
    }
}

#endif
