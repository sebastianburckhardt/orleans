#if !DISABLE_STREAMS

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Orleans.Streams
{
    internal class GrainBasedPubSubRuntime : IStreamPubSub
    {
        public Task<ISet<PubSubSubscriptionState>> RegisterProducer(StreamId streamId, IStreamProducerExtension streamProducer)
        {
            var streamRendezvous = GetRendezvousGrain(streamId);
            return streamRendezvous.RegisterProducer(streamId, streamProducer);
        }

        public Task UnregisterProducer(StreamId streamId, IStreamProducerExtension streamProducer)
        {
            var streamRendezvous = GetRendezvousGrain(streamId);
            return streamRendezvous.UnregisterProducer(streamId, streamProducer);
        }

        public Task RegisterConsumer(StreamId streamId, IStreamConsumerExtension streamConsumer, StreamSequenceToken token)
        {
            var streamRendezvous = GetRendezvousGrain(streamId);
            return streamRendezvous.RegisterConsumer(streamId, streamConsumer, token);
        }

        public Task UnregisterConsumer(StreamId streamId, IStreamConsumerExtension streamConsumer)
        {
            var streamRendezvous = GetRendezvousGrain(streamId);
            return streamRendezvous.UnregisterConsumer(streamId, streamConsumer);
        }

        public Task<int> ProducerCount(StreamId streamId)
        {
            var streamRendezvous = GetRendezvousGrain(streamId);
            return streamRendezvous.ProducerCount(streamId);
        }

        public Task<int> ConsumerCount(StreamId streamId)
        {
            var streamRendezvous = GetRendezvousGrain(streamId);
            return streamRendezvous.ConsumerCount(streamId);
        }

        private static IPubSubRendezvousGrain GetRendezvousGrain(StreamId streamId)
        {
            return (IPubSubRendezvousGrain)OrleansClient.InvokeStaticMethodThroughReflection(
                "Orleans",
                "Orleans.Streams.PubSubRendezvousGrainFactory",
                "GetGrain",
                new Type[] { typeof(Guid), typeof(string) },
                new object[] { streamId.AsGuid, null });
        }
    }
}

#endif //!DISABLE_STREAMS

