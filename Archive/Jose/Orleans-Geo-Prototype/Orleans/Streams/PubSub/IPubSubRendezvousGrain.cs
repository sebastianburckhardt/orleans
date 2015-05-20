#if !DISABLE_STREAMS

using System.Threading.Tasks;
using System.Collections.Generic;
using System;

namespace Orleans.Streams
{
    internal interface IPubSubRendezvousGrain : IGrain // Compare with: IStreamPubSub
    {
        Task<ISet<PubSubSubscriptionState>> RegisterProducer(StreamId streamId, IStreamProducerExtension streamProducer);

        Task UnregisterProducer(StreamId streamId, IStreamProducerExtension streamProducer);

        Task RegisterConsumer(StreamId streamId, IStreamConsumerExtension streamConsumer, StreamSequenceToken token);

        Task UnregisterConsumer(StreamId streamId, IStreamConsumerExtension streamConsumer);

        Task<int> ProducerCount(StreamId streamId);

        Task<int> ConsumerCount(StreamId streamId);

        Task<PubSubSubscriptionState[]> DiagGetConsumers(StreamId streamId);

        Task Validate();
    }
    
    //------- STATE interfaces ----//

    public interface IPubSubGrainState : IGrainState
    {
        HashSet<PubSubPublisherState> Producers { get; set; }
        HashSet<PubSubSubscriptionState> Consumers { get; set; }
    }
}

#endif