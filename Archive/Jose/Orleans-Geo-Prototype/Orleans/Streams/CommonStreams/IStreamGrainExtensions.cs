#if !DISABLE_STREAMS

using System;
using System.Threading.Tasks;
using Orleans;

namespace Orleans.Streams
{
    // This is the extension interface for stream consumers
    [Factory(FactoryAttribute.FactoryTypes.ClientObject)]
    public interface IStreamConsumerExtension : IGrain, IGrainExtension
    {
        Task DeliverItem(StreamId streamId, object item, StreamSequenceToken token);

        // A null exception means that the stream was closed normally
        Task Complete(StreamId streamId, Exception ex);
    }

    // This is the extension interface for stream producers
    [Factory(FactoryAttribute.FactoryTypes.ClientObject)]
    public interface IStreamProducerExtension : IGrain, IGrainExtension
    {
        [AlwaysInterleave]
        Task AddSubscriber(StreamId streamId, IStreamConsumerExtension streamConsumer, StreamSequenceToken token);

        [AlwaysInterleave]
        Task RemoveSubscriber(StreamId streamId, IStreamConsumerExtension streamConsumer);
    }
}

#endif