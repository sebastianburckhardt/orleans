#if !DISABLE_STREAMS

using System;
using System.Threading.Tasks;
using Orleans;
using Orleans.Streams;

namespace UnitTests.SampleStreaming
{
    public interface ISampleStreaming_ProducerGrain : IGrain
    {
        Task BecomeProducer(StreamId streamId, string providerToUse);

        Task StartPeriodicProducing();

        Task StopPeriodicProducing();

        Task<int> NumberProduced { get; }
    }

    public interface ISampleStreaming_ConsumerGrain : IGrain
    {
        Task BecomeConsumer(StreamId streamId, string providerToUse);

        Task StopConsuming();

        Task<int> NumberConsumed { get; }
    }
}

#endif