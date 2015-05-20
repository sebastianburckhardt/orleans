#if !DISABLE_STREAMS

//#define USE_GENERICS

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;
using Orleans.Streams;

namespace UnitTestGrainInterfaces
{
#if USE_GENERICS
    public interface IStreamReliabilityTestGrain<in T> : IGrain
#else
    public interface IStreamReliabilityTestGrain : IGrain
#endif
    {
        Task<int> ReceivedCount { get; }
        Task<int> ErrorsCount { get; }
        Task<int> ConsumerCount { get; }

        Task Ping();
        Task<StreamSubscriptionHandle> AddConsumer(StreamId streamId, string providerName);
        Task RemoveConsumer(StreamId streamId, string providerName, StreamSubscriptionHandle consumerHandle);
        Task BecomeProducer(StreamId streamId, string providerName);
        Task RemoveProducer(StreamId streamId, string providerName);
        Task ClearGrain();

        Task<bool> IsConsumer();
        Task<bool> IsProducer();
        Task<int> GetConsumerHandlesCount();
        Task<int> GetConsumerObserversCount();

#if USE_GENERICS
        Task SendItem(T item);
#else
        Task SendItem(int item);
#endif

        Task<SiloAddress> GetLocation();
    }
}
#endif