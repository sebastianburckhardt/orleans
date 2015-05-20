#if !DISABLE_STREAMS

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;
using Orleans.Streams;

namespace UnitTestGrains
{
    //------- GRAIN interfaces ----//
    public interface IStreaming_ProducerGrain : IGrain
    {
        Task BecomeProducer(StreamId streamId, string providerToUse);
        Task StopBeingProducer();
        Task ProduceSequentialSeries(int count);
        Task ProduceParallelSeries(int count);
        Task ProducePeriodicSeries(int count);
        Task<int> ExpectedItemsProduced { get; }
        Task<int> ItemsProduced { get; }
        Task AddNewConsumerGrain(Guid consumerGrainId);
        Task<int> ProducerCount { get; }
        Task DeactivateProducerOnIdle();

        [AlwaysInterleave]
        Task VerifyFinished();
    }

    public interface IStreaming_ConsumerGrain : IGrain
    {
        Task BecomeConsumer(StreamId streamId, string providerToUse);
        Task StopBeingConsumer();
        Task<int> ItemsConsumed { get; }        
        Task<int> ConsumerCount { get; }
        Task DeactivateConsumerOnIdle();
    }

    public interface IPersistentStreaming_ProducerGrain : IStreaming_ProducerGrain
    {
    }

    public interface IPersistentStreaming_ConsumerGrain : IStreaming_ConsumerGrain
    {
    }

    public interface IStreaming_ProducerConsumerGrain : IGrain, IStreaming_ProducerGrain, IStreaming_ConsumerGrain
    {
    }

    public interface IStreaming_Reentrant_ProducerConsumerGrain : IGrain, IStreaming_ProducerGrain, IStreaming_ConsumerGrain
    {
    }

    //------- STATE interfaces ----//

    public interface IStreaming_ProducerGrain_State : IGrainState
    {
        List<IProducerObserver> Producers { get; set; }
    }

    public interface IStreaming_ConsumerGrain_State : IGrainState
    {
        List<IConsumerObserver> Consumers { get; set; }
    }

    public interface IStreaming_ProducerConsumerGrain_State : IStreaming_ProducerGrain_State, IStreaming_ConsumerGrain_State
    {

    }

    //------- POCO interfaces for objects that implement the actual test logic ----///

    public interface IProducerObserver
    {
        void BecomeProducer(StreamId streamId, IStreamProvider streamProvider);
        void RenewProducer(OrleansLogger logger, IStreamProvider streamProvider);
        Task StopBeingProducer();
        Task ProduceSequentialSeries(int count);
        Task ProduceParallelSeries(int count);
        Task ProducePeriodicSeries(Func<Func<object, Task>, IDisposable> createTimerFunc, int count);
        Task<int> ExpectedItemsProduced { get; }
        Task<int> ItemsProduced { get; }
        Task AddNewConsumerGrain(Guid consumerGrainId);
        Task<int> ProducerCount { get; }
        Task VerifyFinished();
        string ProviderName { get; }
    }

    public interface IConsumerObserver
    {
        Task BecomeConsumer(StreamId streamId, IStreamProvider streamProvider);
        Task RenewConsumer(OrleansLogger logger, IStreamProvider streamProvider);
        Task StopBeingConsumer(IStreamProvider streamProvider);
        Task<int> ItemsConsumed { get; }
        Task<int> ConsumerCount { get; }
        string ProviderName { get; }
    }

}

#endif