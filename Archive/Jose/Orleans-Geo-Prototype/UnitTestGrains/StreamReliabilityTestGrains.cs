#if !DISABLE_STREAMS

//#define USE_GENERICS

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using Orleans.Streams;
using UnitTestGrainInterfaces;

namespace UnitTestGrains
{
    public interface IStreamReliabilityTestGrainState : IGrainState
    {
        // For producer and consumer 
        // -- only need to store because of how we run our unit tests against multiple providers
        string StreamProviderName { get; set; }
        
        // For producer only.
        StreamId StreamId { get; set; }
        bool IsProducer { get; set; }

        // For consumer only.
        HashSet<StreamSubscriptionHandle> ConsumerSubscriptionHandles { get; set; }
    }

    [StorageProvider(ProviderName = "AzureStore")]
#if USE_GENERICS
    public class StreamReliabilityTestGrain<T> : GrainBase<IStreamReliabilityTestGrainState>, IStreamReliabilityTestGrain<T>
#else
    public class StreamReliabilityTestGrain : GrainBase<IStreamReliabilityTestGrainState>, IStreamReliabilityTestGrain
#endif
    {
        [NonSerialized]
        private OrleansLogger logger;

#if USE_GENERICS
        private IAsyncStream<T> Stream { get; set; }
        private IAsyncObserver<T> Producer { get; set; }
        private Dictionary<StreamSubscriptionHandle, MyStreamObserver<T>> Observers { get; set; }
#else
        private IAsyncStream<int> Stream { get; set; }
        private IAsyncObserver<int> Producer { get; set; }
        private Dictionary<StreamSubscriptionHandle, MyStreamObserver<int>> Observers { get; set; }
#endif

        public override async Task ActivateAsync()
        {
            logger = GetLogger("StreamReliabilityTestGrain-" + this.IdentityString);
            logger.Info(String.Format("ActivateAsync IsProducer = {0}, IsConsumer = {1}.", State.IsProducer, State.ConsumerSubscriptionHandles.Count > 0));

            if (Observers == null)   
#if USE_GENERICS
                Observers = new Dictionary<StreamSubscriptionHandle, MyStreamObserver<T>>();
#else
                Observers = new Dictionary<StreamSubscriptionHandle, MyStreamObserver<int>>();
#endif

            if (State.StreamId != null && State.StreamProviderName != null)
            {
                TryInitStream(State.StreamId, State.StreamProviderName);

                if (State.ConsumerSubscriptionHandles.Count > 0)
                {
                    var handles = State.ConsumerSubscriptionHandles.ToArray();
                    State.ConsumerSubscriptionHandles.Clear();
                    await ReconnectConsumerHandles(handles);
                }
                if (State.IsProducer)
                {
                    await BecomeProducer(State.StreamId, State.StreamProviderName);
                }
            }
            else
            {
                logger.Info("No stream yet.");
            }
        }

        public override Task DeactivateAsync()
        {
            logger.Info("DeactivateAsync");
            return base.DeactivateAsync();
        }

        public Task<int> ConsumerCount
        {
            get
            {
                int numConsumers = State.ConsumerSubscriptionHandles.Count;
                logger.Info("ConsumerCount={0}", numConsumers);
                return Task.FromResult(numConsumers);
            }
        }
        public Task<int> ReceivedCount
        {
            get
            {
                int numReceived = Observers.Sum(o => o.Value.NumItems);
                logger.Info("ReceivedCount={0}", numReceived);
                return Task.FromResult(numReceived);
            }
        }
        public Task<int> ErrorsCount
        {
            get
            {
                int numErrors = Observers.Sum(o => o.Value.NumErrors);
                logger.Info("ErrorsCount={0}", numErrors);
                return Task.FromResult(numErrors);
            }
        }

        public Task Ping()
        {
            logger.Info("Ping");
            return TaskDone.Done;
        }

        public async Task<StreamSubscriptionHandle> AddConsumer(StreamId streamId, string providerName)
        {
            logger.Info("AddConsumer StreamId={0} StreamProvider={1} Grain={2}", streamId, providerName, this.AsReference());
            TryInitStream(streamId, providerName);
#if USE_GENERICS
            var observer = new MyStreamObserver<T>();
#else
            var observer = new MyStreamObserver<int>(logger);
#endif
            var consumer = Stream.GetConsumerInterface();
            StreamSubscriptionHandle subsHandle = await consumer.SubscribeAsync(observer);
            Observers.Add(subsHandle, observer);
            State.ConsumerSubscriptionHandles.Add(subsHandle);
            await State.WriteStateAsync();
            return subsHandle;
        }

        public async Task ReconnectConsumerHandles(StreamSubscriptionHandle[] subscriptionHandles)
        {
            logger.Info("ReconnectConsumerHandles SubscriptionHandles={0} Grain={1}", Utils.IEnumerableToString(subscriptionHandles), this.AsReference());

            foreach (StreamSubscriptionHandle subHandle in subscriptionHandles)
            {
#if USE_GENERICS
                var stream = GetStreamProvider(State.StreamProviderName).GetStream<T>(subHandle.StreamId);
                var observer = new MyStreamObserver<T>();
#else
                var stream = GetStreamProvider(State.StreamProviderName).GetStream<int>(subHandle.StreamId);
                var observer = new MyStreamObserver<int>(logger);
#endif
                var consumer = stream.GetConsumerInterface();
                StreamSubscriptionHandle subsHandle = await consumer.SubscribeAsync(observer);
                Observers.Add(subsHandle, observer);
                State.ConsumerSubscriptionHandles.Add(subsHandle);
            }
            await State.WriteStateAsync();
        }

        public async Task RemoveConsumer(StreamId streamId, string providerName, StreamSubscriptionHandle subsHandle)
        {
            logger.Info("RemoveConsumer StreamId={0} StreamProvider={1}", streamId, providerName);
            if (State.ConsumerSubscriptionHandles.Count == 0) throw new InvalidOperationException("Not a Consumer");
            var consumer = Stream.GetConsumerInterface();
            await consumer.UnsubscribeAsync(subsHandle);
            Observers.Remove(subsHandle);
            State.ConsumerSubscriptionHandles.Remove(subsHandle);
            await State.WriteStateAsync();
        }

        public async Task BecomeProducer(StreamId streamId, string providerName)
        {
            logger.Info("BecomeProducer StreamId={0} StreamProvider={1}", streamId, providerName);
            TryInitStream(streamId, providerName);
            Producer = Stream.GetProducerInterface();
            State.IsProducer = true;
            await State.WriteStateAsync();
        }

        public async Task RemoveProducer(StreamId streamId, string providerName)
        {
            logger.Info("RemoveProducer StreamId={0} StreamProvider={1}", streamId, providerName);
            if (!State.IsProducer) throw new InvalidOperationException("Not a Producer");
            // TODO: Unregister producer from PubSub tables
            // TODO: await Producer.OnCompletedAsync();
            // TODO: await Stream.CloseAsync();
            Producer = null;
            State.IsProducer = false;
            await State.WriteStateAsync();
        }

        public async Task ClearGrain()
        {
            logger.Info("ClearGrain.");
            //await Stream.CloseAsync();
            State.ConsumerSubscriptionHandles.Clear();
            State.IsProducer = false;
            Observers.Clear();
            Stream = null;
            await State.ClearStateAsync();
        }

        public Task<bool> IsConsumer()
        {
            bool isConsumer = State.ConsumerSubscriptionHandles.Count > 0;
            logger.Info("IsConsumer={0}", isConsumer);
            return Task.FromResult(isConsumer);
        }
        public Task<bool> IsProducer()
        {
            bool isProducer = State.IsProducer;
            logger.Info("IsProducer={0}", isProducer);
            return Task.FromResult(isProducer);
        }
        public Task<int> GetConsumerHandlesCount()
        {
            return Task.FromResult(State.ConsumerSubscriptionHandles.Count);
        }
        public async Task<int> GetConsumerObserversCount()
        {
#if USE_GENERICS
            var consumer = (StreamConsumer<T>)Stream.GetConsumerInterface();
#else
            var consumer = (StreamConsumer<int>)Stream.GetConsumerInterface();
#endif
            return await consumer.DiagGetConsumerObserversCount();
        }

#if USE_GENERICS
        public async Task SendItem(T item)
#else
        public async Task SendItem(int item)
#endif
        {
            logger.Info("SendItem Item={0}", item);
            await Producer.OnNextAsync(item);
        }

        public Task<SiloAddress> GetLocation()
        {
            SiloAddress siloAddress = _Data.Silo;
            logger.Info("GetLocation SiloAddress={0}", siloAddress);
            return Task.FromResult(siloAddress);
        }

        private void TryInitStream(StreamId streamId, string providerName)
        {
            Assert.IsNotNull(streamId, "Can't have null stream id");
            Assert.IsNotNull(providerName, "Can't have null stream provider name");

            State.StreamId = streamId;
            State.StreamProviderName = providerName;

            if (Stream == null)
            {
                logger.Info("InitStream StreamId={0} StreamProvider={1}", streamId, providerName);

                IStreamProvider streamProvider = GetStreamProvider(providerName);
#if USE_GENERICS
                Stream = streamProvider.GetStream<T>(streamId);
#else
                Stream = streamProvider.GetStream<int>(streamId);
#endif
            }
        }
    }

    [Serializable]
    public class MyStreamObserver<T> : IAsyncObserver<T>
    {
        internal int NumItems { get; private set; }
        internal int NumErrors { get; private set; }

        private OrleansLogger logger;

        internal MyStreamObserver(OrleansLogger logger)
        {
            this.logger = logger;
        }

        public Task OnNextAsync(T item, StreamSequenceToken token)
        {
            NumItems++;
            logger.Info("Received OnNextAsync - Item={0} - Total Items={1} Errors={2}", item, NumItems, NumErrors);
            return TaskDone.Done;
        }

        public Task OnCompletedAsync()
        {
            logger.Info("Receive OnCompletedAsync - Total Items={0} Errors={1}", NumItems, NumErrors);
            return TaskDone.Done;
        }

        public Task OnErrorAsync(Exception ex)
        {
            NumErrors++;
            logger.Info("Received OnErrorAsync - Exception={0} - Total Items={1} Errors={2}", ex, NumItems, NumErrors);
            return TaskDone.Done;
        }
    }
}
#endif