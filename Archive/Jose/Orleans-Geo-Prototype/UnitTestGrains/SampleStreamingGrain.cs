#if !DISABLE_STREAMS

using System;
using System.Threading.Tasks;
using Orleans;

using Orleans.Streams;

namespace UnitTests.SampleStreaming
{
    internal class SampleConsumerObserver<T> : IAsyncObserver<T>
    {
        private SampleStreaming_ConsumerGrain hostingGrain;

        internal SampleConsumerObserver(SampleStreaming_ConsumerGrain _hostingGrain)
        {
            hostingGrain = _hostingGrain;
        }

        public Task OnNextAsync(T item, StreamSequenceToken token = null)
        {
            hostingGrain.logger.Info("OnNextAsync({0}{1})", item, token != null ? token.ToString() : "null");
            hostingGrain.numConsumedItems++;
            return TaskDone.Done;
        }

        public Task OnCompletedAsync()
        {
            hostingGrain.logger.Info("OnCompletedAsync()");
            return TaskDone.Done;
        }

        public Task OnErrorAsync(Exception ex)
        {
            hostingGrain.logger.Info("OnErrorAsync({0})", ex);
            return TaskDone.Done;
        }
    }

    public class SampleStreaming_ProducerGrain : GrainBase, ISampleStreaming_ProducerGrain
    {
        private IAsyncObserver<int> producer;
        private int numProducedItems;
        private IOrleansTimer producerTimer;
        internal OrleansLogger logger;

        public override Task ActivateAsync()
        {
            logger = base.GetLogger("SampleStreaming_ProducerGrain " + base.IdentityString);
            logger.Info("ActivateAsync");
            numProducedItems = 0;
            return TaskDone.Done;
        }

        public Task BecomeProducer(StreamId streamId, string providerToUse)
        {
            logger.Info("BecomeProducer");
            IStreamProvider streamProvider = base.GetStreamProvider(providerToUse);
            IAsyncStream<int> stream = streamProvider.GetStream<int>(streamId);
            producer = stream.GetProducerInterface();
            return TaskDone.Done;
        }

        public Task StartPeriodicProducing()
        {
            logger.Info("StartProducing");
            producerTimer = base.RegisterTimer(TimerCallback, null, TimeSpan.Zero, TimeSpan.FromMilliseconds(10));
            return TaskDone.Done;
        }

        public Task StopPeriodicProducing()
        {
            logger.Info("StopProducing");
            producerTimer.Dispose();
            producerTimer = null;
            return TaskDone.Done;
        }

        public Task<int> NumberProduced
        {
            get { return Task.FromResult(numProducedItems); }
        }

        private Task TimerCallback(object state)
        {
            if (producerTimer != null)
            {
                numProducedItems++;
                logger.Info("TimerCallback ({0})", numProducedItems);
                return producer.OnNextAsync(numProducedItems);
            }
            return TaskDone.Done;
        }
    }

    public class SampleStreaming_ConsumerGrain : GrainBase, ISampleStreaming_ConsumerGrain
    {
        private IAsyncObservable<int> consumer;
        internal int numConsumedItems;
        internal OrleansLogger logger;
        private IAsyncObserver<int> consumerObserver;
        private StreamSubscriptionHandle consumerInterface;

        public override Task ActivateAsync()
        {
            logger = base.GetLogger("SampleStreaming_ConsumerGrain " + base.IdentityString);
            logger.Info("ActivateAsync");
            numConsumedItems = 0;
            consumerInterface = null;
            return TaskDone.Done;
        }

        public async Task BecomeConsumer(StreamId streamId, string providerToUse)
        {
            logger.Info("BecomeConsumer");
            consumerObserver = new SampleConsumerObserver<int>(this);
            IStreamProvider streamProvider = base.GetStreamProvider(providerToUse);
            IAsyncStream<int>  stream = streamProvider.GetStream<int>(streamId);
            consumer = stream.GetConsumerInterface();
            consumerInterface = await consumer.SubscribeAsync(consumerObserver);
        }

        public async Task StopConsuming()
        {
            logger.Info("StopConsuming");
            if (consumerInterface != null)
            {
                await consumer.UnsubscribeAsync(consumerInterface);
                //consumerInterface.Dispose();
                consumerInterface = null;
            }
        }

        public Task<int> NumberConsumed
        {
            get { return Task.FromResult(numConsumedItems); }
        }
    }
}

#endif