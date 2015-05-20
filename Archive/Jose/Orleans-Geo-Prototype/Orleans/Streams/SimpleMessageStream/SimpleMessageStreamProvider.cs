#if !DISABLE_STREAMS
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.Streams;

namespace Orleans.Providers.Streams.SimpleMessageStream
{
    public class SimpleMessageStreamProvider : IStreamProvider
    {
        public string                               Name { get; private set; }

        private OrleansLogger                       _logger;
        private StreamDirectory                     _streamDirectory;
        private IStreamProviderRuntime              _providerRuntime;
        private bool                                _fireAndForgetDelivery;
        internal const string                       FIRE_AND_FORGET_DELIVERY = "FireAndForgetDelivery";
        internal const bool                         DEFAULT_FIRE_AND_FORGET_DELIVERY_VALUE = true;

        public SimpleMessageStreamProvider()
        {
        }

        public bool IsRewindable { get { return false; } }

        public Task Init(string name, IProviderRuntime providerUtilitiesManager, IProviderConfiguration config)
        {
            this.Name = name;
            _providerRuntime = (IStreamProviderRuntime) providerUtilitiesManager;
            string fireAndForgetDeliveryStr;
            if (!config.Properties.TryGetValue(FIRE_AND_FORGET_DELIVERY, out fireAndForgetDeliveryStr))
            {
                _fireAndForgetDelivery = DEFAULT_FIRE_AND_FORGET_DELIVERY_VALUE;
            }
            else
            {
                _fireAndForgetDelivery = Boolean.Parse(fireAndForgetDeliveryStr);
            }

            _logger = _providerRuntime.GetLogger(this.GetType().Name, Logger.LoggerType.Application);
            _logger.Info("Initialized SimpleMessageStreamProvider with name {0} and with property FireAndForgetDelivery: {1}.", Name, _fireAndForgetDelivery);
            _streamDirectory = new StreamDirectory();
            return TaskDone.Done;
        }

        public IAsyncStream<T> GetStream<T>(StreamId streamId)
        {
            if (streamId == null)
                throw new ArgumentNullException("streamId");

            return _streamDirectory.GetOrAddStream<T>(
                streamId, 
                _providerRuntime.ExecutingEntityIdentity(),
                () => new StreamFactory<T>(() => { return GetProducerInterface<T>(streamId); }, () => { return GetConsumerInterface<T>(streamId); }, IsRewindable));
        }

        private IAsyncBatchObserver<T> GetProducerInterface<T>(StreamId streamId)
        {
            return new SimpleMessageStreamProducer<T>(streamId, _providerRuntime, _fireAndForgetDelivery, IsRewindable);
        }

        private IAsyncObservable<T> GetConsumerInterface<T>(StreamId streamId)
        {
            return new StreamConsumer<T>(streamId, _providerRuntime, _providerRuntime.PubSub(StreamPubSubType.GRAINBASED), IsRewindable);
        }
    }
}

#endif