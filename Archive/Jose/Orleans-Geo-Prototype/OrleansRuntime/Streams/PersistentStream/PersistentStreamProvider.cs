#if !DISABLE_STREAMS
using System;
using System.Threading.Tasks;
using Orleans.Providers;

namespace Orleans.Streams
{
    /// <summary>
    /// Persistent stream provider that uses an adapter for persistence
    /// </summary>
    /// <typeparam name="TAdapterFactory"></typeparam>
    public class PersistentStreamProvider<TAdapterFactory> : IStreamProvider
        where TAdapterFactory : IQueueAdapterFactory, new()
    {
        public string                   Name { get; private set; }

        private OrleansLogger           _logger;
        private StreamDirectory         _streamDirectory;
        private IStreamProviderRuntime  _providerRuntime;
        private IQueueAdapter           _queueAdapter;

        public bool IsRewindable { get { return _queueAdapter.IsRewindable; } }

        public async Task Init(string name, IProviderRuntime providerUtilitiesManager, IProviderConfiguration config)
        {
            if (String.IsNullOrEmpty(name))
            {
                throw new ArgumentNullException("name");
            }
            if (providerUtilitiesManager == null)
            {
                throw new ArgumentNullException("providerUtilitiesManager");
            }
            if (config == null)
            {
                throw new ArgumentNullException("config");
            }

            this.Name = name;
            this._providerRuntime = (IStreamProviderRuntime)providerUtilitiesManager;
            this._queueAdapter = await (new TAdapterFactory()).Create(config);
            this._streamDirectory = new StreamDirectory();
            _logger = _providerRuntime.GetLogger(this.GetType().Name, Logger.LoggerType.Application);
            _logger.Info("Initialized PersistentStream with name {0} and with Adapter: {1}.", Name, _queueAdapter.Name);
            
            if (_providerRuntime.InSilo)
            {
                // Start PersistentPullingAgentGrain locally.
                PersistentStreamPullingAgent agent = new PersistentStreamPullingAgent(Constants.PullingAgentSystemTargetId, _providerRuntime); // PersistentStreamPullingAgentGrainFactory.GetGrain(Guid.NewGuid());
                _providerRuntime.RegisterSystemTarget(agent);
                // Init the agent only after it was registered locally.
                IPersistentStreamPullingAgent agentGrainRef = PersistentStreamPullingAgentFactory.Cast(agent.AsReference());
                // Need to call it as a grain reference though.
                await agentGrainRef.Init(_queueAdapter.AsImmutable());
            }
         }

        public IAsyncStream<T> GetStream<T>(StreamId streamId)
        {
            return _streamDirectory.GetOrAddStream<T>(
                streamId, 
                _providerRuntime.ExecutingEntityIdentity(),
                () => new StreamFactory<T>(() => GetProducerInterface<T>(streamId), () => GetConsumerInterface<T>(streamId), IsRewindable));
        }

        private IAsyncBatchObserver<T> GetProducerInterface<T>(StreamId streamId)
        {
            return new PersistentStreamProducer<T>(streamId, _providerRuntime, _queueAdapter, IsRewindable);
        }

        private IAsyncObservable<T> GetConsumerInterface<T>(StreamId streamId)
        {
            return new StreamConsumer<T>(streamId, _providerRuntime, _providerRuntime.PubSub(StreamPubSubType.GRAINBASED), IsRewindable);
        }
    }
}

#endif