#if !DISABLE_STREAMS

using System;
using System.Threading.Tasks;
using Orleans;
using Orleans.Streams;
using UnitTestGrainInterfaces.Halo.Streaming;

namespace UnitTestGrains.Halo.Streaming
{
    class ProducerEventCountingGrain : BaseGrain, IProducerEventCountingGrain
    {
        private IAsyncObserver<int> _producer;
        private int _numProducedItems;
        private OrleansLogger _logger;

        public override Task ActivateAsync()
        {
            _logger = GetLogger("ProducerEventCountingGrain " + IdentityString);
            _logger.Info("Producer.ActivateAsync");
            _numProducedItems = 0;
            return base.ActivateAsync();
        }

        public override async Task DeactivateAsync()
        {
            _logger = GetLogger("ProducerEventCountingGrain " + IdentityString);
            _logger.Info("Producer.DeactivateAsync");
            _numProducedItems = 0;
            await base.DeactivateAsync();
        }

        public Task BecomeProducer(StreamId streamId, string providerToUse)
        {
            _logger.Info("Producer.BecomeProducer");
            if (streamId == null)
            {
                throw new ArgumentNullException("streamId");
            }
            if (String.IsNullOrEmpty(providerToUse))
            {
                throw new ArgumentNullException("providerToUse");
            }
            IStreamProvider streamProvider = GetStreamProvider(providerToUse);
            IAsyncStream<int> stream = streamProvider.GetStream<int>(streamId);
            _producer = stream.GetProducerInterface();
            return TaskDone.Done;
        }

        public Task<int> NumberProduced
        {
            get { return Task.FromResult(_numProducedItems); }
        }

        public async Task SendEvent()
        {
            _logger.Info("Producer.SendEvent called");
            if (_producer == null)
            {
                throw new ApplicationException("Not yet a producer on a stream.  Must call BecomeProducer first.");
            }
            
            await _producer.OnNextAsync(_numProducedItems + 1);

            // update after send in case of error
            _numProducedItems++;
            _logger.Info("Producer.SendEvent - TotalSent: ({0})", _numProducedItems);
        }
    }
}
#endif