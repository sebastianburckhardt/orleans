#if !DISABLE_STREAMS 

using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;
using Orleans.Streams;
using Orleans.Providers.Streams.SimpleMessageStream;

namespace UnitTestGrains
{
    [Serializable]
    public class StreamItem
    {
        public string       Data;
        public StreamId     StreamId;

        public StreamItem(string data, StreamId streamId)
        {
            Data = data;
            StreamId = streamId;
        }

        public override string ToString()
        {
            return String.Format("{0}", Data);
        }
    }

    [Serializable]
    public class ConsumerObserver : IAsyncObserver<StreamItem>, IConsumerObserver
    {
        [NonSerialized]
        private OrleansLogger _logger;
        [NonSerialized]
        private StreamSubscriptionHandle _subscription;
        private int _itemsConsumed;
        private StreamId _streamId;
        private string _providerName;

        public Task<int> ItemsConsumed
        {
            get { return Task.FromResult(_itemsConsumed); }
        }

        private ConsumerObserver(OrleansLogger logger)
        {
            _logger = logger;
            _itemsConsumed = 0;
        }

        public static ConsumerObserver NewObserver(OrleansLogger logger)
        {
            if (null == logger)
                throw new ArgumentNullException("logger");
            return new ConsumerObserver(logger);
        }

        public Task OnNextAsync(StreamItem item, StreamSequenceToken token = null)
        {
            if (!item.StreamId.Equals(_streamId))
            {
                string excStr = String.Format("ConsumerObserver.OnNextAsync: received an item from the wrong stream." + 
                        " Got item {0} from stream = {1}, expecting stream = {2}, numConsumed={3}", 
                        item, item.StreamId, _streamId, _itemsConsumed);
                _logger.Error(0, excStr);
                throw new ArgumentException(excStr);
            }
            ++_itemsConsumed;

            string str = String.Format("ConsumerObserver.OnNextAsync: streamId={0}, item={1}, numConsumed={2}{3}", 
                _streamId, item.Data, _itemsConsumed, token != null ? ", token = " + token.ToString() : "");
            if (ProducerObserver.DEBUG_STREAMING_GRAINS)
            {
                _logger.Info(str);
            }
            else
            {
                _logger.Verbose(str);
            }
            return TaskDone.Done;
        }

        public Task OnCompletedAsync()
        {            
            _logger.Info("ConsumerObserver.OnCompletedAsync");
            return TaskDone.Done;
        }

        public Task OnErrorAsync(Exception ex)
        {            
            _logger.Info("ConsumerObserver.OnErrorAsync: ex={0}", ex);
            return TaskDone.Done;
        }

        public async Task BecomeConsumer(StreamId streamId, IStreamProvider streamProvider)
        {
            _logger.Info("BecomeConsumer");
            if (_providerName != null)
                throw new InvalidOperationException("redundant call to BecomeConsumer");
            _streamId = streamId;
            _providerName = streamProvider.Name;
            IAsyncStream<StreamItem> stream = streamProvider.GetStream<StreamItem>(streamId);
            IAsyncObservable<StreamItem> observable = stream.GetConsumerInterface();
            _subscription = await observable.SubscribeAsync(this);    
        }

        public async Task RenewConsumer(OrleansLogger logger, IStreamProvider streamProvider)
        {
            _logger = logger;
            _logger.Info("RenewConsumer");
            IAsyncStream<StreamItem> stream = streamProvider.GetStream<StreamItem>(_streamId);
            IAsyncObservable<StreamItem> observable = stream.GetConsumerInterface();
            _subscription = await observable.SubscribeAsync(this);
        }

        public async Task StopBeingConsumer(IStreamProvider streamProvider)
        {
            _logger.Info("StopBeingConsumer");
            if (_subscription != null)
            {
                IAsyncStream<StreamItem> stream = streamProvider.GetStream<StreamItem>(_streamId);
                IAsyncObservable<StreamItem> observable = stream.GetConsumerInterface();
                await observable.UnsubscribeAsync(_subscription);
                //_subscription.Dispose();
                _subscription = null;
            }
        }

        public Task<int> ConsumerCount
        {
            get { return Task.FromResult(_subscription == null ? 0 : 1); }
        }

        public string ProviderName { get { return _providerName; } }
    }

    [Serializable]
    public class ProducerObserver : IProducerObserver
    {
        [NonSerialized]
        private OrleansLogger _logger;
        [NonSerialized]
        private IAsyncBatchObserver<StreamItem> _observer;
        [NonSerialized]
        private Dictionary<IDisposable, TimerState> _timers;

        private int _itemsProduced;
        private int _expectedItemsProduced;
        private StreamId _streamId;
        private string _providerName;
        private InterlockedFlag _cleanedUpFlag;
        [NonSerialized]
        private bool _observerDisposedYet;

        public static bool DEBUG_STREAMING_GRAINS = true;

        private ProducerObserver(OrleansLogger logger)
        {
            _logger = logger;
            _observer = null;
            _timers = new Dictionary<IDisposable, TimerState>();

            _itemsProduced = 0;
            _expectedItemsProduced = 0;
            _streamId = null;
            _providerName = null;
            _cleanedUpFlag = new InterlockedFlag();
            _observerDisposedYet = false;
        }

        public static ProducerObserver NewObserver(OrleansLogger logger)
        {
            if (null == logger)
                throw new ArgumentNullException("logger");
            return new ProducerObserver(logger);
        }

        public void BecomeProducer(StreamId streamId, IStreamProvider streamProvider)
        {
            _cleanedUpFlag.ThrowNotInitializedIfSet();

            _logger.Info("BecomeProducer");
            IAsyncStream<StreamItem> stream = streamProvider.GetStream<StreamItem>(streamId);
            _observer = stream.GetProducerInterface();
            var observerAsSMSProducer = _observer as SimpleMessageStreamProducer<StreamItem>;
            // only SimpleMessageStreamProducer implements IDisposable and a means to verify it was cleaned up.
            if (null == observerAsSMSProducer)
            {
                _logger.Info("ProducerObserver.BecomeProducer: producer requires no disposal; test short-circuted.");
                _observerDisposedYet = true;
            }
            else
            {
                _logger.Info("ProducerObserver.BecomeProducer: producer performs disposal during finalization.");
                observerAsSMSProducer.OnDisposeTestHook += 
                    () => 
                        _observerDisposedYet = true;
            }
            _streamId = streamId;
            _providerName = streamProvider.Name;
        }

        public void RenewProducer(OrleansLogger logger, IStreamProvider streamProvider)
        {
            _cleanedUpFlag.ThrowNotInitializedIfSet();

            _logger = logger;
            _logger.Info("RenewProducer");
            IAsyncStream<StreamItem> stream = streamProvider.GetStream<StreamItem>(_streamId);
            _observer = stream.GetProducerInterface();
            var observerAsSMSProducer = _observer as SimpleMessageStreamProducer<StreamItem>;
            // only SimpleMessageStreamProducer implements IDisposable and a means to verify it was cleaned up.
            if (null == observerAsSMSProducer)
            {
                //_logger.Info("ProducerObserver.BecomeProducer: producer requires no disposal; test short-circuted.");
                _observerDisposedYet = true;
            }
            else
            {
                //_logger.Info("ProducerObserver.BecomeProducer: producer performs disposal during finalization.");
                observerAsSMSProducer.OnDisposeTestHook +=
                    () =>
                        _observerDisposedYet = true;
            }
        }

        private async Task<bool> ProduceItem(string data)
        {
            if (_cleanedUpFlag.IsSet)
                return false;

            StreamItem item = new StreamItem(data, _streamId);
            await _observer.OnNextAsync(item);
            _itemsProduced++;
            string str = String.Format("ProducerObserver.ProduceItem: streamId={0}, data={1}, numProduced so far={2}.", _streamId, data, _itemsProduced);
            if (DEBUG_STREAMING_GRAINS)
            {
                _logger.Info(str);
            }
            else
            {
                _logger.Verbose(str);
            }
            return true;
        }

        public async Task ProduceSequentialSeries(int count)
        {
            _cleanedUpFlag.ThrowNotInitializedIfSet();

            if (0 >= count)
                throw new ArgumentOutOfRangeException("count", "The count must be greater than zero.");
            _expectedItemsProduced += count;
            _logger.Info("ProducerObserver.ProduceSequentialSeries: streamId={0}, num items to produce={1}.", _streamId, count);
            for (var i = 1; i <= count; ++i)
                await ProduceItem(String.Format("sequential#{0}", i));
        }

        public Task ProduceParallelSeries(int count)
        {
            _cleanedUpFlag.ThrowNotInitializedIfSet();

            if (0 >= count)
                throw new ArgumentOutOfRangeException("count", "The count must be greater than zero.");
            _logger.Info("ProducerObserver.ProduceParallelSeries: streamId={0}, num items to produce={1}.", _streamId, count);
            _expectedItemsProduced += count;
            var tasks = new Task<bool>[count];
            for (var i = 1; i <= count; ++i)
            {
                int capture = i;
                Func<Task<bool>> func = async () => 
                    { 
                        return await ProduceItem(String.Format("parallel#{0}", capture)); 
                    };
                // Need to call on different threads to force parallel execution.
                tasks[capture - 1] = Task.Factory.StartNew(func).Unwrap();
            }
            return Task.WhenAll(tasks);
        }

        public Task<int> ItemsProduced
        {
            get
            {
                _cleanedUpFlag.ThrowNotInitializedIfSet();
                return Task.FromResult(_itemsProduced);
            }
        }

        public Task ProducePeriodicSeries(Func<Func<object, Task>, IDisposable> createTimerFunc, int count)
        {
            _cleanedUpFlag.ThrowNotInitializedIfSet();

            _logger.Info("ProducerObserver.ProducePeriodicSeries: streamId={0}, num items to produce={1}.", _streamId, count);
            var timer = TimerState.NewTimer(createTimerFunc, ProduceItem, RemoveTimer, count);
            // we can't pass the TimerState object in as the argument-- it might be prematurely collected, so we root
            // it to this object via the _timers dictionary.
            _timers.Add(timer.Handle, timer);
            _expectedItemsProduced += count;
            timer.StartTimer();
            return TaskDone.Done;        
        }

        private void RemoveTimer(IDisposable handle)
        {
            _logger.Info("ProducerObserver.RemoveTimer: streamId={0}.", _streamId);
            if (handle == null)
                throw new ArgumentNullException("handle");
            if (!_timers.Remove(handle))
                throw new InvalidOperationException("handle not found");
        }

        public Task<StreamId> StreamId
        {
            get
            {
                _cleanedUpFlag.ThrowNotInitializedIfSet();
                return Task.FromResult(_streamId);
            }
        }

        public string ProviderName
        {
            get
            {
                _cleanedUpFlag.ThrowNotInitializedIfSet();
                return _providerName;
            }
        }

        public Task AddNewConsumerGrain(Guid consumerGrainId)
        {
            var grain = Streaming_ConsumerGrainFactory.GetGrain(consumerGrainId, "UnitTestGrains.Streaming_ConsumerGrain");
            return grain.BecomeConsumer(_streamId, _providerName);
        }

        public Task<int> ExpectedItemsProduced
        {
            get
            {
                _cleanedUpFlag.ThrowNotInitializedIfSet();
                return Task.FromResult(_expectedItemsProduced);
            }
        }

        public Task<int> ProducerCount
        {
            get { return Task.FromResult(_cleanedUpFlag.IsSet ? 0 : 1); }
        }

        public Task StopBeingProducer()
        {
            _logger.Info("StopBeingProducer");
            if (!_cleanedUpFlag.TrySet())
                return TaskDone.Done;

            if (_timers != null)
            {
                foreach (var i in _timers)
                {
                    try
                    {
                        i.Value.Dispose();
                    }
                    catch (Exception exc)
                    {
                        _logger.Error(1, "StopBeingProducer: Timer Dispose() has thrown", exc);
                    }
                }
                _timers = null;
            }
            _observer = null; // Disposing
            return TaskDone.Done;
        }

        public async Task VerifyFinished()
        {
            _logger.Info("ProducerObserver.VerifyFinished: waiting for observer disposal; streamId={0}", _streamId);
            while (!_observerDisposedYet)
            {
                await Task.Delay(1000);
                GC.Collect();
                GC.WaitForPendingFinalizers(); 
            }
            _logger.Info("ProducerObserver.VerifyFinished: observer disposed; streamId={0}", _streamId);
        }

        private class TimerState : IDisposable
        {
            private bool _started = false;
            public IDisposable Handle { get; private set; }
            private int _counter;
            private Func<string, Task<bool>> _produceItemFunc;
            private Action<IDisposable> _onDisposeFunc;
            private readonly InterlockedFlag _disposedFlag;

            private TimerState(Func<string, Task<bool>> produceItemFunc, Action<IDisposable> onDisposeFunc, int count)
            {
                _produceItemFunc = produceItemFunc;
                _onDisposeFunc = onDisposeFunc;
                _counter = count;
                _disposedFlag = new InterlockedFlag();
            }

            public static TimerState NewTimer(Func<Func<object, Task>, IDisposable> startTimerFunc, Func<string, Task<bool>> produceItemFunc, Action<IDisposable> onDisposeFunc, int count)
            {
                if (null == startTimerFunc)
                    throw new ArgumentNullException("startTimerFunc");
                if (null == produceItemFunc)
                    throw new ArgumentNullException("produceItemFunc");
                if (null == onDisposeFunc)
                    throw new ArgumentNullException("onDisposeFunc");
                if (0 >= count)
                    throw new ArgumentOutOfRangeException("count", count, "argument must be > 0");
                var newOb = new TimerState(produceItemFunc, onDisposeFunc, count);
                newOb.Handle = startTimerFunc(newOb.OnTickAsync);
                if (null == newOb.Handle)
                    throw new InvalidOperationException("startTimerFunc must not return null");
                return newOb;
            }

            public void StartTimer()
            {
                _disposedFlag.ThrowDisposedIfSet(GetType());

                if (_started)
                    throw new InvalidOperationException("timer already started");
                _started = true;
            }

            private async Task OnTickAsync(object unused)
            {
                if (_started && !_disposedFlag.IsSet)
                {
                    --_counter;
                    bool shouldContinue = await _produceItemFunc(String.Format("periodic#{0}", _counter));
                    if (!shouldContinue || 0 == _counter)
                        Dispose();
                }
            }

            public void Dispose()
            {
                Dispose(true);
                GC.SuppressFinalize(this);
            }

            protected virtual void Dispose(bool disposing)
            {
                if (!_disposedFlag.TrySet())
                    return;
                _onDisposeFunc(Handle);
                Handle.Dispose();
                Handle = null;
            }
        }
    }

    public class Streaming_ProducerGrain : GrainBase<IStreaming_ProducerGrain_State>, IStreaming_ProducerGrain
    {
        private OrleansLogger _logger;
        protected List<IProducerObserver> _producers;
        private InterlockedFlag _cleanedUpFlag;

        public override Task ActivateAsync()
        {
            _logger = base.GetLogger("Test.Streaming_ProducerGrain " + base.RuntimeIdentity + "/" + base.IdentityString + "/" + base._Data.ActivationId);
            _logger.Info("ActivateAsync");
             _producers = new List<IProducerObserver>();
            _cleanedUpFlag = new InterlockedFlag();
            return TaskDone.Done;
        }

        public override Task DeactivateAsync()
        {
            _logger.Info("DeactivateAsync");
            return TaskDone.Done;
        }

        public virtual Task BecomeProducer(StreamId streamId, string providerToUse)
        {
            _cleanedUpFlag.ThrowNotInitializedIfSet();

            ProducerObserver producer = ProducerObserver.NewObserver(_logger);
            producer.BecomeProducer(streamId, base.GetStreamProvider(providerToUse));
            _producers.Add(producer);
            return TaskDone.Done;
        }

        public virtual async Task ProduceSequentialSeries(int count)
        {
            _cleanedUpFlag.ThrowNotInitializedIfSet();

            foreach (var producer in _producers)
            {
                await producer.ProduceSequentialSeries(count);
            } 
        }

        public virtual async Task ProduceParallelSeries(int count)
        {
            _cleanedUpFlag.ThrowNotInitializedIfSet();

            await Task.WhenAll(_producers.Select(p => p.ProduceParallelSeries(count)).ToArray());
        }

        public virtual Task ProducePeriodicSeries(int count)
        {
            _cleanedUpFlag.ThrowNotInitializedIfSet();

            return Task.WhenAll(_producers.Select(p => p.ProducePeriodicSeries((timerCallback) =>
                {
                    return base.RegisterTimer(timerCallback, null, TimeSpan.Zero, TimeSpan.FromMilliseconds(10));
                },count)).ToArray());
        }

        private async Task<int> GetExpectedItemsProduced()
        {
            var tasks = _producers.Select(p => p.ExpectedItemsProduced).ToArray();
            await Task.WhenAll(tasks);
            return tasks.Sum(t => t.Result);
        }

        private async Task<int> GetItemsProduced()
        {
            var tasks = _producers.Select(p => p.ItemsProduced).ToArray();
            await Task.WhenAll(tasks);
            return tasks.Sum(t => t.Result);
        }

        public virtual Task<int> ItemsProduced
        {
            get
            {
                _cleanedUpFlag.ThrowNotInitializedIfSet();
                return GetItemsProduced();
            }
        }

        public virtual Task<int> ExpectedItemsProduced
        {
            get
            {
                _cleanedUpFlag.ThrowNotInitializedIfSet();
                return GetExpectedItemsProduced();
            }
        }

        public virtual Task AddNewConsumerGrain(Guid consumerGrainId)
        {
            _cleanedUpFlag.ThrowNotInitializedIfSet();
            return Task.WhenAll(_producers.Select(
                target =>
                    target.AddNewConsumerGrain(consumerGrainId)).ToArray());
        }

        private async Task<int> GetProducerCount()
        {
            _cleanedUpFlag.ThrowNotInitializedIfSet();
            var tasks = _producers.Select(p => p.ProducerCount).ToArray();
            await Task.WhenAll(tasks);
            return tasks.Sum(t => t.Result);
        }

        public virtual Task<int> ProducerCount
        {
            get
            {
                return GetProducerCount();
            }
        }

        public virtual async Task StopBeingProducer()
        {
            if (!_cleanedUpFlag.TrySet())
                return;

            var tasks = _producers.Select(p => p.StopBeingProducer()).ToArray();
            await Task.WhenAll(tasks);
        }

        public virtual async Task VerifyFinished()
        {
            var tasks = _producers.Select(p => p.VerifyFinished()).ToArray();
            await Task.WhenAll(tasks);
            _producers.Clear();
        }

        public virtual Task DeactivateProducerOnIdle()
        {
            _logger.Info("DeactivateProducerOnIdle");
            base.DeactivateOnIdle();
            return TaskDone.Done;
        }
    }

    public class PersistentStreaming_ProducerGrain : Streaming_ProducerGrain, IStreaming_ProducerGrain
    {
        private OrleansLogger _logger;

        public override async Task ActivateAsync()
        {
            await base.ActivateAsync();
            _logger = base.GetLogger("Test.PersistentStreaming_ProducerGrain " + base.RuntimeIdentity + "/" + base.IdentityString + "/" + base._Data.ActivationId);
            _logger.Info("ActivateAsync");
            if (State.Producers == null)
            {
                State.Producers = new List<IProducerObserver>();
                _producers = State.Producers;
            }
            else
            {
                foreach (var producer in State.Producers)
                {
                    producer.RenewProducer(_logger, base.GetStreamProvider(producer.ProviderName));
                    _producers.Add(producer);
                }
            }
        }

        public override Task DeactivateAsync()
        {
            _logger.Info("DeactivateAsync");
            return base.DeactivateAsync();
        }

        public override async Task BecomeProducer(StreamId streamId, string providerToUse)
        {
            await base.BecomeProducer(streamId, providerToUse);
            State.Producers = _producers;
            await State.WriteStateAsync();
        }

        public override async Task ProduceSequentialSeries(int count)
        {
            await base.ProduceParallelSeries(count);
            State.Producers = _producers;
            await State.WriteStateAsync();
        }

        public override async Task ProduceParallelSeries(int count)
        {
            await base.ProduceParallelSeries(count);
            State.Producers = _producers;
            await State.WriteStateAsync();
        }

        public override async Task StopBeingProducer()
        {
            await base.StopBeingProducer();
            State.Producers = _producers;
            await State.WriteStateAsync();
        }

        public override async Task VerifyFinished()
        {
            await base.VerifyFinished();
            await State.ClearStateAsync();
        }
    }

    //[StorageProvider(ProviderName = "MemoryStore")]
    public class Streaming_ConsumerGrain : GrainBase<IStreaming_ConsumerGrain_State>, IStreaming_ConsumerGrain    
    {
        private OrleansLogger _logger;
        protected List<IConsumerObserver> _observers;
        private string _providerToUse;
        
        public override Task ActivateAsync()
        {
            _logger = base.GetLogger("Test.Streaming_ConsumerGrain " + base.RuntimeIdentity + "/" + base.IdentityString + "/" + base._Data.ActivationId);
            _logger.Info("ActivateAsync");    
            _observers = new List<IConsumerObserver>();
            return TaskDone.Done;
        }

        public override Task DeactivateAsync()
        {
            _logger.Info("DeactivateAsync");
            return TaskDone.Done;
        }

        public virtual async Task BecomeConsumer(StreamId streamId, string providerToUse)
        {
            _providerToUse = providerToUse;
            ConsumerObserver consumerObserver = ConsumerObserver.NewObserver(_logger);
            await consumerObserver.BecomeConsumer(streamId, base.GetStreamProvider(providerToUse));
            _observers.Add(consumerObserver);
        }

        private async Task<int> GetItemsConsumed()
        {
            var tasks = _observers.Select(p => p.ItemsConsumed).ToArray();
            await Task.WhenAll(tasks);
            return tasks.Sum(t => t.Result);
        }

        public virtual Task<int> ItemsConsumed
        {
            get { return GetItemsConsumed(); }
        }

        private async Task<int> GetConsumerCount()
        {
            var tasks = _observers.Select(p => p.ConsumerCount).ToArray();
            await Task.WhenAll(tasks);
            return tasks.Sum(t => t.Result);
        }

        public virtual Task<int> ConsumerCount
        {
            get { return GetConsumerCount(); }
        }

        public virtual async Task StopBeingConsumer()
        {
            var tasks = _observers.Select(obs => obs.StopBeingConsumer(base.GetStreamProvider(_providerToUse))).ToArray();
            await Task.WhenAll(tasks);
            _observers.Clear();
        }

        public virtual Task DeactivateConsumerOnIdle()
        {
            _logger.Info("DeactivateConsumerOnIdle");

            Task.Delay(TimeSpan.FromSeconds(2)).ContinueWith((task) => { _logger.Info("DeactivateConsumerOnIdle ContinueWith fired."); }).Ignore(); // .WithTimeout(TimeSpan.FromSeconds(2));
            base.DeactivateOnIdle();
            return TaskDone.Done;
        }
    }

    [StorageProvider(ProviderName = "MemoryStore")]
    public class PersistentStreaming_ConsumerGrain : Streaming_ConsumerGrain, IPersistentStreaming_ConsumerGrain
    {
        private OrleansLogger _logger;

        public override async Task ActivateAsync()
        {
            await base.ActivateAsync();
            _logger = base.GetLogger("Test.PersistentStreaming_ConsumerGrain " + base.RuntimeIdentity + "/" + base.IdentityString + "/" + base._Data.ActivationId);
            _logger.Info("ActivateAsync");

            if (State.Consumers == null)
            {
                State.Consumers = new List<IConsumerObserver>();
                _observers = State.Consumers;
            }
            else
            {
                foreach (var consumer in State.Consumers)
                {
                    await consumer.RenewConsumer(_logger, base.GetStreamProvider(consumer.ProviderName));
                    _observers.Add(consumer);
                }
            }
        }

        public override Task DeactivateAsync()
        {
            _logger.Info("DeactivateAsync");
            return base.DeactivateAsync();
        }

        public override async Task BecomeConsumer(StreamId streamId, string providerToUse)
        {
            await base.BecomeConsumer(streamId, providerToUse);
            State.Consumers = _observers;
            await State.WriteStateAsync();
        }

        public override async Task StopBeingConsumer()
        {
            await base.StopBeingConsumer();
            State.Consumers = _observers;
            await State.WriteStateAsync();
        }
    }


    [Reentrant]
    public class Streaming_Reentrant_ProducerConsumerGrain : Streaming_ProducerConsumerGrain, IStreaming_Reentrant_ProducerConsumerGrain
    {
        private OrleansLogger _logger;

        public override Task ActivateAsync()
        {
            _logger = base.GetLogger("Test.Streaming_Reentrant_ProducerConsumerGrain " + base.RuntimeIdentity + "/" + base.IdentityString + "/" + base._Data.ActivationId);
            _logger.Info("ActivateAsync");
            return base.ActivateAsync();
        }

        protected override OrleansLogger MyLogger()
        {
            return _logger;
        }
    }

    public class Streaming_ProducerConsumerGrain : GrainBase, IStreaming_ProducerConsumerGrain
    {
        private OrleansLogger _logger;
        private ProducerObserver _producer;
        private ConsumerObserver _consumer;
        private string _providerToUseForConsumer;

        protected virtual OrleansLogger MyLogger()
        {
            return _logger;
        }

        public override Task ActivateAsync()
        {
            _logger = base.GetLogger("Test.Streaming_ProducerConsumerGrain " + base.RuntimeIdentity + "/" + base.IdentityString + "/" + base._Data.ActivationId);
            _logger.Info("ActivateAsync");
            return TaskDone.Done;
        }
        public override Task DeactivateAsync()
        {
            _logger.Info("DeactivateAsync");
            return TaskDone.Done;
        }

        public Task BecomeProducer(StreamId streamId, string providerToUse)
        {
            _producer = ProducerObserver.NewObserver(MyLogger());
            _producer.BecomeProducer(streamId, base.GetStreamProvider(providerToUse));
            return TaskDone.Done;
        }

        public Task ProduceSequentialSeries(int count)
        {
            return _producer.ProduceSequentialSeries(count);
        }

        public Task ProduceParallelSeries(int count)
        {
            return _producer.ProduceParallelSeries(count);
        }

        public Task ProducePeriodicSeries(int count)
        {
            return _producer.ProducePeriodicSeries((timerCallback) =>
            {
                return base.RegisterTimer(timerCallback, null, TimeSpan.Zero, TimeSpan.FromMilliseconds(10));
            }, count);
        }

        public Task<int> ItemsProduced
        {
            get { return _producer.ItemsProduced; }
        }

        public Task AddNewConsumerGrain(Guid consumerGrainId)
        {
            return _producer.AddNewConsumerGrain(consumerGrainId);
        }

        public Task BecomeConsumer(StreamId streamId, string providerToUse)
        {
            _providerToUseForConsumer = providerToUse;
            _consumer = ConsumerObserver.NewObserver(MyLogger());
            return _consumer.BecomeConsumer(streamId, base.GetStreamProvider(providerToUse));
        }

        public Task<int> ItemsConsumed
        {
            get { return _consumer.ItemsConsumed; }
        }

        public Task<int> ExpectedItemsProduced
        {
            get { return _producer.ExpectedItemsProduced; }
        }

        public Task<int> ConsumerCount
        {
            get { return _consumer.ConsumerCount; }
        }

        public Task<int> ProducerCount
        {
            get { return _producer.ProducerCount; }
        }

        public async Task StopBeingConsumer()
        {
            await _consumer.StopBeingConsumer(base.GetStreamProvider(_providerToUseForConsumer));
            _consumer = null;
        }

        public async Task StopBeingProducer()
        {
            await _producer.StopBeingProducer();
            
        }

        public async Task VerifyFinished()
        {
            await _producer.VerifyFinished();
            _producer = null;
        }

        public Task DeactivateConsumerOnIdle()
        {
            base.DeactivateOnIdle();
            return TaskDone.Done;
        }

        public Task DeactivateProducerOnIdle()
        {
            _logger.Info("DeactivateProducerOnIdle");
            base.DeactivateOnIdle();
            return TaskDone.Done;
        }
    }
}

#endif