#if !DISABLE_STREAMS
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.Streams;

namespace Orleans.Streams
{
    internal class StreamFactory<T> : IAsyncStream<T>
    {
        private readonly Func<IAsyncBatchObserver<T>>           _producerCreatorFunc;
        private readonly Func<IAsyncObservable<T>>              _consumerCreatorFunc;
        private WeakReference<IAsyncBatchObserver<T>>           _producerInterface;
        private IAsyncObservable<T>                             _consumerInterface;
        private readonly object                                 _initLock = new object(); // need the lock since the same code runs in the provider on the client and in the silo.

        public bool IsRewindable                                 { get; private set;}

        internal StreamFactory(Func<IAsyncBatchObserver<T>> producerCreatorFunc, Func<IAsyncObservable<T>> consumerCreatorFunc, bool isRewindable)
        {
            if (null == producerCreatorFunc)
                throw new ArgumentNullException("producerCreatorFunc");
            if (null == consumerCreatorFunc)
                throw new ArgumentNullException("consumerCreatorFunc");

            _producerCreatorFunc = producerCreatorFunc;
            _consumerCreatorFunc = consumerCreatorFunc;
            _producerInterface = null;
            _consumerInterface = null;
            IsRewindable = isRewindable;
        }

        public IAsyncBatchObserver<T> GetProducerInterface()
        {
            IAsyncBatchObserver<T> result = null;
            lock (_initLock)
            {
                if (_producerInterface == null || !_producerInterface.TryGetTarget(out result))
                {
                    result = _producerCreatorFunc();
                    _producerInterface = new WeakReference<IAsyncBatchObserver<T>>(result);
                }
                return result;
            }    
        }

        public IAsyncObservable<T> GetConsumerInterface()
        {
            if (_consumerInterface == null)
            {
                lock (_initLock)
                {
                    if (_consumerInterface == null)
                    {
                        _consumerInterface = _consumerCreatorFunc();
                    }
                }
            }
            return _consumerInterface;
        }

        // GKTODO: what does it mean to close the stream? 
        // Close local streamProducer/streamConsumer end? eliminate the whole stream (close ALL not-only-local consumers/producers)?
        // For now we decided leave it as not implemented.
        public Task CloseAsync(Exception ex = null)
        {
            throw new NotImplementedException("Closing stream is not implemented for now.");
        }
    }
}

#endif