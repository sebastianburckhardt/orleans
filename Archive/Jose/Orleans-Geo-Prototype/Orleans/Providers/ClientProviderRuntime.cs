using System;
using System.Collections.Generic;
using System.Globalization;
using System.Reflection;
using System.Threading.Tasks;

#if !DISABLE_STREAMS
using Orleans.Streams;
#endif


namespace Orleans.Providers
{
    internal class ClientProviderRuntime : IProviderRuntime
#if !DISABLE_STREAMS
        ,IStreamProviderRuntime
#endif
    { 
        private static volatile ClientProviderRuntime instance;
        private static readonly object syncRoot = new Object();
#if !DISABLE_STREAMS
        private readonly GrainBasedPubSubRuntime pubSub;
#endif
        private ClientProviderRuntime() 
        {
#if !DISABLE_STREAMS
            pubSub = new GrainBasedPubSubRuntime();
#endif
        }

        public static ClientProviderRuntime Instance
        {
            get
            {
                if (instance == null)
                {
                    lock (syncRoot)
                    {
                        if (instance == null)
                            instance = new ClientProviderRuntime();
                    }
                }
                return instance;
            }
        }

        public OrleansLogger GetLogger(string loggerName, Logger.LoggerType logType)
        {
            return Logger.GetLogger(loggerName, logType);
        }

        public string ExecutingEntityIdentity()
        {
            return GrainClient.Current.Identity;
        }

        public SiloAddress ExecutingSiloAddress
        {
            get { throw new NotImplementedException(); }
        }

        public void RegisterSystemTarget(ISystemTarget target)
        {
            throw new NotImplementedException();
        }

        public IDisposable RegisterTimer(Func<object, Task> asyncCallback, object state, TimeSpan dueTime, TimeSpan period)
        {
            return new AsyncTaskSafeTimer(asyncCallback, state, dueTime, period);
        }

        public async Task<Tuple<TExtension, TExtensionInterface>> BindExtension<TExtension, TExtensionInterface>(Func<TExtension> newExtensionFunc)
            where TExtension : IGrainExtension
            where TExtensionInterface : IGrainExtension
        {
            var extension = newExtensionFunc();

            // until we have a means to get the factory related to a grain interface, we have to search linearly for
            // the factory. 
            var factoryName = String.Format("{0}.{1}Factory", typeof(TExtensionInterface).Namespace, typeof(TExtensionInterface).Name.Substring(1)); // skip the I
            object obj = OrleansClient.InvokeStaticMethodThroughReflection(
                    typeof(TExtensionInterface).Assembly.FullName,
                    factoryName,
                    "CreateObjectReference",
                    new Type[] { typeof(TExtensionInterface) },
                    new object[] { extension });
            Task<TExtensionInterface> task = (Task<TExtensionInterface>)obj;
            IAddressable addressable = (IAddressable)await task;

            TExtensionInterface typedAddressable = (TExtensionInterface)OrleansClient.InvokeStaticMethodThroughReflection(
                 typeof(TExtensionInterface).Assembly.FullName,
                 factoryName,
                 "Cast",
                 new Type[] { typeof(IAddressable) },
                 new object[] { addressable });
            // we have to return the extension as well as the IAddressable because the caller needs to root the extension
            // to prevent it from being collected (the IAddressable uses a weak reference).
            return Tuple.Create(extension, typedAddressable);
        }

#if !DISABLE_STREAMS
        public IStreamPubSub PubSub(StreamPubSubType pubSubType)
        {
            if (pubSubType == StreamPubSubType.GRAINBASED)
                return pubSub;
            return null;
        }
#endif

        public IConsistentRingProviderForGrains ConsistentRingProvider 
        {
            get { throw new NotImplementedException(); }
        }

        public bool InSilo { get { return false; } }

        public Task InvokeWithinSchedulingContextAsync(Func<Task> asyncFunc, object context)
        {
            if (context != null)
                throw new ArgumentException("The grain client only supports a null scheduling context.");
            return Task.Run(asyncFunc);
        }

        public object GetCurrentSchedulingContext()
        {
            return null;
        }
    }
}


