using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Mime;
using System.Threading;
using System.Threading.Tasks;
using Orleans.Runtime.Coordination;
using Orleans.Runtime.Scheduler;
using Orleans.Runtime.MembershipService;
using Orleans.Scheduler;

using Orleans.Providers;

#if !DISABLE_STREAMS 
using Orleans.Streams;
#endif

namespace Orleans.Runtime.Providers
{
    internal class SiloProviderRuntime : IProviderRuntime
#if !DISABLE_STREAMS 
        ,IStreamProviderRuntime
#endif
    { 
        private static volatile SiloProviderRuntime instance;
        private static object syncRoot = new Object();
#if !DISABLE_STREAMS 
        private readonly GrainBasedPubSubRuntime pubSub;
#endif
        private readonly ConsistentRingProviderForGrains ring;

        private SiloProviderRuntime() 
        {
#if !DISABLE_STREAMS 
            pubSub = new GrainBasedPubSubRuntime();
#endif
            ring = new ConsistentRingProviderForGrains(InsideGrainClient.Current.ConsistentRingProvider);
        }

        // Implementation copied from: http://msdn.microsoft.com/en-us/library/ff650316.aspx
        public static SiloProviderRuntime Instance
        {
            get
            {
                if (instance == null)
                {
                    lock (syncRoot)
                    {
                        if (instance == null)
                            instance = new SiloProviderRuntime();
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
            ActivationData currentActivation = GetCurrentActivationData();
            return currentActivation.Address.ToString();
        }

        public SiloAddress ExecutingSiloAddress { get { return Silo.CurrentSilo.SiloAddress; } }

        public void RegisterSystemTarget(ISystemTarget target)
        {
            Silo.CurrentSilo.RegisterSystemTarget((SystemTarget)target);
        }

        public IDisposable RegisterTimer(Func<object, Task> asyncCallback, object state, TimeSpan dueTime, TimeSpan period)
        {
            var timer = OrleansTimerInsideGrain.FromTaskCallback(asyncCallback, state, dueTime, period);
            timer.Start();
            return timer;
        }

#if !DISABLE_STREAMS 
        public IStreamPubSub PubSub(StreamPubSubType pubSubType)
        {
            if (pubSubType == StreamPubSubType.GRAINBASED)
                return pubSub;
            return null;
        }
#endif
        public IConsistentRingProviderForGrains ConsistentRingProvider { get { return ring; } }

        public bool InSilo { get { return true; } }

        public Task<Tuple<TExtension, TExtensionInterface>> BindExtension<TExtension, TExtensionInterface>(Func<TExtension> newExtensionFunc)
            where TExtension : IGrainExtension
            where TExtensionInterface : IGrainExtension
        {
            // Hookup Extension.
            IAddressable currentGrain = GrainClient.Current.CurrentGrain;
            TExtension extension;
            if (!TryGetExtensionHandler(out extension))
            {
                extension = newExtensionFunc();
                if (!TryAddExtension(extension))
                {
                    throw new OrleansException("Failed to register " + typeof(TExtension).Name);
                }
            }

            var factoryName = String.Format("{0}.{1}Factory", typeof(TExtensionInterface).Namespace, typeof(TExtensionInterface).Name.Substring(1)); // skip the I
            TExtensionInterface currentTypedGrain = (TExtensionInterface)OrleansClient.InvokeStaticMethodThroughReflection(
                typeof(TExtensionInterface).Assembly.FullName,
                factoryName,
                "Cast",
                new Type[] { typeof(IAddressable) },
                new object[] { currentGrain });

            return Task.FromResult(Tuple.Create(extension, currentTypedGrain));
        }

        /// <summary>
        /// Adds the specified extension handler to the currently running activation.
        /// This method must be called during an activation turn.
        /// </summary>
        /// <param name="handler"></param>
        /// <returns></returns>
        internal bool TryAddExtension(IGrainExtension handler)
        {
            ActivationData currentActivation = GetCurrentActivationData();
            IGrainExtensionMethodInvoker invoker = TryGetInvoker(handler.GetType());
            if (invoker == null)
            {
                throw new SystemException("Extension method invoker was not generated for an extension interface");
            }
            return currentActivation.TryAddExtension(invoker, handler);
        }

        private ActivationData GetCurrentActivationData()
        {
            var context = RuntimeContext.Current.ActivationContext as OrleansContext;
            if (context == null)
            {
                throw new InvalidOperationException("Attempting to GetCurrentActivationData when not in an activation scope");
            }
            ActivationData currentActivation = context.Activation;
            return currentActivation;
        }

        /// <summary>
        /// Removes the specified extension handler (and any other extension that implements the same interface ID)
        /// from the currently running activation.
        /// This method must be called during an activation turn.
        /// </summary>
        /// <param name="handler"></param>
        internal void RemoveExtension(IGrainExtension handler)
        {
            ActivationData currentActivation = GetCurrentActivationData();
            currentActivation.RemoveExtension(handler);
        }

        internal bool TryGetExtensionHandler<TExtension>(out TExtension result)
        {
            ActivationData currentActivation = GetCurrentActivationData();
            IGrainExtension untypedResult;
            if (currentActivation.TryGetExtensionHandler(typeof(TExtension), out untypedResult))
            {
                result = (TExtension)untypedResult;
                return true;
            }
            else
            {
                result = default(TExtension);
                return false;
            }
        }

        private IGrainExtensionMethodInvoker TryGetInvoker(Type handlerType)
        {
            var interfaces = GrainClientGenerator.GrainInterfaceData.GetServiceInterfaces(handlerType).Values;
            if(interfaces.Count != 1)
                throw new InvalidOperationException(String.Format("Extension type {0} implements more than one grain interface.", handlerType.FullName));

            var interfaceId = GrainClientGenerator.GrainInterfaceData.ComputeInterfaceId(interfaces.First());
            var invoker = GrainTypeManager.Instance.GetInvoker(interfaceId);
            if (invoker != null)
                return (IGrainExtensionMethodInvoker) invoker;
            else
                throw new ArgumentException("Provider extension handler type " + handlerType + " was not found in the type manager", "handler");
        }
        
        public Task InvokeWithinSchedulingContextAsync(Func<Task> asyncFunc, object context)
        {
            if (null == asyncFunc)
                throw new ArgumentNullException("asyncFunc");
            if (null == context)
                throw new ArgumentNullException("context");
            if (!(context is ISchedulingContext))
                throw new ArgumentNullException("context object is not of a ISchedulingContext type.");

            // copied from InsideGrainClient.ExecAsync().
            return OrleansTaskScheduler.Instance.RunOrQueueAsyncCompletion(
                () => AsyncCompletion.FromTask(asyncFunc()),
                (ISchedulingContext)context).AsTask();
        }

        public object GetCurrentSchedulingContext()
        {
            return AsyncCompletion.Context;
        }
    }
}
