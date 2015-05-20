using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;

using Orleans.Counters;
using Orleans.Runtime.Coordination;
using Orleans.Runtime.Scheduler;

using Orleans.Scheduler;
using Orleans.Serialization;
using Orleans.Storage;


namespace Orleans.Runtime
{
    /// <summary>
    /// Internal class for system grains to get access to runtime object
    /// </summary>
    internal class InsideGrainClient : IGrainClient, IGrainClientInternal, ISiloShutdownParticipant
    {
        private static readonly Logger logger = Logger.GetLogger("InsideGrainClient", Logger.LoggerType.Runtime);
        private static readonly Logger invokeExceptionLogger = Logger.GetLogger("InsideGrainClient.InvokeException", Logger.LoggerType.Runtime);
        private static readonly Logger appLogger = Logger.GetLogger("Application", Logger.LoggerType.Application);

        private readonly Dispatcher dispatcher;

        private readonly ILocalGrainDirectory directory;

        internal readonly IConsistentRingProvider ConsistentRingProvider;

        private readonly List<IDisposable> disposables;

        private readonly Dictionary<CorrelationId, CallbackData> callbacks;

        public TimeSpan ResponseTimeout { get; private set; }

        private Action tryFinishShutdownAction;

        private readonly GrainTypeManager typeManager;

        private GrainInterfaceMap grainInterfaceMap;

#if !DISABLE_STREAMS
        public Orleans.Streams.IStreamProviderManager CurrentStreamProviderManager { get; internal set; }
#endif

        public InsideGrainClient(Dispatcher dispatcher, Catalog catalog, ILocalGrainDirectory directory, SiloAddress silo, OrleansConfiguration config, NodeConfiguration nodeConfig, IConsistentRingProvider ring, GrainTypeManager typeManager)
        {
            this.dispatcher = dispatcher;
            Silo = silo;
            //this.transport = transport;
            this.directory = directory;
            this.ConsistentRingProvider = ring;
            Catalog = catalog;
            disposables = new List<IDisposable>();
            callbacks = new Dictionary<CorrelationId, CallbackData>();
            Config = config;
            NodeConfig = nodeConfig;
            config.OnConfigChange("Globals/Message",
                () => ResponseTimeout = Config.Globals.ResponseTimeout);
            CallbackData.Config = Config.Globals;
            GrainClient.Current = this;
            
            this.typeManager = typeManager;
        }

        public static InsideGrainClient Current { get { return (InsideGrainClient)GrainClient.Current; } }

        public Catalog Catalog { get; private set; }

        public SiloAddress Silo { get; private set; }

        public Dispatcher Dispatcher { get { return dispatcher; } }

        public OrleansConfiguration Config { get; private set; }

        private readonly NodeConfiguration NodeConfig;

        public OrleansTaskScheduler Scheduler { get { return Dispatcher.Scheduler; } }

        #region Implementation of IGrainClient

        public void SendRequest(GrainReference target, InvokeMethodRequest request, TaskCompletionSource<object> context, Action<Message, TaskCompletionSource<object>> callback, string debugContext, InvokeMethodOptions options, string genericType = null, PlacementStrategy placement = null)
        {
            var message = GrainClient.CreateMessage(request, options, placement);
            SendRequestMessage(target, message, context, callback, debugContext, options, genericType);
        }

        private void SendRequestMessage(GrainReference target, Message message, TaskCompletionSource<object> context, Action<Message, TaskCompletionSource<object>> callback, string debugContext, InvokeMethodOptions options, string genericType = null)
        {
            // fill in sender
            if (message.SendingSilo == null)
                message.SendingSilo = Silo;
            if (!String.IsNullOrEmpty(genericType))
                message.GenericGrainType = genericType;

            OrleansContext orleansContext = RuntimeContext.Current != null ? RuntimeContext.Current.ActivationContext as OrleansContext : null;
            if (orleansContext == null)
            {
                throw new ArgumentException(
                    String.Format("Trying to send a message on a silo not from within grain and not from within system target (RuntimeContext is not set to OrleansContext) "
                        + "RuntimeContext.Current={0} TaskScheduler.Current={1}",
                        RuntimeContext.Current == null ? "null" : RuntimeContext.Current.ToString(),
                        TaskScheduler.Current),
                    "OrleansContext");
            }
            else if (orleansContext.ContextType == SchedulingContextType.SystemThread)
            {
                throw new ArgumentException(String.Format("Trying to send a message on a silo not from within grain and not from within system target (RuntimeContext is of SchedulingContextType.SystemThread type)"), "context");
            }
            else if (orleansContext.ContextType == SchedulingContextType.Activation)
            {
                message.SendingActivation = orleansContext.Activation.ActivationId;
                message.SendingGrain = orleansContext.Activation.Grain;
            }
            else if (orleansContext.ContextType == SchedulingContextType.SystemTarget)
            {
                message.SendingActivation = orleansContext.SystemTarget.ActivationId;
                message.SendingGrain = orleansContext.SystemTarget.Grain;
            }

            // fill in destination
            GrainId targetGrainId = target.GrainId;
            message.TargetGrain = targetGrainId;
            if (targetGrainId.IsSystemTarget)
            {
                SiloAddress targetSilo = (target.SystemTargetSilo ?? Silo);
                message.TargetSilo = targetSilo;
                message.TargetActivation = ActivationId.GetSystemActivation(targetGrainId, targetSilo);
                message.Category = targetGrainId.Equals(Constants.MembershipOracleId)
                    ? Message.Categories.Ping : Message.Categories.System;
            }

            if (debugContext != null)
                message.DebugContext = debugContext;
            var oneWay = (options & InvokeMethodOptions.OneWay) != 0;
            if (context == null && !oneWay)
                logger.Warn(ErrorCode.IGC_SendRequest_NullContext, "Null context {0}: {1}", message, new System.Diagnostics.StackTrace());
            message.AddTimestamp(Message.LifecycleTag.Create);

            if (message.IsExpirableMessage(Config.Globals))
            {
                message.Expiration = DateTime.UtcNow + ResponseTimeout + Constants.MAXIMUM_CLOCK_SKEW;
            }

            if (!oneWay)
            {
                var callbackData = new CallbackData(callback, TryResendMessage, context, message, () => UnRegisterCallback(message.Id));
                RegisterCallback(message.Id, callbackData);
                callbackData.StartTimer(ResponseTimeout);
            }

            if (targetGrainId.IsSystemTarget)
            {
                // Messages to system targets bypass the task system and get sent "in-line"
                dispatcher.TransportMessage(message);
            }
            else
            {
                dispatcher.SendMessage(message);
            }
        }

        private void SendResponse(Message request, OrleansResponse response)
        {
            // Don't process messages that have already timed out
            if (request.IsExpired)
            {
                request.DropExpiredMessage(MessagingStatisticsGroup.Phase.Respond);
                return;
            }

            dispatcher.SendResponse(request, response);
        }

        /// <summary>
        /// Reroute a message coming in through a gateway
        /// </summary>
        /// <param name="message"></param>
        internal void RerouteMessage(Message message)
        {
            message.RemoveHeader(Message.Header.ReroutingRequested);
            ResendMessage_Impl(message);
        }

        private bool TryResendMessage(Message message)
        {
            if (!message.MayResend(Config.Globals))
            {
                return false;
            }
            message.ResendCount = message.ResendCount + 1;
            message.RemoveHeader(Message.Header.TaskHeader);

            MessagingProcessingStatisticsGroup.OnIGCMessageResend(message);
            ResendMessage_Impl(message);
            return true;
        }

        internal bool TryForwardMessage(Message message)
        {
            if (!message.MayForward(Config.Globals))
            {
                return false;
            }
            message.ForwardCount = message.ForwardCount + 1;
            message.RemoveHeader(Message.Header.TaskHeader);

            MessagingProcessingStatisticsGroup.OnIGCMessageForwared(message);
            ResendMessage_Impl(message);
            return true;
        }

        private void ResendMessage_Impl(Message message)
        {
            if (logger.IsVerbose) logger.Verbose("Resend {0}", message);
            message.SetMetadata(Message.Metadata.TARGET_HISTORY, message.GetTargetHistory());

            if (message.TargetGrain.IsSystemTarget)
            {
                dispatcher.SendSystemTargetMessage(message);
            }
            else
            {
                message.RemoveHeader(Message.Header.TargetActivation);
                message.RemoveHeader(Message.Header.TargetSilo);
                dispatcher.SendMessage(message);
            }
        }

        /// <summary>
        /// Register a callback for when response is received for a message
        /// </summary>
        /// <param name="id"></param>
        /// <param name="callbackData"></param>
        internal void RegisterCallback(CorrelationId id, CallbackData callbackData)
        {
            lock (callbacks)
            {
                callbacks.Add(id, callbackData);
            }
        }

        internal bool TryGetCallback(CorrelationId id, out CallbackData callbackData)
        {
            lock (callbacks)
            {
                return callbacks.TryGetValue(id, out callbackData);
            }
        }

        /// <summary>
        /// UnRegister a callback.
        /// </summary>
        /// <param name="id"></param>
        internal void UnRegisterCallback(CorrelationId id)
        {
            lock (callbacks)
            {
                callbacks.Remove(id);
            }
        }

        public void SniffIncomingMessage(Message message)
        {
            try
            {
                if (message.ContainsHeader(Message.Header.CacheInvalidationHeader))
                {
                    foreach (ActivationAddress address in message.CacheInvalidationHeader)
                    {
                        directory.InvalidateCacheEntryPartly(address.Grain, address.Activation);
                    }
                }
#if false
                //// 1:
                //// Also record sending activation address for responses only in the cache.
                //// We don't record sending addresses for requests, since it is not clear that this silo ever wants to send messages to the grain sending this request.
                //// However, it is sure that this silo does send messages to the sender of a reply. 
                //// In most cases it will already have its address cached, unless it had a wrong outdated address cached and now this is a fresher address.
                //// It is anyway always safe to cache the replier address.
                //// 2: 
                //// after further thought deciode not to do it.
                //// It seems to better not bother caching the sender of a response at all, 
                //// and instead to take a very occasional hit of a full remote look-up instead of this small but non-zero hit on every response.
                //if (message.Direction.Equals(Message.Directions.Response) && message.Result.Equals(Message.ResponseTypes.Success))
                //{
                //    ActivationAddress sender = message.SendingAddress;
                //    // just make sure address we are about to cache is OK and cachable.
                //    if (sender.IsComplete && !sender.Grain.IsClient && !sender.Grain.IsSystemTargetType && !sender.Activation.IsSystemTargetType)
                //    {
                //        directory.AddCacheEntry(sender);
                //    }
                //}
#endif
            }
            catch (Exception exc)
            {
                logger.Warn(ErrorCode.IGC_SniffIncomingMessage_Exc, "SniffIncomingMessage has thrown exception. Ignoring.", exc);
            }
        }

        internal async Task Invoke(IAddressable target, IInvokable invokable, Message message)
        {
            try
            {
                // Don't process messages that have already timed out
                if (message.IsExpired)
                {
                    message.DropExpiredMessage(MessagingStatisticsGroup.Phase.Invoke);
                    return;
                }

                //MessagingProcessingStatisticsGroup.OnRequestProcessed(message, "Invoked");
                message.AddTimestamp(Message.LifecycleTag.InvokeIncoming);

                RequestContext.ImportFromMessage(message);
                if (Config.Globals.PerformDeadlockDetection && !message.TargetGrain.IsSystemTarget)
                {
                    UpdateDeadlockInfoInRequestContext(new RequestInvocationHistory(message));
                    // RequestContext is automatically saved in the msg upon send and propagated to the next hop
                    // in GrainClient.CreateMessage -> RequestContext.ExportToMessage(message);
                }
                var request = (InvokeMethodRequest)message.BodyObject;

                IGrainMethodInvoker invoker = invokable.GetInvoker(message.InterfaceId, message.GenericGrainType);

                object resultObject = await invoker.Invoke(target, request.InterfaceId, request.MethodId, request.Arguments);

                if (message.Direction == Message.Directions.OneWay)
                {
                    return;
                }
                else
                {
                    SafeSendResponse(message, resultObject);
                }
            }
            catch (Exception exc)
            {
                if (invokeExceptionLogger.IsVerbose)
                {
                    invokeExceptionLogger.Verbose(ErrorCode.Runtime_Error_100322,
                        "Exception during Invoke of message: {0}. {1}", message, exc.GetBaseException().Message);
                }
                SafeSendExceptionResponse(message, exc);
            }
        }

        private void SafeSendResponse(Message message, object resultObject)
        {
            try
            {
                SendResponse(message, new OrleansResponse(SerializationManager.DeepCopy(resultObject)));
            }
            catch (Exception exc)
            {
                logger.Warn(ErrorCode.IGC_SendResponseFailed,
                    "Exception trying to send a response: {0}", exc);
                SendResponse(message, OrleansResponse.ExceptionResponse(exc)); 
            }
        }

        private void SafeSendExceptionResponse(Message message, Exception ex)
        {
            try
            {
                SendResponse(message, OrleansResponse.ExceptionResponse((Exception)SerializationManager.DeepCopy(ex)));
            }
            catch (Exception exc1)
            {
                try
                {
                    logger.Warn(ErrorCode.IGC_SendExceptionResponseFailed,
                        "Exception trying to send an exception response: {0}", exc1);
                    SendResponse(message, OrleansResponse.ExceptionResponse(exc1));
                }
                catch (Exception exc2)
                {
                    logger.Warn(ErrorCode.IGC_UnhandledExceptionInInvoke,
                        "Exception trying to send an exception. Ignoring and not trying to send again. Exc: {0}", exc2);
                }
            }
        }

        // assumes deadlock information was already loaded into RequestContext from the message
        private static void UpdateDeadlockInfoInRequestContext(RequestInvocationHistory thisInvocation)
        {
#if false
            List<RequestInvocationHistory> newChain = new List<RequestInvocationHistory>();
            IEnumerable<RequestInvocationHistory> prevChain = null;

            object obj = RequestContext.Get(RequestContext.CallChainRequestContextHeader);
            if (obj != null)
            {
                prevChain = ((IEnumerable)obj).Cast<RequestInvocationHistory>();
                newChain.AddRange(prevChain);
            }
            newChain.Add(thisInvocation); // append this call to the end of the call chain.
            RequestContext.Set(RequestContext.CallChainRequestContextHeader, newChain);
#endif
            IList prevChain;
            object obj = RequestContext.Get(RequestContext.CallChainRequestContextHeader);
            if (obj != null)
            {
                prevChain = ((IList)obj);
            }
            else
            {
                prevChain = new List<RequestInvocationHistory>();
                RequestContext.Set(RequestContext.CallChainRequestContextHeader, prevChain);
            }
            // append this call to the end of the call chain. Update in place.
            prevChain.Add(thisInvocation);
        }

        public void ReceiveResponse(Message message)
        {
            if (message.Result == Message.ResponseTypes.Rejection)
            {
                if (!message.TargetSilo.Matches(this.CurrentSilo))
                {
                    // gatewayed message - gateway back to sender
                    if (logger.IsVerbose2) logger.Verbose2(ErrorCode.Dispatcher_NoCallbackForRejectionResp, "No callback for rejection response message: {0}", message);
                    dispatcher.Transport.SendMessage(message);
                    return;
                }
                if (logger.IsVerbose) logger.Verbose(ErrorCode.Dispatcher_HandleMsg, "HandleMessage {0}", message);
                switch (message.RejectionType)
                {
                    case Message.RejectionTypes.DuplicateRequest:
                        // ignore
                        return;

                    case Message.RejectionTypes.Unrecoverable:
                    // fall through & reroute
                    case Message.RejectionTypes.FutureTransient:
                    // fall through
                    case Message.RejectionTypes.Transient:
                        if (!message.ContainsHeader(Message.Header.CacheInvalidationHeader))
                        {
                            // Remove from local directory cache. Note that SendingGrain is the original target, since message is the rejection response.
                            // If CacheMgmtHeader is present, we already did this. Otherwise, we left this code for backward compatability. 
                            // It should be retired as we move to use CacheMgmtHeader in all relevant places.
                            directory.InvalidateCacheEntry(message.SendingGrain);
                        }
                        break;

                    default:
                        logger.Error(ErrorCode.Dispatcher_InvalidEnum_RejectionType, "Missing enum in switch: " + message.RejectionType);
                        break;
                }
            }
            CallbackData callbackData;
            bool found = TryGetCallback(message.Id, out callbackData);
            if (found)
            {
                // IMPORTANT: we do not schedule the response callback via the scheduler, since the only thing it does
                // is to resolve/break the resolver. The continuations/waits that are based on this resolution will be scheduled as work items. 
                callbackData.DoCallback(message);
            }
            else
            {
                if (logger.IsVerbose) logger.Verbose(ErrorCode.Dispatcher_NoCallbackForResp, "No callback for response message: " + message);
            }
        }

        public OrleansLogger AppLogger
        {
            get { return appLogger; }
        }

        public string Identity
        {
            get { return Silo.ToLongString(); }
        }

        public IAddressable CurrentGrain
        {
            get
            {
                if (RuntimeContext.Current != null)
                {
                    OrleansContext context = RuntimeContext.Current.ActivationContext as OrleansContext;
                    if (context != null && context.Activation != null)
                    {
                        return context.Activation.GrainInstance;
                    }
                }
                return null;
            }
        }

        public IStorageProvider CurrentStorageProvider
        {
            get
            {
                if (RuntimeContext.Current != null)
                {
                    OrleansContext context = RuntimeContext.Current.ActivationContext as OrleansContext;
                    if (context != null && context.Activation != null)
                    {
                        return context.Activation.StorageProvider;
                    }
                }
                throw new InvalidOperationException("Storage provider only available from inside grain");
            }
        }

        private void LogReminderOperation(string operation, string reminderName, GrainId grainId, SiloAddress destination)
        {
            logger.Info("{0} for reminder {1}, grainId: {2} responsible silo: {3}/x{4, 8:X8} based on {5}", 
                operation, reminderName, 
                grainId.ToStringWithHashCode(),
                destination, destination.GetConsistentHashCode(), 
                ConsistentRingProvider.ToString());
        }

        public AsyncValue<IOrleansReminder> RegisterOrUpdateReminder(string reminderName, TimeSpan dueTime, TimeSpan period)
        {
            CheckValidReminderServiceType("RegisterOrUpdateReminder");
            GrainId grainId = CurrentGrain.AsReference().GrainId;
            SiloAddress destination = MapGrainIdToSiloRing(grainId);
            if (logger.IsInfo) LogReminderOperation("RR", reminderName, grainId, destination);
            return AsyncValue.FromTask(GetReminderService(destination).RegisterOrUpdateReminder(grainId, reminderName, dueTime, period));
        }

        public AsyncCompletion UnregisterReminder(IOrleansReminder reminder)
        {
            CheckValidReminderServiceType("UnregisterReminder");
            GrainId grainId = CurrentGrain.AsReference().GrainId;
            SiloAddress destination = MapGrainIdToSiloRing(grainId);
            if (logger.IsInfo) LogReminderOperation("UR", "", grainId, destination);
            return AsyncCompletion.FromTask(GetReminderService(destination).UnregisterReminder(reminder));
        }

        public AsyncValue<IOrleansReminder> GetReminder(string reminderName)
        {
            CheckValidReminderServiceType("GetReminder");
            GrainId grainId = CurrentGrain.AsReference().GrainId;
            SiloAddress destination = MapGrainIdToSiloRing(grainId);
            if (logger.IsInfo) LogReminderOperation("GR", reminderName, grainId, destination);
            return AsyncValue.FromTask(GetReminderService(destination).GetReminder(grainId, reminderName));
        }

        public AsyncValue<List<IOrleansReminder>> GetReminders()
        {
            CheckValidReminderServiceType("GetReminders");
            GrainId grainId = CurrentGrain.AsReference().GrainId;
            SiloAddress destination = MapGrainIdToSiloRing(grainId);
            if (logger.IsInfo) LogReminderOperation("GRs", "", grainId, destination);
            return AsyncValue.FromTask(GetReminderService(destination).GetReminders(grainId));
        }

        public async Task ExecAsync(Func<Task> action, ISchedulingContext context)
        {
            // Schedule call back to grain context
            await OrleansTaskScheduler.Instance.RunOrQueueAsyncCompletion(
                () => AsyncCompletion.FromTask(action()), 
                context).AsTask();
        }

        public void Reset()
        {
            throw new InvalidOperationException();
        }

        public TimeSpan GetResponseTimeout()
        {
            return ResponseTimeout;
        }

        public void SetResponseTimeout(TimeSpan timeout)
        {
            ResponseTimeout = timeout;
        }

        public Task<GrainReference> CreateObjectReference(IGrainObserver obj, IGrainMethodInvoker invoker)
        {
            return CreateObjectReference((IAddressable)obj, invoker);
        }

        public Task<GrainReference> CreateObjectReference(IAddressable obj, IGrainMethodInvoker invoker)
        {
            throw new InvalidOperationException("Cannot create a local object reference from a grain.");
        }

        public Task DeleteObjectReference(IGrainObserver obj)
        {
            return DeleteObjectReference((IAddressable)obj);
        }

        public Task DeleteObjectReference(IAddressable obj)
        {
            throw new InvalidOperationException("Cannot delete a local object reference from a grain.");
        }

        public ActivationAddress CurrentActivation
        {
            get
            {
                var context = RuntimeContext.Current.ActivationContext as OrleansContext;
                return context == null ? null
                    : ActivationAddress.GetAddress(Silo, context.Activation.Grain, context.Activation.ActivationId);
            }
        }

        public SiloAddress CurrentSilo
        {
            get { return Silo; }
        }

        public void DeactivateOnIdle(ActivationId id)
        {
            ActivationData data;
            if (!Catalog.TryGetActivationData(id, out data))
                return; // already gone
            data.ResetKeepAliveRequest(); // DeactivateOnIdle method would undo / override any current “keep alive” setting, making this grain immideately avaliable for deactivation.
            Catalog.QueueShutdownActivations(new List<ActivationData> { data });
        }

        #endregion

        #region Implementation of ISiloShutdownParticipant

        public void BeginShutdown(Action tryFinishShutdown)
        {
            this.tryFinishShutdownAction = tryFinishShutdown;
            //if (pendingWrites == 0)
            if (true)
            {
                tryFinishShutdownAction();
            }
        }

        public bool CanFinishShutdown()
        {
            //return pendingWrites == 0;
            return true;
        }

        public void FinishShutdown()
        {
            Stop();
        }

        public SiloShutdownPhase Phase { get { return SiloShutdownPhase.Messaging; } }

        #endregion

        internal void Stop()
        {
            lock (disposables)
            {
                foreach (var disposable in disposables)
                {
                    try
                    {
                        disposable.Dispose();
                    }
                    catch (Exception e)
                    {
                        logger.Warn(ErrorCode.IGC_DisposeError, "Exception while disposing: " + e.Message, e);
                    }
                }
            }
        }

        internal void Start()
        {
            grainInterfaceMap = typeManager.GetTypeCodeMap();
        }

        public IGrainTypeResolver GrainTypeResolver
        {
            get { return grainInterfaceMap; }
        }

        private void CheckValidReminderServiceType(string doingWhat)
        {
            if (Config.Globals.ReminderServiceType.Equals(GlobalConfiguration.ReminderServiceProviderType.NotSpecified))
            {
                throw new InvalidOperationException(
                    string.Format("Cannot {0} when ReminderServiceProviderType is {1}",
                    doingWhat, GlobalConfiguration.ReminderServiceProviderType.NotSpecified));
            }
        }

        private SiloAddress MapGrainIdToSiloRing(GrainId grainId)
        {
            int hashCode = grainId.GetUniformHashCode();
            return ConsistentRingProvider.GetPrimary(hashCode);
        }

        private IReminderService GetReminderService(SiloAddress destination)
        {
            return ReminderServiceFactory.GetSystemTarget(Constants.ReminderServiceId, destination);
        }

        public string CaptureRuntimeEnvironment()
        {
            var callStack = new System.Diagnostics.StackTrace(1); // Don't include this method in stack trace
            return String.Format("   TaskScheduler={0}\n   AsyncCompletion.Context={1}\n   RuntimeContext={2}\n   WorkerPoolThread={3}\n   WorkerPoolThread.CurrentWorkerThread.ManagedThreadId={4}\n   Thread.CurrentThread.ManagedThreadId={5}\n   StackTrace=\n{6}",
                    TaskScheduler.Current,
                    AsyncCompletion.Context,
                    RuntimeContext.Current,
                    WorkerPoolThread.CurrentWorkerThread == null ? "null" : WorkerPoolThread.CurrentWorkerThread.Name,
                    WorkerPoolThread.CurrentWorkerThread == null ? "null" : WorkerPoolThread.CurrentWorkerThread.ManagedThreadId.ToString(CultureInfo.InvariantCulture),
                    System.Threading.Thread.CurrentThread.ManagedThreadId,
                    callStack);
        }


        public IGrainMethodInvoker GetInvoker(int interfaceId, string genericGrainType = null)
        {
            return GrainTypeManager.Instance.GetInvoker(interfaceId, genericGrainType);
        }

        public SiloStatus GetSiloStatus(SiloAddress siloAddress)
        {
            return Orleans.Runtime.Silo.CurrentSilo.LocalSiloStatusOracle.GetApproximateSiloStatus(siloAddress);
        }
    }
}
