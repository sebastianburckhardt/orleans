using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

using Orleans.Counters;
using Orleans.Messaging;
using Orleans.Providers;

using Orleans.Serialization;
using Orleans.Storage;
using Orleans.AzureUtils;

namespace Orleans
{
    internal class OutsideGrainClient : IGrainClient, IGrainClientInternal, IDisposable
    {
        internal static bool TestOnlyThrowExceptionDuringInit { get; set; }

        private readonly Logger logger;
        private readonly Logger appLogger;

        private readonly ClientConfiguration config;

        private readonly Dictionary<CorrelationId, CallbackData> callbacks;
        private readonly Dictionary<GrainId, LocalObjectData> localObjects;
        //private readonly Dictionary<GrainId, CorrelationId> lastSent;

        private readonly ProxiedMessageCenter transport;
        private bool listenForMessages;
        private CancellationTokenSource listeningCTS;

        private ClientStatisticsManager clientStatistics;
        private readonly Guid clientId;
        private GrainInterfaceMap grainInterfaceMap;
        private readonly ThreadTrackingStatistic incomingMessagesThreadTimeTracking;

        /// <summary>
        /// Response timeout.
        /// </summary>
        private TimeSpan responseTimeout;

        private static readonly Object staticLock = new Object();

        OrleansLogger IGrainClient.AppLogger
        {
            get { return appLogger; }
        }

        public ActivationAddress CurrentActivation
        {
            get;
            private set;
        }

        public SiloAddress CurrentSilo
        {
            get { return CurrentActivation.Silo; } //transport.MyAddress
        }

        public string Identity
        {
            get { return CurrentActivation.ToString(); }
        }

        public IAddressable CurrentGrain
        {
            get { return null; }
        }

        public IStorageProvider CurrentStorageProvider
        {
            get { throw new InvalidOperationException("Storage provider only available from inside grain"); }
        }

        internal List<IPEndPoint> Gateways
        {
            get
            {
                return transport.gatewayManager.listProvider.GetGateways();
            }
        }

#if !DISABLE_STREAMS
        public Orleans.Streams.IStreamProviderManager CurrentStreamProviderManager { get; private set; }
#endif

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope",
            Justification = "MessageCenter is IDisposable but cannot call Dispose yet as it lives past the end of this method call.")]
        public OutsideGrainClient(ClientConfiguration cfg, bool secondary = false)
        {
            this.clientId = Guid.NewGuid();

            if (cfg == null)
            {
                Console.WriteLine("An attempt to create an OutsideGrainClient with null ClientConfiguration object.");
                throw new ArgumentException("OutsideGrainClient was attempted to be created with null ClientConfiguration object.", "cfg");
            }

            this.config = cfg;

            if (!Logger.IsInitialized) Logger.Initialize(config);
            StatisticsCollector.Initialize(config);
            SerializationManager.Initialize(config.UseStandardSerializer);
            logger = Logger.GetLogger("OutsideGrainClient", Logger.LoggerType.Runtime);
            appLogger = Logger.GetLogger("Application", Logger.LoggerType.Application);

            try
            {
                LoadAdditionalAssemblies();
                
                PlacementStrategy.Initialize();

                callbacks = new Dictionary<CorrelationId, CallbackData>();
                localObjects = new Dictionary<GrainId, LocalObjectData>();
                //lastSent = new Dictionary<GrainId, CorrelationId>();
                CallbackData.Config = config;

                if (!secondary)
                {
                    UnobservedExceptionsHandlerClass.SetUnobservedExceptionHandler(UnhandledException);
                }
                // Ensure SerializationManager static constructor is called before AssemblyLoad event is invoked
                SerializationManager.GetDeserializer(typeof(String));
                // Ensure that any assemblies that get loaded in the future get recorded
                AppDomain.CurrentDomain.AssemblyLoad += NewAssemblyHandler;

                // Load serialization info for currently-loaded assemblies
                foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
                {
                    if (!assembly.ReflectionOnly)
                    {
                        SerializationManager.FindSerializationInfo(assembly);
                    }
                    else
                    {
                        try { if (logger.IsVerbose2) logger.Verbose2("Skipping scan of assembly {0} because it is loaded into the reflection-only context", assembly.Location); }
                        catch { }
                    }
                }

                responseTimeout = Debugger.IsAttached ? Constants.DEFAULT_RESPONSE_TIMEOUT : config.ResponseTimeout;
                BufferPool.InitGlobalBufferPool(config);
                IPAddress localAddress = OrleansConfiguration.GetLocalIPAddress(config.PreferredFamily, config.NetInterface);
              
                logger.Info(ErrorCode.Runtime_Error_100313, "-------------- Initializing OutsideGrainClient on " + config.DNSHostName + " at " + localAddress + " client GUID Id " + clientId + " ------------------------");
                logger.Info(ErrorCode.Runtime_Error_100314, "Starting OutsideGrainClient with runtime Version='{0}' Config= \n{1}", Version.Current, config.ToString());

                if (TestOnlyThrowExceptionDuringInit)
                {
                    throw new ApplicationException("TestOnlyThrowExceptionDuringInit");
                }

                config.CheckGatewayProviderSettings();

                var generation = -SiloAddress.AllocateNewGeneration(); // Client generations are negative
                IGatewayListProvider gwListProvider = GatewayProviderFactory.CreateGatewayListProvider(config).WithTimeout(AzureTableDefaultPolicies.TableCreation_TIMEOUT).Result;
                transport = new ProxiedMessageCenter(config, localAddress, generation, clientId, gwListProvider);
                
                this.StreamingInitialize();

                if (StatisticsCollector.CollectThreadTimeTrackingStats)
                {
                    string threadName = "ClientReceiver";
                    incomingMessagesThreadTimeTracking = new ThreadTrackingStatistic(threadName);
                }
            }
            catch (Exception exc)
            {
                if (logger != null) logger.Error(ErrorCode.Runtime_Error_100319, "OutsideGrainClient constructor failed.", exc);
                ConstructorReset();
                throw;
            }
        }

        private void StreamingInitialize()
        {
#if !DISABLE_STREAMS
            var streamProviderManager = new Streams.StreamProviderManager();
            streamProviderManager
                .LoadStreamProviders(
                    this.config.ProviderConfigurations,
                    ClientProviderRuntime.Instance)
                .Wait();
            CurrentStreamProviderManager = streamProviderManager;
#endif
        }

        private static void LoadAdditionalAssemblies()
        {
            var directories =
                new Dictionary<string, SearchOption>
                    {
                        {
                            Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), 
                            SearchOption.AllDirectories
                        }
                    };
            var excludeCriteria =
                new AssemblyLoaderPathNameCriterion[]
                    {
                        AssemblyLoaderCriteria.ExcludeResourceAssemblies,
                        AssemblyLoaderCriteria.ExcludeSystemBinaries()
                    };
#if !DISABLE_STREAMS
            var loadCriteria =
                new AssemblyLoaderReflectionCriterion[]
                    {
                        AssemblyLoaderCriteria.LoadTypesAssignableFrom(typeof(Streams.IStreamProvider))
                    };

            AssemblyLoader.LoadAssemblies(directories, excludeCriteria, loadCriteria, Logger.GetLogger("AssemblyLoader", Logger.LoggerType.Runtime));
#endif
        }

        private void NewAssemblyHandler(object sender, AssemblyLoadEventArgs args)
        {
            SerializationManager.FindSerializationInfo(args.LoadedAssembly);
        }

        private void UnhandledException(ISchedulingContext context, Exception exception)
        {
            logger.Error(ErrorCode.Runtime_Error_100007, String.Format("OutsideGrainClient caught an UnobservedException."), exception);
            logger.Assert(ErrorCode.Runtime_Error_100008, context == null, "context should be not null only inside OrleansRuntime and not on the client.");
        }

        public void Start()
        {
            lock (staticLock)
            {
                if (GrainClient.Current != null)
                    throw new InvalidOperationException("Can only have one GrainClient per AppDomain");
                GrainClient.Current = this;
            }
            StartInternal();

            logger.Info(ErrorCode.ProxyClient_StartDone, "Started OutsideGrainClient with Global Client Grain ID: " + CurrentActivation.ToString() + ", client GUID ID: " + clientId);
              
        }

        // used for testing to (carefully!) allow two clients in the same process
        internal void StartInternal()
        {
            transport.Start();
            Logger.MyIPEndPoint = transport.MyAddress.Endpoint; // transport.MyAddress is only set after transport is Started.
            CurrentActivation = ActivationAddress.NewActivationAddress(transport.MyAddress, GrainId.NewClientGrainId());

            clientStatistics = new ClientStatisticsManager(config);
            clientStatistics.Start(config, transport, clientId).WaitWithThrow(AzureTableDefaultPolicies.TableCreation_TIMEOUT);

            listeningCTS = new CancellationTokenSource();
            var ct = listeningCTS.Token;
            listenForMessages = true;

            // todo: thread spawn, stop, etc. - keeping it very simple for now
            AsyncCompletion.StartNew(
                () => RunClientMessagePump(ct)
            ).LogErrors(logger, ErrorCode.Runtime_Error_100326).Ignore();

            grainInterfaceMap = transport.GetTypeCodeMap().Result;
        }

        private void RunClientMessagePump(CancellationToken ct)
        {
            if (StatisticsCollector.CollectThreadTimeTrackingStats)
            {
                incomingMessagesThreadTimeTracking.OnStartExecution();
            }
            while (listenForMessages)
            {
                var message = transport.WaitMessage(Message.Categories.Application, ct);

                if (message == null) // if wait was cancelled
                    break;
#if TRACK_DETAILED_STATS
                        if (StatisticsCollector.CollectThreadTimeTrackingStats)
                        {
                            incomingMessagesThreadTimeTracking.OnStartProcessing();
                        }
#endif
                switch (message.Direction)
                {
                    case Message.Directions.Response:
                        {
                            ReceiveResponse(message);
                            break;
                        }
                    case Message.Directions.OneWay:
                    case Message.Directions.Request:
                        {
                            this.DispatchToLocalObject(message);
                            break;
                        }
                    default:
                        logger.Error(ErrorCode.Runtime_Error_100327, String.Format("Message not supported: {0}.", message));
                        break;
                }
#if TRACK_DETAILED_STATS
                        if (StatisticsCollector.CollectThreadTimeTrackingStats)
                        {
                            incomingMessagesThreadTimeTracking.OnStopProcessing();
                            incomingMessagesThreadTimeTracking.IncrementNumberOfProcessed();
                        }
#endif
            }
            if (StatisticsCollector.CollectThreadTimeTrackingStats)
            {
                incomingMessagesThreadTimeTracking.OnStopExecution();
            }
        }

        private void DispatchToLocalObject(Message message)
        {
            LocalObjectData objectData;
            bool found = false;
            lock (localObjects)
            {
                found = localObjects.TryGetValue(message.TargetGrain, out objectData);
            }

            if (found)
                this.InvokeLocalObjectAsync(objectData, message);
            else
            {
                logger.Error(
                    ErrorCode.ProxyClient_OGC_TargetNotFound,
                    String.Format(
                        "Unexpected target grain in request: {0}. Message={1}",
                        message.TargetGrain, 
                        message));
            }
        }

        private void InvokeLocalObjectAsync(LocalObjectData objectData, Message message)
        {
            IAddressable obj = (IAddressable)objectData.LocalObject.Target;
            if (obj == null)
            {
                //// Remove from the dictionary record for the garbage collected object? But now we won't be able to detect invalid dispatch IDs anymore.
                logger.Warn(ErrorCode.Runtime_Error_100162, 
                    String.Format("Object associated with Grain ID {0} has been garbage collected. Deleting object reference and unregistering it. Message = {1}", objectData.Grain, message));
                lock (localObjects)
                {    
                    // Try to remove. If it's not there, we don't care.
                    localObjects.Remove(objectData.Grain);
                }
                UnregisterObjectReference(objectData.Grain).Ignore();
                return;
            }

            bool start;
            lock (objectData.Messages)
            {
                objectData.Messages.Enqueue(message);
                start = !objectData.Running;
                objectData.Running = true;
            }
            if (logger.IsVerbose) logger.Verbose("InvokeLocalObjectAsync {0} start {1}", message, start);
            if (start)
            {
                // we use Task.Run() to ensure that the message pump operates asynchronously
                // with respect to the current thread. see 
                // http://channel9.msdn.com/Events/TechEd/Europe/2013/DEV-B317#fbid=aIWUq0ssW74
                // at position 54:45. 
                //
                // according to the information posted at:
                // http://stackoverflow.com/questions/12245935/is-task-factory-startnew-guaranteed-to-use-another-thread-than-the-calling-thr
                // this idiom is dependent upon the a TaskScheduler not implementing the
                // override QueueTask as task inlining (as opposed to queueing). this seems 
                // implausible to the author, since none of the .NET schedulers do this and
                // it is considered bad form (the OrleansTaskScheduler does not do this).
                //
                // if, for some reason this doesn't hold true, we can guarantee what we
                // want by passing a placeholder continuation token into Task.StartNew() 
                // instead. i.e.:
                //
                // return Task.StartNew(() => ..., new CancellationToken()); 
                Func<Task> asyncFunc =
                    async () =>
                        await this.LocalObjectMessagePumpAsync(objectData);
                Task.Run(asyncFunc).Ignore();
            }
        }

        private async Task LocalObjectMessagePumpAsync(LocalObjectData objectData)
        {
            while (true)
            {
                Message message;
                lock (objectData.Messages)
                {
                    if (objectData.Messages.Count == 0)
                    {
                        objectData.Running = false;
                        break;
                    }
                    message = objectData.Messages.Dequeue();
                }

                if (this.ExpireMessageIfExpired(message, MessagingStatisticsGroup.Phase.Invoke))
                    return;

                RequestContext.ImportFromMessage(message);
                var request = (InvokeMethodRequest)message.BodyObject;
                var targetOb = (IAddressable)objectData.LocalObject.Target;
                Task<object> resultPromiseTask = null;
                Exception caught = null;
                try
                {
                    // exceptions thrown within this scope are not considered to be thrown from user code
                    // and not from runtime code.
                    resultPromiseTask = 
                        objectData.Invoker.Invoke(
                            targetOb, 
                            request.InterfaceId, 
                            request.MethodId, 
                            request.Arguments);
                    if (resultPromiseTask != null) // it will be null for one way messages
                    {
                        await resultPromiseTask;
                    }
                }
                catch (Exception exc)
                {
                    // the exception needs to be reported in the log or propagated back to the caller.
                    caught = exc;
                }
                if (caught != null)
                    this.ReportException(message, caught);
                else if (message.Direction != Message.Directions.OneWay)
                    await this.SendResponseAsync(message, AsyncValue.FromTask(resultPromiseTask));
            }
        }

        private bool ExpireMessageIfExpired(Message message, MessagingStatisticsGroup.Phase phase)
        {
            if (message.IsExpired)
            {
                message.DropExpiredMessage(phase);
                return true;
            }
            else
                return false;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private Task 
            SendResponseAsync(
                Message message, 
                AsyncCompletion promise)
        {
            // at this point, the promise is expected to be fulfilled.
            if (!promise.IsCompleted)
                throw new ArgumentException("promise");

            if (this.ExpireMessageIfExpired(message, MessagingStatisticsGroup.Phase.Respond))
                return TaskDone.Done;

            var resultObject = promise.GetObjectValue();

            object deepCopy = null;
            try
            {
                // we're expected to notify the caller if the deep copy failed.
                deepCopy = SerializationManager.DeepCopy(resultObject);
            }
            catch (Exception exc2)
            {
                SendResponse(message, OrleansResponse.ExceptionResponse(exc2));
                this.logger.Warn(
                    ErrorCode.ProxyClient_OGC_SendResponseFailed,
                    "Exception trying to send a response.", exc2);
                return TaskDone.Done;
            }

            // the deep-copy succeeded.
            SendResponse(message, new OrleansResponse(deepCopy));
            return TaskDone.Done;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private void ReportException(Message message, Exception exception)
        {
            var request = (InvokeMethodRequest)message.BodyObject;
            switch (message.Direction)
            {
                default:
                    throw new InvalidOperationException();
                case Message.Directions.OneWay:
                    {
                        this.logger.Error(
                            ErrorCode.ProxyClient_OGC_UnhandledExceptionInOneWayInvoke,
                            String.Format(
                                "Exception during invocation of notification method {0}, interface {1}. Ignoring exception because this is a one way request.", 
                                request.MethodId, 
                                request.InterfaceId), 
                            exception);
                        break;
                    }
                case Message.Directions.Request:
                    {
                        Exception deepCopy = null;
                        try
                        {
                            // we're expected to notify the caller if the deep copy failed.
                            deepCopy = (Exception)SerializationManager.DeepCopy(exception);
                        }
                        catch (Exception ex2)
                        {
                            SendResponse(message, OrleansResponse.ExceptionResponse(ex2));
                            this.logger.Warn(
                                ErrorCode.ProxyClient_OGC_SendExceptionResponseFailed,
                                "Exception trying to send an exception response", ex2);
                            return;
                        }
                        // the deep-copy succeeded.
                        var response = OrleansResponse.ExceptionResponse(deepCopy);
                        SendResponse(message, response);
                        break;
                    }
            }
        }

        private void SendResponse(Message request, OrleansResponse response)
        {
            var message = request.CreateResponseMessage();
            message.BodyObject = response;

            transport.SendMessage(message);
        }

        /// <summary>
        /// For internal testing only.
        /// </summary>
        public void Disconnect()
        {
            transport.Disconnect();
        }

        /// <summary>
        /// For internal testing only.
        /// </summary>
        public void Reconnect()
        {
            transport.Reconnect();
        }

        #region Implementation of IGrainClient

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope",
            Justification = "CallbackData is IDisposable but instances exist beyond lifetime of this method so cannot Dispose yet.")]
        public void SendRequest(GrainReference target, InvokeMethodRequest request, TaskCompletionSource<object> context, Action<Message, TaskCompletionSource<object>> callback, string debugContext = null, InvokeMethodOptions options = InvokeMethodOptions.None, string genericType = null, PlacementStrategy placement = null)
        {
            var message = GrainClient.CreateMessage(request, options, placement);
            SendRequestMessage(target, message, context, callback, debugContext, options, genericType);
        }

        private void SendRequestMessage(GrainReference target, Message message, TaskCompletionSource<object> context, Action<Message, TaskCompletionSource<object>> callback, string debugContext = null, InvokeMethodOptions options = InvokeMethodOptions.None, string genericType = null)
        {
            GrainId targetGrainId = target.GrainId;
            var oneWay = (options & InvokeMethodOptions.OneWay) != 0;
            //message.RequestId = RequestId.NewId((options & InvokeMethodOptions.ReadOnly) != 0);
            message.SendingGrain = CurrentActivation.Grain;
            message.SendingActivation = CurrentActivation.Activation;
            message.TargetGrain = targetGrainId;
            if (!String.IsNullOrEmpty(genericType))
                message.GenericGrainType = genericType;

            if (targetGrainId.IsSystemTarget)
            {
                // If the silo isn't be supplied, it will be filled in by the sender to be the gateway silo
                message.TargetSilo = target.SystemTargetSilo;
                if (target.SystemTargetSilo != null)
                {
                    message.TargetActivation = ActivationId.GetSystemActivation(targetGrainId, target.SystemTargetSilo);
                }
            }
            //if (!message.IsUnordered)
            //{
            //    CorrelationId time;
            //    lock (lastSent)
            //    {
            //        if (lastSent.TryGetValue(message.TargetGrain, out time))
            //            message.PriorMessageId = time;
            //        lastSent[message.TargetGrain] = message.Id;
            //    }
            //}
            if (debugContext != null)
            {
                message.DebugContext = debugContext;
            }
            if (message.IsExpirableMessage(config))
            {
                // don't set expiration for system target messages.
                message.Expiration = DateTime.UtcNow + responseTimeout + Constants.MAXIMUM_CLOCK_SKEW;
            }

            if (!oneWay)
            {
                var callbackData = new CallbackData(callback, TryResendMessage, context, message, () => UnRegisterCallback(message.Id));
                lock (callbacks)
                {
                    callbacks.Add(message.Id, callbackData);
                }
                callbackData.StartTimer(responseTimeout);
            }

            if (logger.IsVerbose2) logger.Verbose2("Send {0}", message);
            transport.SendMessage(message);
        }


        private bool TryResendMessage(Message message)
        {
            if (!message.MayResend(config))
            {
                return false;
            }

            if (logger.IsVerbose) logger.Verbose("Resend {0}", message);

            message.ResendCount = message.ResendCount + 1;
            message.RemoveHeader(Message.Header.TaskHeader);
            message.SetMetadata(Message.Metadata.TARGET_HISTORY, message.GetTargetHistory());
            
            if (!message.TargetGrain.IsSystemTarget)
            {
                message.RemoveHeader(Message.Header.TargetActivation);
                message.RemoveHeader(Message.Header.TargetSilo);
            }
            transport.SendMessage(message);
            return true;
        }

        public bool ProcessOutgoingMessage(Message message)
        {
            throw new NotImplementedException();
        }

        public bool ProcessIncomingMessage(Message message)
        {
            throw new NotImplementedException();
        }

        public void ReceiveResponse(Message response)
        {
            if (logger.IsVerbose2) logger.Verbose2("Received {0}", response);

            // ignore duplicate requests
            if (response.Result == Message.ResponseTypes.Rejection && response.RejectionType == Message.RejectionTypes.DuplicateRequest)
                return;

            CallbackData callbackData;
            bool found;
            lock (callbacks)
            {
                found = callbacks.TryGetValue(response.Id, out callbackData);
            }
            if (found)
            {
                callbackData.DoCallback(response);
            }
            else
            {
                logger.Warn(ErrorCode.Runtime_Error_100011, "No callback for response message: " + response);
            }
        }

        private void UnRegisterCallback(CorrelationId id)
        {
            lock (callbacks)
            {
                callbacks.Remove(id);
            }
        }

        public void Reset()
        {
            Utils.SafeExecute(() =>
            {
                if (logger != null)
                {
                    logger.Info("OutsideGrainClient.Reset(): client GUID Id " + clientId);
                }
            });

            Utils.SafeExecute(() =>
            {
                if (StatisticsCollector.CollectThreadTimeTrackingStats)
                {
                    incomingMessagesThreadTimeTracking.OnStopExecution();
                }
            }, logger, "OrleansClient.incomingMessagesThreadTimeTracking.OnStopExecution");
            Utils.SafeExecute(() =>
            {
                if (transport != null)
                {
                    transport.PrepareToStop();
                }
            }, logger, "OrleansClient.PrepareToStop-Transport");

            listenForMessages = false;
            Utils.SafeExecute(() =>
                {
                    if (listeningCTS != null)
                    {
                        listeningCTS.Cancel();
                    }
                }, logger, "OrleansClient.Stop-ListeningCTS");
            Utils.SafeExecute(() =>
            {
                if (transport != null)
                {
                    transport.Stop();
                }
            }, logger, "OrleansClient.Stop-Transport");
            Utils.SafeExecute(() =>
            {
                if (clientStatistics != null)
                {
                    clientStatistics.Stop();
                }
            }, logger, "OrleansClient.Stop-ClientStatistics");
            ConstructorReset();
        }

        private void ConstructorReset()
        {
            Utils.SafeExecute(() =>
            {
                if (logger != null)
                {
                    logger.Info("OutsideGrainClient.ConstructorReset(): client GUID Id " + clientId);
                }
            });
            
            try
            {
                UnobservedExceptionsHandlerClass.ResetUnobservedExceptionHandler();
            }
            catch (Exception) { }
            try
            {
                Logger.UnInitialize();
            }
            catch (Exception) { }
        }

        public void SetResponseTimeout(TimeSpan timeout)
        {
            responseTimeout = timeout;
        }
        public TimeSpan GetResponseTimeout()
        {
            return responseTimeout;
        }

        public AsyncValue<IOrleansReminder> RegisterOrUpdateReminder(string reminderName, TimeSpan dueTime, TimeSpan period)
        {
            throw new InvalidOperationException("RegisterReminder can only be called from inside a grain");
        }

        public AsyncCompletion UnregisterReminder(IOrleansReminder reminder)
        {
            throw new InvalidOperationException("UnregisterReminder can only be called from inside a grain");
        }

        public AsyncValue<IOrleansReminder> GetReminder(string reminderName)
        {
            throw new InvalidOperationException("GetReminder can only be called from inside a grain");
        }
        
        public AsyncValue<List<IOrleansReminder>> GetReminders()
        {
            throw new InvalidOperationException("GetReminders can only be called from inside a grain");
        }

        public SiloStatus GetSiloStatus(SiloAddress silo)
        {
            throw new InvalidOperationException("GetSiloStatus can only be called on the silo.");
        }

        public async Task ExecAsync(Func<Task> action, ISchedulingContext context)
        {
            await Task.Run(action); // No grain context on client - run on .NET thread pool
        }

        public Task<GrainReference> CreateObjectReference(IGrainObserver obj, IGrainMethodInvoker invoker)
        {
            return this.CreateObjectReference((IAddressable)obj, invoker);
        }

        public async Task<GrainReference> CreateObjectReference(IAddressable obj, IGrainMethodInvoker invoker)
        {
            if (obj is GrainReference)
                throw new ArgumentException("Argument obj is already a grain reference.");

            GrainId target = GrainId.NewClientAddressableGrainId();
            await transport.RegisterObserver(target);
            lock (localObjects)
            {
                localObjects.Add(target, new LocalObjectData(obj, target, invoker));
            }
            return GrainReference.FromGrainId(target);
        }

        public Task DeleteObjectReference(IGrainObserver obj)
        {
            return DeleteObjectReference((IAddressable)obj);
        }

        public Task DeleteObjectReference(IAddressable obj)
        {
            if (!(obj is GrainReference))
                throw new ArgumentException("Argument reference is not a grain reference.");

            GrainReference reference = (GrainReference) obj;

            return DeleteResolvedObjectReference(reference);
        }

        private Task DeleteResolvedObjectReference(GrainReference reference)
        {
            LocalObjectData objData;

            lock (localObjects)
            {
                if (localObjects.TryGetValue(reference.GrainId, out objData))
                    localObjects.Remove(reference.GrainId);
                else
                    throw new ArgumentException("Reference is not associated with a local object.", "reference");
            }
            return UnregisterObjectReference(objData.Grain);
        }

        private async Task UnregisterObjectReference(GrainId grain)
        {
            try
            {

                await transport.UnregisterObserver(grain);
                if (logger.IsVerbose) 
                    logger.Verbose(ErrorCode.Runtime_Error_100315, "Successfully unregistered client target {0}", grain);
            }
            catch (Exception exc)
            {
                logger.Error(ErrorCode.Runtime_Error_100012, String.Format("Failed to unregister client target {0}.", grain), exc);
            }
        }

        public void DeactivateOnIdle(ActivationId id)
        {
            throw new InvalidOperationException();
        }

        #endregion

        private class LocalObjectData
        {
            internal WeakReference LocalObject { get; private set; }
            internal IGrainMethodInvoker Invoker { get; private set; }
            internal GrainId Grain { get; private set; }
            internal Queue<Message> Messages { get; private set; }
            internal bool Running { get; set; }

            internal LocalObjectData(IAddressable obj, GrainId grain, IGrainMethodInvoker invoker)
            {
                LocalObject = new WeakReference(obj);
                Grain = grain;
                Invoker = invoker;
                Messages = new Queue<Message>();
                Running = false;
            }
        }

        public void Dispose()
        {
            if (listeningCTS != null)
            {
                listeningCTS.Dispose();
                listeningCTS = null;
            }

            GC.SuppressFinalize(this);
        }


        public IGrainTypeResolver GrainTypeResolver
        {
            get { return grainInterfaceMap; }
        }

        public string CaptureRuntimeEnvironment()
        {
            throw new NotImplementedException();
        }

        public IGrainMethodInvoker GetInvoker(int interfaceId, string genericGrainType = null)
        {
            throw new NotImplementedException();
        }
    }
}
