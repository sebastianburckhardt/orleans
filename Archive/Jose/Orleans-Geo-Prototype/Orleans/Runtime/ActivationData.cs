//#define HISTORY
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Orleans.Counters;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans
{
    /// <summary>
    /// Maintains additional per-activation state that is required for Orleans internal operations.
    /// MUST lock this object for any concurrent access
    /// todo: compartmentalize by usage? e.g. using separate interfaces for data for catalog, journal, etc.
    /// </summary>
    internal class ActivationData : IInvokable
    {
        // This class is used for activations that have extension invokers. It keeps a dictionary of 
        // invoker objects to use with the activation, and extend the default invoker
        // defined for the grain class.
        // Note that in all cases we never have more than one copy of an actual invoker;
        // we may have a ExtensionInvoker per activation, in the worst case.
        private class ExtensionInvoker : IGrainMethodInvoker
        {
            // Because calls to ExtensionInvoker are allways made within the activation context,
            // we rely on the single-threading guarantee of the runtime and do not protect the map with a lock.
            private Dictionary<int, Tuple<IGrainExtension, IGrainExtensionMethodInvoker>> _extensionMap; // key is the extension interface ID
            
            /// <summary>
            /// Try to add an extension for the specific interface ID.
            /// Fail and return false if there is already an extension for that interface ID.
            /// Note that if an extension invoker handles multiple interface IDs, it can only be associated
            /// with one of those IDs when added, and so only conflicts on that one ID will be detected and prevented.
            /// </summary>
            /// <param name="interfaceId"></param>
            /// <param name="invoker"></param>
            /// <param name="handler"></param>
            /// <returns></returns>
            internal bool TryAddExtension(IGrainExtensionMethodInvoker invoker, IGrainExtension handler)
            {
                if (_extensionMap == null)
                {
                    _extensionMap = new Dictionary<int, Tuple<IGrainExtension, IGrainExtensionMethodInvoker>>(1);
                }

                if (_extensionMap.ContainsKey(invoker.InterfaceId))
                    return false;
                _extensionMap.Add(invoker.InterfaceId, new Tuple<IGrainExtension, IGrainExtensionMethodInvoker>(handler, invoker));
                return true;
            }

            /// <summary>
            /// Removes all extensions for the specified interface id.
            /// Returns true if the chained invoker no longer has any extensions and may be safely retired.
            /// </summary>
            /// <param name="interfaceId"></param>
            /// <returns>true if the chained invoker is now empty, false otherwise</returns>
            public bool Remove(IGrainExtension extension)
            {
                int interfaceId = 0;
                foreach(int iface in _extensionMap.Keys)
                    if (_extensionMap[iface].Item1 == extension)
                    {
                        interfaceId = iface;
                        break;
                    }

                if (interfaceId != 0) // found
                {
                    _extensionMap.Remove(interfaceId);
                    return _extensionMap.Count == 0;
                }
                throw new InvalidOperationException(String.Format("Extension {0} is not installed", extension.GetType().FullName));
            }

            public bool TryGetExtensionHandler(Type extensionType, out IGrainExtension result)
            {
                result = null;

                if (_extensionMap == null)
                    return false;

                foreach (var ext in _extensionMap.Values)
                    if (extensionType == ext.Item1.GetType())
                    {
                        result = ext.Item1;
                        return true;
                    }

                return false;
            }

            /// <summary>
            /// Invokes the appropriate grain or extension method for the request interface ID and method ID.
            /// First each extension invoker is tried; if no extension handles the request, then the base
            /// invoker is used to handle the request.
            /// The base invoker will throw an appropriate exception if the request is not recognized.
            /// </summary>
            /// <param name="grain"></param>
            /// <param name="interfaceId"></param>
            /// <param name="methodId"></param>
            /// <param name="arguments"></param>
            /// <returns></returns>
            public Task<object> Invoke(IAddressable grain, int interfaceId, int methodId, object[] arguments)
            {
                if (_extensionMap != null && _extensionMap.ContainsKey(interfaceId))
                {
                    IGrainExtensionMethodInvoker invoker = _extensionMap[interfaceId].Item2;
                    IGrainExtension extension = _extensionMap[interfaceId].Item1;
                    return invoker.Invoke(extension, interfaceId, methodId, arguments);
                }
                
                throw new InvalidOperationException(String.Format("Extension invoker invoked with an unknown inteface ID:{0}.", interfaceId));
            }

            public bool IsExtensionInstalled(int interfaceId)
            {
                if (_extensionMap == null)
                    return false;

                return _extensionMap.ContainsKey(interfaceId);
            }

            public int InterfaceId
            {
                get { return 0; } // 0 indicates an extension invoker that may have multiple intefaces inplemented by extensions.
            }
        }

        // This is the maximum amount of time we expect a request to continue processing
        private static TimeSpan _maxRequestProcessingTime;
        public TimeSpan CollectionAgeLimit { get; private set; }

        private IGrainMethodInvoker _lastInvoker;

        // This is the maximum number of enqueued request messages for a single activation before we write a warning log or reject new requests.
        private LimitValue MaxEnqueuedRequestsLimit;

        private readonly Logger logger;

        public static void Init(OrleansConfiguration config)
        {
            // TODO: Add a config parameter for this
            _maxRequestProcessingTime = config.Globals.ResponseTimeout.Multiply(5);
        }

        public ActivationData(ActivationAddress addr, PlacementStrategy placedUsing, IActivationCollector collector)
        {
            if (null == addr)
                throw new ArgumentNullException("addr");
            if (null == placedUsing)
                throw new ArgumentNullException("placedUsing");
            if (null == collector)
                throw new ArgumentNullException("collector");

            this.logger = Logger.GetLogger("ActivationData", Logger.LoggerType.Runtime);
            this.ResetKeepAliveRequest();
            Address = addr;
            State = ActivationState.Create;
            PlacedUsing = placedUsing;
            if (!Grain.IsSystemTarget && !Constants.IsSystemGrain(Grain))
            {
                _collector = collector;
            }
        }

        #region Method invocation

        private ExtensionInvoker _extensionInvoker;
        public IGrainMethodInvoker GetInvoker(int interfaceId, string genericGrainType=null)
        {
            if (_lastInvoker != null && interfaceId == _lastInvoker.InterfaceId) // extension invoker returns InterfaceId==0, so this condition will never be true if an extension is installed
                return _lastInvoker;
            
            if (_extensionInvoker == null || !_extensionInvoker.IsExtensionInstalled(interfaceId))
                _lastInvoker = GrainClient.InternalCurrent.GetInvoker(interfaceId, genericGrainType);
            else
                _lastInvoker = _extensionInvoker;
            
            return _lastInvoker;
        }

        internal bool TryAddExtension(IGrainExtensionMethodInvoker invoker, IGrainExtension extension)
        {
            if(_extensionInvoker == null)
                _extensionInvoker = new ExtensionInvoker();

            return _extensionInvoker.TryAddExtension(invoker, extension);
        }

        internal void RemoveExtension(IGrainExtension extension)
        {
            if (_extensionInvoker != null)
            {
                if (_extensionInvoker.Remove(extension))
                    _extensionInvoker = null;
            }
            else
                throw new InvalidOperationException("Grain extensions not installed.");
        }

        internal bool TryGetExtensionHandler(Type extensionType, out IGrainExtension result)
        {
            if (_extensionInvoker != null)
            {
                return _extensionInvoker.TryGetExtensionHandler(extensionType, out result);
            }

            result = null;
            return false;
        }

        #endregion


        private GrainBase _grainInstance;
        private Type _grainInstanceType;

        internal GrainBase GrainInstance { get { return _grainInstance;  } }
        internal Type GrainInstanceType { get { return _grainInstanceType; } }

        internal void SetGrainInstance(GrainBase grainInstance)
        {
            _grainInstance = grainInstance;
            if (grainInstance != null)
            {
                _grainInstanceType = grainInstance.GetType();
            }
        }

        /// <summary>
        /// For internal (run-time) use only.
        /// </summary>
        public IStorageProvider StorageProvider { get; set; }

        #region Catalog

        public ActivationAddress Address { get; private set; }

        /// <summary>
        /// For internal (run-time) use only.
        /// </summary>
        public SiloAddress Silo { get { return Address.Silo;  } }

        /// <summary>
        /// For internal (run-time) use only.
        /// </summary>
        public GrainId Grain { get { return Address.Grain; } }

        public ActivationId ActivationId { get { return Address.Activation; } }

        /// <summary>
        /// For internal (run-time) use only.
        /// </summary>
        public ActivationState State { get; private set; } // ActivationData

        public void SetState(ActivationState state)
        {
            this.State = state;
        }

        /// <summary>
        /// If State == Invalid, this may contain a forwarding address for incoming messages
        /// </summary>
        public ActivationAddress ForwardingAddress { get; set; }

        /// <summary>
        /// For internal (run-time) use only.
        /// </summary>
        private readonly IActivationCollector _collector;
        internal IActivationCollector ActivationCollector { get { return _collector; } }

        public DateTime CollectionTicket { get; private set; }
        private bool _collectionCancelledFlag;

        public bool TrySetCollectionCancelledFlag()
        {
            lock (this)
            {
                if (default(DateTime) != CollectionTicket && !_collectionCancelledFlag)
                {
                    _collectionCancelledFlag = true;
                    return true;
                }
                else
                {
                    return false;
                }
            }
        }
        public void SetCollectionAgeLimit(TimeSpan ageLimit)
        {
            CollectionAgeLimit = ageLimit;
        }

        public void ResetCollectionCancelledFlag()
        {
            lock (this)
            {
                _collectionCancelledFlag = false;
            }
        }

        public void ResetCollectionTicket()
        {
            CollectionTicket = default(DateTime);
        }

        public void SetCollectionTicket(DateTime ticket)
        {
            if (ticket == default(DateTime))
            {
                throw new ArgumentException("default(DateTime) is disallowed", "ticket");
            }
            if (CollectionTicket != default(DateTime))
            {
                throw new InvalidOperationException("call ResetCollectionTicket before calling SetCollectionTicket.");
            }

            CollectionTicket = ticket;
        }

        #endregion

        #region Dispatcher

        public PlacementStrategy PlacedUsing { get; private set; }

        /// <summary>
        /// For internal (run-time) use only.
        /// </summary>
        public Message Running { get; private set; }

        // list of AlwaysInterleave requests currently running on this activation.
        // For now we just put them in a list for tracking and debugging.
        private List<Message> AlwaysInterleaveRequests; 

        public void RecordRunning(Message message)
        {
            // Note: This method is always called while holding lock on this activation, so no need for additional locks here

            numStartedRunning++;
            // TODO: consider recording all msgs if this is interleaving case.
            if (Running == null)
            {
                Running = message;
                // TODO: Handle long request detection for reentrant activations -- this logic only works for non-reentrant activations
                CurrentRequestStartTime = DateTime.UtcNow;
            }
            else if (message.IsAlwaysInterleave)
            {
                // Don't over-write the current Running message with new incoming AlwaysInterleave message.
                // If we are currently executing a write message, we don't want to loose this fact.
                // When this AlwaysInterleave message is done OnActivationEndedTurn will be called.
                // If we don't have any more pending turns, we will call OnActivationCompletedRequest and reset Running to null.
                // Otherwise, we will keep Running until all pending tunrs are done.
                if (AlwaysInterleaveRequests == null)
                {
                    AlwaysInterleaveRequests = new List<Message>(1);
                }
                AlwaysInterleaveRequests.Add(message);
            }
        }

        public void ResetRunning(Message message)
        {
            // Note: This method is always called while holding lock on this activation, so no need for additional locks here

            if (message == null || message.Equals(Running))
            {
                numFinishedRunning++;
                Running = null;
                AlwaysInterleaveRequests = null;
                // TODO: Handle long request detection for reentrant activations -- this logic only works for non-reentrant activations
                CurrentRequestStartTime = DateTime.MinValue;
                _becameIdle = DateTime.UtcNow;
                if (_collector != null)
                {
                    _collector.TryRescheduleCollection(this, CollectionAgeLimit);
                }
            }
        }

        private long currentlyExecutingCount;
        private long enqueuedOnDispatcherCount;

        /// <summary>
        /// Number of messages that are actively being processed [as opposed to being in the Waiting queue].
        /// In most cases this will be 0 or 1, but for Reentrant grains can be >1.
        /// </summary>
        public long CurrentlyExecutingCount { get { return Interlocked.Read(ref currentlyExecutingCount); } }

        /// <summary>
        /// Number of messages that are being received [as opposed to being in the scheduler queue or actively processed].
        /// </summary>
        public long EnqueuedOnDispatcherCount { get { return Interlocked.Read(ref enqueuedOnDispatcherCount); } }

        /// <summary>Increment the number of in-flight messages currently being processed.</summary>
        public void IncrementInFlightCount() { Interlocked.Increment(ref currentlyExecutingCount); }
        
        /// <summary>Decrement the number of in-flight messages currently being processed.</summary>
        public void DecrementInFlightCount() { Interlocked.Decrement(ref currentlyExecutingCount); }

        /// <summary>Increment the number of messages currently in the prcess of being received.</summary>
        public void IncrementEnqueuedOnDispatcherCount() { Interlocked.Increment(ref enqueuedOnDispatcherCount); }

        /// <summary>Decrement the number of messages currently in the prcess of being received.</summary>
        public void DecrementEnqueuedOnDispatcherCount() { Interlocked.Decrement(ref enqueuedOnDispatcherCount); }

        private int numStartedRunning;
        private int numFinishedRunning;

        /// <summary>
        /// For internal (run-time) use only.
        /// grouped by sending activation: responses first, then sorted by id
        /// </summary>
        private List<Message> waiting;

        public int WaitingCount 
        { 
            get 
            {
                if (waiting == null) return 0;
                return waiting.Count;  
            } 
        }

        /// <summary>
        /// For internal (run-time) use only.
        /// </summary>
        public List<Action> OnInactive { get; set; } // ActivationData

        public bool IsInactive
        {
            get
            {
                return (State == ActivationState.Inactive) &&
                    Running == null &&
                    (waiting==null || waiting.Count == 0);
            }
        }

        /// <summary>
        /// Returns whether this activation is eligible for collection.
        /// </summary>
        public bool IsCollectionCandidate { get { return _collector != null && IsInactive && !ShouldBeKeptAlive; } } 

        /// <summary>
        /// Returns how long this activation has been idle.
        /// </summary>
        public TimeSpan GetIdleness(DateTime now)
        {
            if (now == default(DateTime))
            {
                throw new ArgumentException("default(DateTime) is not allowed; Use DateTime.UtcNow instead.", "now");
            }

            return now - _becameIdle;
        }

        /// <summary>
        /// Returns whether this activation has been idle long enough to be collected.
        /// </summary>
        public bool IsStale(DateTime now)
        {
            TimeSpan idleness = GetIdleness(now);
            return CollectionAgeLimit <= idleness;
        }

        public bool IsUsable
        {
            get
            {
                if (State == ActivationState.Create) return false;
                if (State == ActivationState.Activating) return false;
                if (State == ActivationState.Deactivating) return false;
                if (State == ActivationState.Invalid) return false;
                return true;
            }
        }

        private DateTime CurrentRequestStartTime;
        private DateTime _becameIdle;

        /// <summary>
        /// Insert in a FIFO order
        /// </summary>
        /// <param name="message"></param>
        public bool EnqueueMessage(Message message)
        {
            lock (this)
            {
                if (State == ActivationState.Invalid)
                {
                    logger.Warn(ErrorCode.Dispatcher_InvalidActivation,
                        "Cannot enqueue message to invalid actiation {0} : {1}", this.ToDetailedString(), message);
                    return false;
                }
                //logger.Info(0, "EnqueueMessage");
                // If maxRequestProcessingTime is never set, then we will skip this check
                if (_maxRequestProcessingTime.TotalMilliseconds > 0 && Running != null)
                {
                    // TODO: Handle long request detection for reentrant activations -- this logic only works for non-reentrant activations
                    TimeSpan currentRequestActiveTime = DateTime.UtcNow - CurrentRequestStartTime;
                    if (currentRequestActiveTime > _maxRequestProcessingTime)
                    {
                        logger.Warn(ErrorCode.Dispatcher_ExtendedMessageProcessing,
                             "Current request has been active for {0} for activation {1}: {2}", currentRequestActiveTime, this.ToDetailedString(), Running);
                    }
                }

                if (waiting == null)
                {
                    waiting = new List<Message>();
                }
                waiting.Add(message);
                return true;
            }
        }
 
        /// <summary>
        /// Check whether this activation is overloaded. 
        /// Returns OrleansLimitExceededException if overloaded, otherwise <c>null</c>c>
        /// </summary>
        /// <param name="log">Logger to use for reporting any overflow condition</param>
        /// <returns>Returns OrleansLimitExceededException if overloaded, otherwise <c>null</c>c></returns>
        public OrleansLimitExceededException CheckOverloaded(Logger log)
        {
            LimitValue limitValue = GetMaxEnqueuedRequestLimit();

            int maxRequestsHardLimit = limitValue.HardLimitThreshold;
            int maxRequestsSoftLimit = limitValue.SoftLimitThreshold;

            if (maxRequestsHardLimit <= 0 && maxRequestsSoftLimit <= 0) return null; // No limits are set

            int count = GetRequestCount();

            if (maxRequestsHardLimit > 0 && count > maxRequestsHardLimit) // Hard limit
            {
                log.Warn(ErrorCode.Catalog_Reject_ActivationTooManyRequests, 
                    String.Format("Overload - {0} enqueued requests for activation {1}, exceeding hard limit rejection threshold of {2}",
                        count, this, maxRequestsHardLimit));

                return new OrleansLimitExceededException(limitValue.Name, count, maxRequestsHardLimit, this.ToString());
            }
            else if (maxRequestsSoftLimit > 0 && count > maxRequestsSoftLimit) // Soft limit
            {
                log.Warn(ErrorCode.Catalog_Warn_ActivationTooManyRequests,
                    String.Format("Hot - {0} enqueued requests for activation {1}, exceeding soft limit warning threshold of {2}",
                        count, this, maxRequestsSoftLimit));
                return null;
            }

            return null;
        }

        internal int GetRequestCount()
        {
            lock (this)
            {
                long numInDispatcher = EnqueuedOnDispatcherCount;
                long numActive = CurrentlyExecutingCount;
                long numWaiting = WaitingCount;
                return (int)(numInDispatcher + numActive + numWaiting);
            }
        }

        private LimitValue GetMaxEnqueuedRequestLimit()
        {
            if (MaxEnqueuedRequestsLimit != null)
            {
                return MaxEnqueuedRequestsLimit;
            }
            else if (GrainInstanceType != null)
            {
                string limitName = GrainClientGenerator.GrainInterfaceData.IsStatelessWorker(GrainInstanceType)
                                       ? LimitNames.Limit_MaxEnqueuedRequests_StatelessWorker
                                       : LimitNames.Limit_MaxEnqueuedRequests;
                MaxEnqueuedRequestsLimit = LimitManager.GetLimit(limitName); // Cache for next time
                return MaxEnqueuedRequestsLimit;
            }
            else
            {
                return LimitManager.GetLimit(LimitNames.Limit_MaxEnqueuedRequests);
            }
        }

        public Message PeekNextWaitingMessage()
        {
            if (waiting != null && waiting.Count > 0)
                return waiting[0];
            return null;
        }

        public void DequeueNextWaitingMessage()
        {
            if (waiting != null && waiting.Count > 0)
                waiting.RemoveAt(0);
        }

        /// <summary>
        /// For internal (run-time) use only.
        /// </summary>
        public void AddOnInactive(Action action) // ActivationData
        {
            lock (this)
            {
                if (OnInactive == null)
                {
                    OnInactive = new List<Action>();
                }
                OnInactive.Add(action);
            }
        }

        public void RunOnInactive()
        {
            lock (this)
            {
                if (OnInactive != null)
                {
                    var actions = OnInactive;
                    OnInactive = null;
                    foreach (var action in actions)
                    {
                        action();
                    }
                }
            }
        }

        internal void ReroutePending(Action<Message> reroute)
        {
            lock (this)
            {
                if (waiting == null) return;
                foreach (var message in waiting)
                {
                    MessagingProcessingStatisticsGroup.OnDispatcherMessageReRouted(message);

                    message.RemoveHeader(Message.Header.ReroutingRequested);
                    message.SetMetadata(Message.Metadata.TARGET_HISTORY, message.GetTargetHistory());
                    message.RemoveHeader(Message.Header.TargetActivation);
                    message.RemoveHeader(Message.Header.TargetSilo);

                    reroute(message);
                }
                waiting.Clear();
            }
        }
        /// <summary>
        /// If non-null, this activation is in a shutdown state and should
        /// not accept new transactions. Resolve when all transactions
        /// are committed or aborted.
        /// </summary>
        internal AsyncCompletionResolver Shutdown { get; set; }
        #endregion
        
        #region Garbage collection (timers)

        public void DelayDeactivation(TimeSpan timespan)
        {
            if (timespan <= TimeSpan.Zero)
            {
                // reset any current keepAliveUntill
                ResetKeepAliveRequest();
            }
            else if (timespan == TimeSpan.MaxValue)
            {
                // otherwise creates negative time.
                keepAliveUntil = DateTime.MaxValue;
            }
            else
            {
                keepAliveUntil = DateTime.UtcNow + timespan;
            }
        }

        public void ResetKeepAliveRequest()
        {
            keepAliveUntil = DateTime.MinValue;
        }

        private DateTime keepAliveUntil;

        public bool ShouldBeKeptAlive { get { return keepAliveUntil >= DateTime.UtcNow; } }

        #endregion

        public string DumpStatus()
        {
            var sb = new StringBuilder();
            lock (this)
            {
                sb.AppendFormat("   {0}", ToDetailedString());
                if (Running != null)
                {
                    sb.AppendFormat("   Processing message: {0}", Running);
                }
                if (AlwaysInterleaveRequests != null)
                {
                    sb.AppendFormat("   Processing {0} AlwaysInterleaveRequests", AlwaysInterleaveRequests.Count);
                }
                if (waiting!=null && waiting.Count > 0)
                {
                    sb.AppendFormat("   Messages queued within ActivationData: {0}", PrintWaitingQueue());
                }
            }
            return sb.ToString();
        }

        /// <summary>
        /// For internal (run-time) use only.
        /// </summary>
        public override string ToString()
        {
            return String.Format("[Activation: {0}{1}{2}{3} State={4}]",
                 Silo,
                 Grain,
                 ActivationId,
                 GetActivationInfoString(),
                 State);
        }

        internal string ToDetailedString()
        {
            return String.Format("[Activation: {0}{1}{2}{3} State={4} NonReentrancyQueueSize={5} EnqueuedOnDispatcher={6} CurrentlyExecutingCount={7} NumStartedRunning={8} NumFinishedRunning={9}]",
                 Silo.ToLongString(),
                 Grain.ToDetailedString(),
                 ActivationId,
                 GetActivationInfoString(),
                 State,                         // 4
                 WaitingCount,                  // 5 NonReentrancyQueueSize
                 EnqueuedOnDispatcherCount,     // 6 EnqueuedOnDispatcher
                 CurrentlyExecutingCount,       // 7 CurrentlyExecutingCount
                 numStartedRunning,             // 8 NumStartedRunning
                 numFinishedRunning);           // 9 NumFinishedRunning
        }

        /// <summary>
        /// For internal (run-time) use only.
        /// </summary>
        public string Name
        {
            get
            {
                return String.Format("[Activation: {0}{1}{2}{3}]",
                     Silo,
                     Grain,
                     ActivationId,
                     GetActivationInfoString());
            }
        }

        /// <summary>
        /// Return string containing dump of the queue of waiting work items
        /// </summary>
        /// <returns></returns>
        /// <remarks>Note: Caller must be holding lock on this activation while calling this method.</remarks>
        internal string PrintWaitingQueue()
        {
            return Utils.IEnumerableToString(waiting);
        }

        private string GetActivationInfoString()
        {
            if (GrainInstanceType == null)
                return string.Empty;
            else
                return String.Format(" #GrainType={0}", GrainInstanceType.FullName);
        }
    }

    /// <summary>
    /// For internal (run-time) use only.
    /// </summary>
    internal enum ActivationState
    {
        /// <summary>
        /// For internal (run-time) use only.
        /// being created
        /// </summary>
        Create,
        ///// <summary>
        ///// For internal (run-time) use only.
        ///// Activation is in the middle of activation process.
        ///// </summary>
        Activating,
        /// <summary>
        /// For internal (run-time) use only.
        /// not in an active task
        /// </summary>
        Inactive,
        ///// <summary>
        ///// For internal (run-time) use only.
        ///// Activation is in the middle of deactivation process.
        ///// </summary>
        Deactivating,
        /// <summary>
        /// Tombstone for activation that was unable to be properly created
        /// </summary>
        Invalid,
    }
}
