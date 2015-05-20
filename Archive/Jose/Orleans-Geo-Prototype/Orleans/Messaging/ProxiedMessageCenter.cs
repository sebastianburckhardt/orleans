using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Orleans.Counters;

namespace Orleans.Messaging
{
    // <summary>
    // For internal use only.
    // This class is used on the client only.
    // It provides the client counterpart to the Gateway and GatewayAcceptor classes on the silo side.
    // 
    // There is one ProxiedMessageCenter instance per OutsideGrainClient. There can be multiple ProxiedMessageCenter instances
    // in a single process, but because GrainClient keeps a static pointer to a single OutsideGrainClient instance, this is not
    // generally done in practice.
    // 
    // Each ProxiedMessageCenter keeps a collection of GatewayConnection instances. Each of these represents a bidirectional connection
    // to a single gateway endpoint. Requests are assigned to a specific connection based on the target grain ID, so that requests to
    // the same grain will go to the same gateway, in sending order. To do this efficiently and scalably, we bucket grains together
    // based on their hash code mod a reasonably large number (currently 8192).
    // 
    // When the first message is sent to a bucket, we assign a gateway to that bucket, selecting in round-robin fashion from the known
    // gateways. If this is the first message to be sent to the gateway, we will create a new connection for it and assign the bucket to
    // the new connection. Either way, all messages to grains in that bucket will be sent to the assigned connection as long as the
    // connection is live.
    // 
    // Connections stay live as long as possible. If a socket error or other communications error occurs, then the client will try to 
    // reconnect twice before giving up on the gateway. If the connection cannot be re-established, then the gateway is deemed (temporarily)
    // dead, and any buckets assigned to the connection are unassigned (so that the next message sent will cause a new gateway to be selected).
    // There is no assumption that this death is permanent; the system will try to reuse the gateway every 5 minutes.
    // 
    // The list of known gateways is managed by the GatewayManager class. See comments there for details...
    // =======================================================================================================================================
    // Locking and lock protocol:
    // The ProxiedMessageCenter instance itself may be accessed by many client threads simultaneously, and each GatewayConnection instance
    // is accessed by its own thread, by the thread for its Receiver, and potentially by client threads from within the ProxiedMessageCenter.
    // Thus, we need locks to protect the various data structured from concurrent modifications.
    // 
    // Each GatewayConnection instance has a "lockable" field that is used to lock local information. This lock is used by both the GatewayConnection
    // thread and the Receiver thread.
    // 
    // The ProxiedMessageCenter instance also has a "lockable" field. This lock is used by any client thread running methods within the instance.
    // 
    // Note that we take care to ensure that client threads never need locked access to GatewayConnection state and GatewayConnection threads never need
    // locked access to ProxiedMessageCenter state. Thus, we don't need to worry about lock ordering across these objects.
    // 
    // Finally, the GatewayManager instance within the ProxiedMessageCenter has two collections, knownGateways and knownDead, that it needs to
    // protect with locks. Rather than using a "lockable" field, each collection is lcoked to protect the collection.
    // All sorts of threads can run within the GatewayManager, including client threads and GatewayConnection threads, so we need to
    // be careful about locks here. The protocol we use is to always take GatewayManager locks last, to only take them within GatewayManager methods,
    // and to always release them before returning from the method. In addition, we never simultaneously hold the knownGateways and knownDead locks,
    // so there's no need to worry about the order in which we take and release those locks.
    // </summary>

    #region Utility classes
    /// <summary>
    /// The GatewayManager class holds the list of known gateways, as well as maintaining the list of "dead" gateways.
    /// 
    /// The known list can come from one of two places: the full list may appear in the client configuration object, or 
    /// the config object may contain an IGatewayListProvider delegate. If both appear, then the delegate takes priority.
    /// </summary>
    internal class GatewayManager : IGatewayListListener
    {
        internal readonly IGatewayListProvider listProvider;
        private SafeTimer gwRefreshTimer;
        private readonly Dictionary<IPEndPoint, DateTime> knownDead;
        private List<IPEndPoint> cachedLiveGateways;
        private DateTime lastRefreshTime;
        private int roundRobinCounter;
        private readonly SafeRandom rand;
        private readonly Logger logger;
        private readonly object lockable;
        private readonly ClientConfiguration config;
        private bool gwRefreshCallInitiated;

        public GatewayManager(ClientConfiguration cfg, IGatewayListProvider gwListProvider)
        {
            config = cfg;
            knownDead = new Dictionary<IPEndPoint, DateTime>();
            rand = new SafeRandom();
            logger = Logger.GetLogger("Messaging.GatewayManager", Logger.LoggerType.Runtime);
            lockable = new object();
            gwRefreshCallInitiated = false;

            listProvider = gwListProvider;
            List<IPEndPoint> knownGateways = listProvider.GetGateways().ToList();

            if (knownGateways == null || !knownGateways.Any())
            {
                string gwProviderType = gwListProvider.GetType().FullName;
                string err = String.Format("Could not find any gateway in {0}. Orleans client cannot initialize.", gwProviderType);
                logger.Error(ErrorCode.GatewayManager_NoGateways, err);
                throw new OrleansException(err);
            }
            else
            {
                logger.Info(ErrorCode.GatewayManager_FoundKnownGateways, "Found {0} knownGateways from GW listProvider {1}", knownGateways.Count, Utils.IEnumerableToString(knownGateways));
            }

            if (listProvider is IGatewayListObserverable)
            {
                ((IGatewayListObserverable)listProvider).SubscribeToGatewayNotificationEvents(this);
            }

            if (cfg.PreferedGatewayIndex >= 0)
            {
                roundRobinCounter = cfg.PreferedGatewayIndex;
            }
            else
            {
                roundRobinCounter = rand.Next(knownGateways.Count);
            }
            cachedLiveGateways = knownGateways;
            lastRefreshTime = DateTime.UtcNow;
            if(listProvider.IsUpdatable)
            {
                gwRefreshTimer = new SafeTimer(RefreshSnapshotLiveGateways_TimerCallback, null, config.GatewayListRefreshPeriod, config.GatewayListRefreshPeriod);
            }
        }

        public void Stop()
        {
            if (gwRefreshTimer != null)
            {
                Utils.SafeExecute(gwRefreshTimer.Dispose, logger);
            }
            gwRefreshTimer = null;

            if (listProvider!=null && listProvider is IGatewayListObserverable)
            {
                Utils.SafeExecute(
                    () => ((IGatewayListObserverable)listProvider).UnSubscribeFromGatewayNotificationEvents(this),
                    logger);
            }
        }

        public void MarkAsDead(IPEndPoint gateway)
        {
            lock (lockable)
            {
                knownDead[gateway] = DateTime.UtcNow;
                List<IPEndPoint> copy = cachedLiveGateways.ToList();
                copy.Remove(gateway);
                // swap the reference, don't mutate cachedLiveGateways, so we can access cachedLiveGateways without the lock.
                cachedLiveGateways = copy;
            }
        }

        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append("GatewayManager: ");
            lock (lockable)
            {
                if (cachedLiveGateways != null)
                {
                    sb.Append(cachedLiveGateways.Count);
                    sb.Append(" cachedLiveGateways, ");
                }
                if (knownDead != null)
                {
                    sb.Append(knownDead.Count);
                    sb.Append(" known dead gateways.");
                }
            }
            return sb.ToString();
        }

        /// <summary>
        /// Selects a gateway to use for a new bucket. 
        /// 
        /// Note that if a list provider delegate was given, the delegate is invoked every time this method is called. 
        /// This method performs caching to avoid hammering the ultimate data source.
        /// 
        /// This implementation does a simple round robin selection. It assumes that the gateway list from the provider
        /// is in the same order every time.
        /// </summary>
        /// <returns></returns>
        public IPEndPoint GetLiveGateway()
        {
            List<IPEndPoint> live = GetLiveGateways();
            int count = live.Count;
            if (count > 0)
            {
                lock (lockable)
                {
                    // Round-robin through the known gateways and take the next live one, starting from where we last left off
                    roundRobinCounter = (roundRobinCounter + 1) % count;
                    return live[roundRobinCounter];
                }
            }
            // If we drop through, then all of the known gateways are presumed dead
            return null;
        }

        public List<IPEndPoint> GetLiveGateways()
        {
            // Never takes a lock and returns the cachedLiveGateways list quickly without any operation.
            // Asynchronously starts gw refresh only when it is empty.
            if (!cachedLiveGateways.Any())
            {
                Expedite_UpdateLiveGatewaysSnapshot();   
            }
            return cachedLiveGateways;
        }

        private void Expedite_UpdateLiveGatewaysSnapshot()
        {
            // If there is already an expedited refresh call in place, don't call again, until the previous one is finished.
            // We don't want to issue too many GW refresh calls.
            if (listProvider != null && listProvider.IsUpdatable && !gwRefreshCallInitiated)
            {
                // Initiate gw list refresh asynchronously. The Refresh timer will keep ticking regardless.
                // We don't want to block the client with synchronously Refresh call.
                // Client's call will fail with "No GWs found" but we will try to refresh the list quickly.
                gwRefreshCallInitiated = true;
                AsyncCompletion ac = AsyncCompletion.StartNew(() =>
                    {
                        RefreshSnapshotLiveGateways_TimerCallback(null);
                        gwRefreshCallInitiated = false;
                    });
                ac.Ignore();
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public void GatewayListNotification(List<IPEndPoint> gateways)
        {
            try
            {
                UpdateLiveGatewaysSnapshot(gateways, listProvider.MaxStaleness);
            }
            catch (Exception exc)
            {
                logger.Error(ErrorCode.ProxyClient_GetGateways, "Exception occurred during GatewayListNotification -> UpdateLiveGatewaysSnapshot", exc);
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private void RefreshSnapshotLiveGateways_TimerCallback(object context)
        {
            try
            {
                if (listProvider != null && listProvider.IsUpdatable)
                {
                    // the listProvider.GetGateways() is not under lock.
                    List<IPEndPoint> currentKnownGWs = listProvider.GetGateways().ToList();
                    if (logger.IsVerbose) logger.Verbose("Found {0} knownGateways from GW listProvider {1}", currentKnownGWs.Count, Utils.IEnumerableToString(currentKnownGWs));
                    // the next one will grab the lock.
                    UpdateLiveGatewaysSnapshot(currentKnownGWs, listProvider.MaxStaleness);
                }
            }
            catch (Exception exc)
            {
                logger.Error(ErrorCode.ProxyClient_GetGateways, "Exception occurred during RefreshSnapshotLiveGateways_TimerCallback -> listProvider.GetGateways()", exc);
            }
        }

        // This function is called asynchronously from gw refresh timer.
        private void UpdateLiveGatewaysSnapshot(List<IPEndPoint> currentKnownGWs, TimeSpan maxStaleness)
        {
            // this is a short lock, protecting the access to knownDead and cachedLiveGateways.
            lock (lockable)
            {
                List<IPEndPoint> live = new List<IPEndPoint>();
                // now take whatever listProvider gave us and exclude those we think are dead.
                foreach (var trial in currentKnownGWs)
                {
                    DateTime diedAt;
                    // We consider a node to be dead if we recorded it is dead due to socket error
                    // and it was recorded (diedAt) not too long ago (less than maxStaleness ago). 
                    // The latter is to cover the case when the GW provider returns an outdated list that does not yet reflect the actually recently died GW.
                    // If it has passed more than maxStaleness - we assume maxStaleness is the upper bound on GW provider freshness.
                    bool isDead = knownDead.TryGetValue(trial, out diedAt) && DateTime.UtcNow.Subtract(diedAt) < maxStaleness; //  config.GatewayListRefreshPeriod;
                    if (!isDead)
                    {
                        live.Add(trial);
                    }
                }
                // swap cachedLiveGateways pointer in one atomic operation
                cachedLiveGateways = live;
                DateTime prevRefresh = lastRefreshTime;
                lastRefreshTime = DateTime.UtcNow;
                logger.Info(ErrorCode.GatewayManager_FoundKnownGateways, 
                        "Refreshed the live GateWay list. Found {0} gws from GW listProvider: {1}. Picked only known live out of them. Now has {2} live GWs: {3}. Previous refresh time was = {4}",
                        currentKnownGWs.Count, 
                        Utils.IEnumerableToString(currentKnownGWs), 
                        cachedLiveGateways.Count, 
                        Utils.IEnumerableToString(cachedLiveGateways),
                        prevRefresh);
            }
        }
    }

    #endregion

    internal class ProxiedMessageCenter : IMessageCenter
    {
        #region Constants

        internal static readonly TimeSpan MINIMUM_INTERCONNECT_DELAY = TimeSpan.FromMilliseconds(100);   // wait one tenth of a second between connect attempts
        internal const int CONNECT_RETRY_COUNT = 2;                                                      // Retry twice before giving up on a gateway server

        #endregion

        internal Guid Id { get; private set; }
        internal bool Running { get; private set; }

        internal readonly GatewayManager gatewayManager;
        internal readonly OrleansRuntimeQueue<Message> pendingInboundMessages;
        private readonly MethodInfo _registrarGetSystemTarget;
        private readonly MethodInfo _typeManagerGetSystemTarget;
        private readonly Dictionary<IPEndPoint, GatewayConnection> gatewayConnections;
        private int numMessages;
        private readonly HashSet<GrainId> registeredLocalObjects;
        private readonly int generation;
        //private readonly uint GRAIN_BUCKET_COUNT = 4;// (uint)Math.Pow(2, 13);
        // The grainBuckets array is used to select the connection to use when sending an ordered message to a grain.
        // Requests are bucketed by GrainID, so that all requests to a grain get routed through the same bucket.
        // Each bucket holds a (possibly null) weak reference to a GatewayConnection object. That connection instance is used
        // if the WeakReference is non-null, is alive, and points to a live gateway connection. If any of these conditions is
        // false, then a new gateway is selected using the gateway manager, and a new connection established if necessary.
        private readonly WeakReference[] grainBuckets;
        private readonly SafeRandom rand;
        private readonly Logger logger;
        private readonly object lockable;
        public SiloAddress MyAddress { get; private set; }
        public IMessagingConfiguration MessagingConfiguration { get; private set; }
        private QueueTrackingStatistic queueTracking;

        /// <summary>
        /// For internal use only.
        /// </summary>
        /// <param name="config"></param>
        ///
        public ProxiedMessageCenter(ClientConfiguration config, IPAddress localAddress, int gen, Guid clientId, IGatewayListProvider gwListProvider)
        {
            lockable = new object();
            this.generation = gen;
            this.MyAddress = SiloAddress.New(new IPEndPoint(localAddress, 0), gen);
            Id = clientId;
            Running = false;
            MessagingConfiguration = config;
            gatewayManager = new GatewayManager(config, gwListProvider);
            pendingInboundMessages = new OrleansRuntimeQueue<Message>();
            _registrarGetSystemTarget = OrleansClient.GetStaticMethodThroughReflection("Orleans", "Orleans.ClientObserverRegistrarFactory", "GetSystemTarget", null);
            _typeManagerGetSystemTarget = OrleansClient.GetStaticMethodThroughReflection("Orleans", "Orleans.TypeManagerFactory", "GetSystemTarget", null);
            gatewayConnections = new Dictionary<IPEndPoint, GatewayConnection>();
            numMessages = 0;
            registeredLocalObjects = new HashSet<GrainId>();
            grainBuckets = new WeakReference[config.ClientSenderBuckets];
            rand = new SafeRandom();
            logger = Logger.GetLogger("Messaging.ProxiedMessageCenter", Logger.LoggerType.Runtime);
            if (logger.IsVerbose) logger.Verbose("Proxy grain client constructed");
            IntValueStatistic.FindOrCreate(StatNames.STAT_CLIENT_CONNECTED_GW_COUNT, () =>
                {
                    lock (gatewayConnections)
                    {
                        return gatewayConnections.Values.Where(conn => conn.IsLive).Count();
                    }
                });
            if (StatisticsCollector.CollectQueueStats)
            {
                queueTracking = new QueueTrackingStatistic("ClientReceiver");
            }
        }

        /// <summary>
        /// For internal use only.
        /// </summary>
        public void Start()
        {
            Running = true;
            if (StatisticsCollector.CollectQueueStats)
            {
                queueTracking.OnStartExecution();
            }
            if (logger.IsVerbose) logger.Verbose("Proxy grain client started");
        }

        /// <summary>
        /// For internal use only.
        /// </summary>
        public void PrepareToStop()
        {
            var results = new List<Task>();
            List<GrainId> observers = registeredLocalObjects.AsList();
            foreach (var observer in observers)
            {
                var promise = UnregisterObserver(observer);
                results.Add(promise);
                promise.Ignore(); // Avoids some funky end-of-process race conditions
            }
            Utils.SafeExecute(() =>
            {
                bool ok = Task.WhenAll(results).Wait(TimeSpan.FromSeconds(5));
                if (!ok) throw new TimeoutException("Unregistering Observers");
            }, logger, "Unregistering Observers");
        }

        /// <summary>
        /// For internal use only.
        /// </summary>
        public void Stop()
        {
            Running = false;

            if (StatisticsCollector.CollectQueueStats)
            {
                queueTracking.OnStopExecution();
            }
            gatewayManager.Stop();

            foreach (var gateway in gatewayConnections.Values)
            {
                gateway.Stop();
            }
        }

        /// <summary>
        /// For internal use only.
        /// </summary>
        /// <param name="msg"></param>
        public void SendMessage(Message msg)
        {
            GatewayConnection gatewayConnection = null;
            bool startRequired = false;

            // If there's a specific gateway specified, use it
            if (msg.TargetSilo != null)
            {
                IPEndPoint addr = msg.TargetSilo.Endpoint;
                lock (lockable)
                {
                    if (!gatewayConnections.TryGetValue(addr, out gatewayConnection) || !gatewayConnection.IsLive)
                    {
                        gatewayConnection = new GatewayConnection(addr, this);
                        gatewayConnections[addr] = gatewayConnection;
                        if (logger.IsVerbose) logger.Verbose("Creating gateway to {0} for pre-addressed message", addr);
                        startRequired = true;
                    }
                }
            }
                // For untargeted messages to system targets, and for unordered messages, pick a next connection in round robin fashion.
            else if (msg.TargetGrain.IsSystemTarget || msg.IsUnordered)
            {
                // Get the cached list of live gateways.
                // Pick a next gateway name in a round robin fashion.
                // See if we have a live connection to it.
                // If Yes, use it.
                // If not, create a new GatewayConnection and start it.
                // If start fails, we will mark this connection as dead and remove it from the GetCachedLiveGatewayNames.
                lock (lockable)
                {
                    int msgNumber = numMessages;
                    numMessages = unchecked(numMessages + 1);
                    List<IPEndPoint> gatewayNames = gatewayManager.GetLiveGateways();
                    int numGWs = gatewayNames.Count;
                    if (numGWs == 0)
                    {
                        RejectMessage(msg, "No gateways available");
                        logger.Warn(ErrorCode.ProxyClient_CannotSend, "Unable to send message {0}; gateway manager state is {1}", msg, gatewayManager);
                        return;
                    }
                    IPEndPoint addr = gatewayNames[msgNumber % numGWs];
                    if (!gatewayConnections.TryGetValue(addr, out gatewayConnection) || !gatewayConnection.IsLive)
                    {
                        gatewayConnection = new GatewayConnection(addr, this);
                        gatewayConnections[addr] = gatewayConnection;
                        if (logger.IsVerbose) logger.Verbose(ErrorCode.ProxyClient_CreatedGwUnordered, "Creating gateway to {0} for unordered message to grain {1}", addr, msg.TargetGrain);
                        startRequired = true;
                    }
                    // else - Fast path - we've got a live gatewayConnection to use
                }
            }
                // Otherwise, use the buckets to ensure ordering.
            else
            {
                var index = msg.TargetGrain.GetHashCode_Modulo((uint)grainBuckets.Length);
                lock (lockable)
                {
                    // Repeated from above, at the declaration of the grainBuckets array:
                    // Requests are bucketed by GrainID, so that all requests to a grain get routed through the same bucket.
                    // Each bucket holds a (possibly null) weak reference to a GatewayConnection object. That connection instance is used
                    // if the WeakReference is non-null, is alive, and points to a live gateway connection. If any of these conditions is
                    // false, then a new gateway is selected using the gateway manager, and a new connection established if necessary.
                    var weakRef = grainBuckets[index];
                    if ((weakRef != null) && weakRef.IsAlive)
                    {
                        gatewayConnection = weakRef.Target as GatewayConnection;
                    }
                    if ((gatewayConnection == null) || !gatewayConnection.IsLive)
                    {
                        var addr = gatewayManager.GetLiveGateway();
                        if (addr == null)
                        {
                            RejectMessage(msg, "No gateways available");
                            logger.Warn(ErrorCode.ProxyClient_CannotSend_NoGateway, "Unable to send message {0}; gateway manager state is {1}", msg, gatewayManager);
                            return;
                        }
                        if (logger.IsVerbose2) logger.Verbose2(ErrorCode.ProxyClient_NewBucketIndex, "Starting new bucket index {0} for ordered messages to grain {1}", index, msg.TargetGrain);
                        if (!gatewayConnections.TryGetValue(addr, out gatewayConnection) || !gatewayConnection.IsLive)
                        {
                            gatewayConnection = new GatewayConnection(addr, this);
                            gatewayConnections[addr] = gatewayConnection;
                            if (logger.IsVerbose) logger.Verbose(ErrorCode.ProxyClient_CreatedGwToGrain, "Creating gateway to {0} for message to grain {1}, bucket {2}, grain id hash code {3}X", addr, msg.TargetGrain, index,
                                               msg.TargetGrain.GetHashCode().ToString("x"));
                            startRequired = true;
                        }
                        grainBuckets[index] = new WeakReference(gatewayConnection);
                    }
                }
            }

            if (startRequired)
            {
                gatewayConnection.Start();

                if (gatewayConnection.IsLive)
                {
                    // Register existing client observers with the new gateway
                    List<GrainId> localObjects;
                    lock (lockable)
                    {
                        localObjects = registeredLocalObjects.ToList();
                    }

                    var registrar = GetRegistrar(gatewayConnection.Silo);
                    foreach (var obj in localObjects)
                    {
                        registrar.RegisterClientObserver(obj, Id).Ignore();
                    }
                }
                else
                {
                    // if failed to start GW connection (failed to connect), try sending this msg to another GW.
                    RejectOrResend(msg);
                    return;
                }
            }

            try
            {
                gatewayConnection.QueueRequest(msg);
                if (logger.IsVerbose2) logger.Verbose2(ErrorCode.ProxyClient_QueueRequest, "Sending message {0} via gateway {1}", msg, gatewayConnection.Address);
            }
            catch (InvalidOperationException)
            {
                // This exception can be thrown if the gateway connection we selected was closed since we checked (i.e., we lost the race)
                // If this happens, we reject if the message is targeted to a specific silo, or try again if not
                RejectOrResend(msg);
            }
        }

        private void RejectOrResend(Message msg)
        {
            if (msg.TargetSilo != null)
            {
                RejectMessage(msg, "Target silo is unavailable");
            }
            else
            {
                SendMessage(msg);
            }
        }

        public async Task RegisterObserver(GrainId grainId)
        {
            List<GatewayConnection> connections;
            lock (lockable)
            {
                connections = gatewayConnections.Values.Where(conn => conn.IsLive).ToList();
                registeredLocalObjects.Add(grainId);
            }

            if (connections.Count <= 0)
            {
                return;
            }

            List<Task<ActivationAddress>> tasks = new List<Task<ActivationAddress>>();
            foreach (var connection in connections)
            {
                Task<ActivationAddress> t = GetRegistrar(connection.Silo).RegisterClientObserver(grainId, Id);
                tasks.Add(t);
            }

            // TODO: We should re-think if this should be WhenAny vs. WhenAll
            // Alan had it originally WhenAny, we are now changing it to be WhenAll.

            ActivationAddress[] addrTasks = await Task.WhenAll(tasks);
            ActivationAddress addr = addrTasks[0];

            //Task<ActivationAddress> addrTask = await Task.WhenAny(tasks);
            //ActivationAddress addr = await addrTask;
            // Jorgen:
            // Task.WhenAny returns Task<Task<T>> but then you await which takes off the outer Task to get just Task<T>. 
            // The semantics of Task.WhenAny are that when the outer Task is resolved when one of the input tasks collection is resolved, and it returns that matching Task.
            // http://msdn.microsoft.com/en-us/library/hh194858(v=vs.110).aspx
            // "The returned task will complete when any of the supplied tasks has completed. 
            //  The returned task will always end in the RanToCompletion state with its Result set to the first task to complete. 
            //  This is true even if the first task to complete ended in the Canceled or Faulted state."
            // So, from WhenAny semantics, we know that addrTask will already be resolved, so .Result will fast-path to return the ActivationAddress from that Task.

            // Used to be:
            //var addr = await Task.WhenAny(
            //        connections.Select(connection => GetRegistrar(connection.Silo).RegisterClientObserver(id, Id))
            //        ).Unwrap();
            //var addr = await AsyncCompletionAggregateExtensions.JoinAny(connections.Select(connection => GetRegistrar(connection.Silo).RegisterClientObserver(id, Id)));
           
            lock (lockable)
            {
                GatewayConnection gatewayConnection;
                if (gatewayConnections.TryGetValue(addr.Silo.Endpoint, out gatewayConnection))
                {
                    gatewayConnection.AddObject(addr);
                }
            }
        }

        public Task UnregisterObserver(GrainId id)
        {
            List<GatewayConnection> connections;
            lock (lockable)
            {
                connections = gatewayConnections.Values.Where(conn => conn.IsLive).ToList();
                registeredLocalObjects.Remove(id);
            }

            var results = connections.Select(connection => GetRegistrar(connection.Silo).UnregisterClientObserver(id));

            return Task.WhenAll(results);
        }

        public Task<GrainInterfaceMap> GetTypeCodeMap()
        {
            IPEndPoint gateway = gatewayManager.GetLiveGateway();

            if (gateway == null)
            {
                throw new OrleansException("Not connected to a gateway");
            }

            var silo = SiloAddress.New(gateway, 0);
            return GetTypeManager(silo).GetTypeCodeMap(silo);
        }

        /// <summary>
        /// For internal use only.
        /// </summary>
        /// <param name="type"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public Message WaitMessage(Message.Categories type, CancellationToken ct)
        {
            try
            {
                //return pendingInboundMessages.Take(ct);
                return pendingInboundMessages.Take();
            }
            catch (ThreadAbortException tae)
            {
                // Silo may be shutting-down, so downgrade to verbose log
                logger.Verbose(ErrorCode.ProxyClient_ThreadAbort, "Received thread abort exception -- exiting. {0}", tae);
                Thread.ResetAbort();
                return null;
            }
            catch (OperationCanceledException oce)
            {
                logger.Verbose(ErrorCode.ProxyClient_OperationCancelled, "Received operation cancelled exception -- exiting. {0}", oce);
                return null;
            }
            catch (Exception ex)
            {
                logger.Error(ErrorCode.ProxyClient_ReceiveError, "Unexpected error getting an inbound message", ex);
                return null;
            }
        }

        internal void QueueIncomingMessage(Message msg)
        {
#if TRACK_DETAILED_STATS
            if (StatisticsCollector.CollectQueueStats)
            {
                queueTracking.OnEnQueueRequest(1, pendingInboundMessages.Count);
            }
#endif
            pendingInboundMessages.Add(msg);
        }

        private void RejectMessage(Message msg, string reasonFormat, params object[] reasonParams)
        {
            if (Running)
            {
                var reason = String.Format(reasonFormat, reasonParams);
                if (msg.Direction != Message.Directions.Request)
                {
                    if (logger.IsVerbose) logger.Verbose(ErrorCode.ProxyClient_DroppingMsg, "Dropping message: {0}. Reason = {1}", msg, reason);
                }
                else
                {
                    if (logger.IsVerbose) logger.Verbose(ErrorCode.ProxyClient_RejectingMsg, "Rejecting message: {0}. Reason = {1}", msg, reason);
                    MessagingStatisticsGroup.OnRejectedMessage(msg);
                    Message error = msg.CreateRejectionResponse(Message.RejectionTypes.Unrecoverable, reason);
                    QueueIncomingMessage(error);
                }
            }
        }

        /// <summary>
        /// For testing use only
        /// </summary>
        public void Disconnect()
        {
            throw new NotImplementedException("Disconnect");
        }

        /// <summary>
        /// For testing use only.
        /// </summary>
        public void Reconnect()
        {
            throw new NotImplementedException("Reconnect");
        }

        #region Random IMessageCenter stuff

        /// <summary>
        /// For internal use only.
        /// </summary>
        public bool IsProxying
        {
            get { return false; }
        }


        /// <summary>
        /// For internal use only.
        /// </summary>
        public int SendQueueLength
        {
            get { return 0; }
        }

        /// <summary>
        /// For internal use only.
        /// </summary>
        public int ReceiveQueueLength
        {
            get { return 0; }
        }

        #endregion

        private IClientObserverRegistrar GetRegistrar(SiloAddress destination)
        {
            return (IClientObserverRegistrar)_registrarGetSystemTarget.Invoke(null, new object[] { Constants.ClientObserverRegistrarId, destination });
        }

        private ITypeManager GetTypeManager(SiloAddress destination)
        {
            return (ITypeManager)_typeManagerGetSystemTarget.Invoke(null, new object[] { Constants.TypeManagerId, destination });
        }
    }
}
