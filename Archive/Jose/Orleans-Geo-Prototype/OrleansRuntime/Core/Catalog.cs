using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Orleans.Counters;
using Orleans.RuntimeCore.Configuration;
using Orleans.Runtime.Coordination;
using Orleans.Runtime.Core;
using Orleans.Runtime.Scheduler;

using Orleans.Scheduler;
using Orleans.Storage;


namespace Orleans.Runtime
{
    internal class Catalog : SystemTarget, ICatalog, IPlacementContext, ISiloStatusListener, ISiloShutdownParticipant
    {
        /// <summary>
        /// Exception to indicate that the activation would have been a duplicate so messages pending for it should be redirected.
        /// </summary>
        [Serializable]
        internal class DuplicateActivationException : Exception
        {
            public DuplicateActivationException() : base() { }

            public ActivationAddress ActivationToUse { get; set; }

            public SiloAddress PrimaryDirectoryForGrain { get; set; } // for diagnostics only!
        }

        [Serializable]
        internal class NonExistentActivationException : Exception
        {
            public NonExistentActivationException(string message) : base(message) { }

            public ActivationAddress NonExistentActivation { get; set; }
        }

        private readonly ILocalGrainDirectory directory;

        public GrainTypeManager GrainTypeManager { get; private set; }

        private readonly OrleansTaskScheduler scheduler;

        private readonly TargetDirectory activations;

        private IStorageProviderManager _storageProviderManager;

        internal ISiloStatusOracle SiloStatusOracle { get; set; }

        private Dispatcher dispatcher;

        private readonly Logger logger;

        private readonly ActivationCollector _activationCollector;

        private int collectionNumber;

        private IOrleansTimer gcTimer;

        private readonly OrleansConfiguration configuration;

        private readonly GlobalConfiguration _config;

        public SiloAddress LocalSilo { get; private set; }

        private readonly string LocalSiloName;

        private readonly CounterStatistic activationRegistrations;
        private readonly CounterStatistic acticationUnRegistrations;
        private readonly IntValueStatistic inProcessRequests;
        private readonly CounterStatistic _collectionCounter;

        internal Catalog(GrainId grain, SiloAddress silo, string siloName, ILocalGrainDirectory directory, GrainTypeManager typeManager,
            OrleansTaskScheduler scheduler, TargetDirectory activations, OrleansConfiguration config, out Action<Dispatcher> setDispatcher)
            : base(grain, silo)
        {
            this.LocalSilo = silo;
            this.LocalSiloName = siloName;
            this.directory = directory;
            this.activations = activations;
            this.scheduler = scheduler;
            this.configuration = config;
            this.GrainTypeManager = typeManager;
            logger = Logger.GetLogger("Catalog", Logger.LoggerType.Runtime);
            _config = config.Globals;
            setDispatcher = d => dispatcher = d;
            _activationCollector = new ActivationCollector(config.Globals.CollectionQuantum);
            GC.GetTotalMemory(true); // need to call once w/true to ensure false returns OK value

            configuration.OnConfigChange("Globals/Activation", () => scheduler.RunOrQueueAction(Start, this.SchedulingContext), false);
            IntValueStatistic.FindOrCreate(StatNames.STAT_CATALOG_ACTIVATION_COUNT, () => activations.Count);
            activationRegistrations = CounterStatistic.FindOrCreate(StatNames.STAT_CATALOG_ACTIVATION_REGISTRATIONS);
            acticationUnRegistrations = CounterStatistic.FindOrCreate(StatNames.STAT_CATALOG_ACTIVATION_UNREGISTRATIONS);
            _collectionCounter = CounterStatistic.FindOrCreate(StatNames.STAT_CATALOG_ACTIVATION_COLLECTION_NUMBER_OF_COLLECTIONS);
            inProcessRequests = IntValueStatistic.FindOrCreate(StatNames.STAT_MESSAGING_PROCESSING_ACTIVATION_DATA_ALL, () =>
            {
                long counter = 0;
                lock (activations)
                {
                    foreach (var activation in activations)
                    {
                        ActivationData data = activation.Value;
                        counter += data.GetRequestCount();
                    }
                }
                return counter;
            });
        }

        internal void SetStorageManager(IStorageProviderManager storageProviderManager)
        {
            this._storageProviderManager = storageProviderManager;
        }

        internal void Start()
        {
            if (gcTimer != null)
                gcTimer.Dispose();
            var t = OrleansTimerInsideGrain.FromTaskCallback(OnTimer, null, TimeSpan.Zero, _activationCollector.Quantum);
            t.Start();
            gcTimer = t;
        }

        private Task OnTimer(object _)
        {
           return CollectActivations_Impl(_activationCollector.ScanStale);
        }

        public Task CollectActivations(TimeSpan ageLimit)
        {
           return CollectActivations_Impl(
               () =>
                    _activationCollector.ScanAll(ageLimit));
        }

        private async Task CollectActivations_Impl(Func<List<ActivationData>> scanFunc)
        {
            var watch = new Stopwatch();
            watch.Start();
            var number = Interlocked.Increment(ref collectionNumber);
            long memBefore = GC.GetTotalMemory(false) / (1024 * 1024);
            logger.Info(ErrorCode.Catalog_BeforeCollection, "Before collection#{0}: memory={1}MB, #activations={2}, collector={3}.",
                number, memBefore, activations.Count, _activationCollector.ToString());
            List<ActivationData> list = scanFunc();
            _collectionCounter.Increment();
            if (null != list && list.Count > 0)
            {
                if (logger.IsVerbose) logger.Verbose("CollectActivations{0}", list.ToStrings(d => d.Grain.ToString() + d.ActivationId));
                await ShutdownActivations(list).AsTask();
            }
            long memAfter = GC.GetTotalMemory(false) / (1024 * 1024);
            watch.Stop();
            logger.Info(ErrorCode.Catalog_AfterCollection, "After collection#{0}: memory={1}MB, #activations={2}, collected {3} activations, collector={4}, collection time={5}.",
                number, memAfter, activations.Count, list.Count, _activationCollector.ToString(), watch.Elapsed);
        }

        public List<Tuple<GrainId, string, int>> GetGrainStatistics()
        {
            var counts = new Dictionary<string, Dictionary<GrainId, int>>();
            lock (activations)
            {
                foreach (var activation in activations)
                {
                    ActivationData data = activation.Value;
                    if (data == null || data.GrainInstance == null)
                        continue;
                    // todo: generic type expansion
                    var grainTypeName = TypeUtils.GetFullName(data.GrainInstanceType);
                    if (grainTypeName.EndsWith(GrainClientGenerator.GrainInterfaceData.ActivationClassNameSuffix))
                    {
                        grainTypeName = grainTypeName.Substring(0, grainTypeName.Length - GrainClientGenerator.GrainInterfaceData.ActivationClassNameSuffix.Length);
                    }
                    Dictionary<GrainId, int> grains;
                    int n;
                    if (!counts.TryGetValue(grainTypeName, out grains))
                    {
                        counts.Add(grainTypeName, new Dictionary<GrainId, int> { { data.Grain, 1 } });
                    }
                    else if (!grains.TryGetValue(data.Grain, out n))
                        grains[data.Grain] = 1;
                    else
                        grains[data.Grain] = n + 1;
                }
            }
            return counts
                .SelectMany(p => p.Value.Select(p2 => Tuple.Create(p2.Key, p.Key, p2.Value)))
                .ToList();
        }

        public IEnumerable<KeyValuePair<string, long>> GetSimpleGrainStatistics()
        {
            return activations.GetSimpleGrainStatistics();
        }

        public DetailedGrainReport GetDetailedGrainReport(GrainId grain)
        {
            DetailedGrainReport report = new DetailedGrainReport();
            report.Grain = grain;
            report.SiloAddress = LocalSilo;
            report.SiloName = LocalSiloName;
            report.LocalCacheActivationAddresses = directory.GetLocalCacheData(grain);
            report.LocalDirectoryActivationAddresses = directory.GetLocalDirectoryData(grain);
            report.PrimaryForGrain = directory.GetPrimaryForGrain(grain);
            try
            {
                PlacementStrategy unused;
                string grainClassName;
                GrainTypeManager.GetTypeInfo(grain.GetTypeCode(), out grainClassName, out unused);
                report.GrainClassTypeName = grainClassName;
            }
            catch (Exception exc)
            {
                report.GrainClassTypeName = exc.ToString();
            }

            List<ActivationData> acts = activations.FindTargets(grain);
            if (acts != null)
            {
                report.LocalActivations = acts.Select(activationData => activationData.ToDetailedString()).ToList();
            }
            else
            {
                report.LocalActivations = new List<string>();
            }
            return report;
        }

        private bool ShouldUseSingleActivation(ActivationData activationData)
        {
            // [mlr] currently, the only supported multi-activation grain is one using the LocalPlacement strategy.
            return !(activationData.PlacedUsing is LocalPlacement);
        }

        #region MessageTargets

        /// <summary>
        /// Register a new object to which messages can be delivered with the local lookup table and scheduler.
        /// </summary>
        /// <param name="activation"></param>
        public void RegisterMessageTarget(ActivationData activation) // was Silo
        {
            var context = new OrleansContext(activation);

            scheduler.RegisterWorkContext(context);

            activations.RecordNewTarget(activation);
            activationRegistrations.Increment();
        }

        /// <summary>
        /// Unregister message target and stop delivering messages to it
        /// todo: it could still be running - rundown protocol?
        /// </summary>
        /// <param name="activation"></param>
        public void UnregisterMessageTarget(ActivationData activation)
        {
            activations.RemoveTarget(activation);

            // todo: this should be removed once we've refactored the deactivation code path.
                _activationCollector.TryCancelCollection(activation);

            scheduler.UnregisterWorkContext(new OrleansContext(activation));

            if (activation.GrainInstance != null)
            {
                string grainTypeName = TypeUtils.GetFullName(activation.GrainInstanceType);
                if (grainTypeName.EndsWith(GrainClientGenerator.GrainInterfaceData.ActivationClassNameSuffix))
                {
                    grainTypeName = grainTypeName.Substring(0, grainTypeName.Length - GrainClientGenerator.GrainInterfaceData.ActivationClassNameSuffix.Length);
                }
                activations.DecrementGrainCounter(grainTypeName);
                activation.SetGrainInstance(null);
            }
            acticationUnRegistrations.Increment();
        }

        /// <summary>
        /// FOR TESTING PURPOSES ONLY!!
        /// </summary>
        /// <param name="grain"></param>
        internal int UnregisterGrainForTesting(GrainId grain)
        {
            var acts = activations.FindTargets(grain);
            int numActsBefore = 0;
            if (acts != null)
            {
                numActsBefore = acts.Count;
                foreach (var act in acts)
                {
                    //activations.RemoveTarget(act);
                    UnregisterMessageTarget(act);
                }
            }
            return numActsBefore;
        }

        #endregion
        #region Grains

        /// <summary>
        /// Get runtime-related information about a grain type, if available
        /// </summary>
        /// <param name="type">Grain type</param>
        /// <param name="result">Data</param>
        /// <returns>True if available</returns>
        public bool TryGetTypeData(Type type, out GrainTypeData result) // was OrleansController
        {
            Type found = null;
            if (GrainTypeManager.TryGetData(type.FullName, out result))
                return true;
            foreach (var iface in type.GetInterfaces())
            {
                GrainTypeData data;
                if (GrainTypeManager.TryGetData(iface.FullName, out data) &&
                    (found == null || found.IsAssignableFrom(iface)))
                {
                    found = iface;
                    result = data;
                }
            }
            return found != null;
        }

        internal bool IsReentrantGrain(ActivationId running)
        {
            ActivationData target;
            GrainTypeData data;
            return TryGetActivationData(running, out target) &&
                target.GrainInstance != null &&
                GrainTypeManager.TryGetData(TypeUtils.GetFullName(target.GrainInstanceType), out data) &&
                data.IsReentrant;
        }

        public void GetGrainTypeInfo(int typeCode, out string grainClass, out PlacementStrategy placement, string genericArguments = null)
        {
            GrainTypeManager.GetTypeInfo(typeCode, out grainClass, out placement, genericArguments);
        }

        #endregion
        #region Activations

        public int ActivationCount { get { return activations.Count; } }

        /// <summary>
        /// If activation already exists, use it
        /// Otherwise, create an activation of an existing grain by reading its state.
        /// Return immediately using a dummy that will queue messages.
        /// Concurrently start creating and initializing the real activation and replace it when it is ready.
        /// </summary>
        /// <param name="address">Grain's activation address</param>
        /// <param name="placement">If not null, a new activation is expected to be created given the specified strategy.</param>
        /// <param name="grainType">The type of grain to be activated or created</param>
        /// <param name="genericInterface">Specific generic type of grain to be activated or created</param>
        /// <param name="activatedPromise"></param>
        /// <returns></returns>
        public ActivationData GetOrCreateActivation(
            ActivationAddress address,
            PlacementStrategy placement,
            string grainType,
            string genericInterface,
            out AsyncCompletion activatedPromise)
        {
            ActivationData result;
            activatedPromise = AsyncCompletion.Done;
            lock (activations)
            {
                if (TryGetActivationData(address.Activation, out result))
                {
                    // we don't care if an activation can't be touched, so we ignore any failure.
                    _activationCollector.TryRescheduleCollection(result, result.CollectionAgeLimit);
                    return result;
                }
                // [mlr] 'placement' not being null is an indication that we are creating an activation.
                if (placement != null)
                {
                    // create a dummy activation that will queue up messages until the real data arrives
                    result = new ActivationData(address, placement, _activationCollector);
                    RegisterMessageTarget(result);
                }
            } // End lock

            if (result == null)
            {
                string msg = String.Format("Non-existent activation: {0}, grain type: {1}.",
                                           address.ToFullString(), grainType);
                if (logger.IsVerbose) logger.Verbose(ErrorCode.CatalogNonExistingActivation2, msg);
                CounterStatistic.FindOrCreate(StatNames.STAT_CATALOG_NON_EXISTING_ACTIVATIONS).Increment();
                throw new NonExistentActivationException(msg) { NonExistentActivation = address };
            }
            // [mlr] is 'placement' expected to be non-null at this point?
            // i.e. if the grain had been found, it would have been returned from within
            // the lock scope. the logic suggests this but does not make it clear.
            // UPDATE: everything works in the nightly & bvt unit tests, so i don't see 
            // any evidence yet that this is not the case.

            string genericArguments = String.IsNullOrEmpty(genericInterface) ? null
                : TypeUtils.GenericTypeArgsString(genericInterface);

            lock (result)
            {
                if (result.GrainInstance == null)
                {
                    CreateGrainInstance(grainType, result, genericArguments);
                }

                string type = grainType;
                if (string.IsNullOrEmpty(type))
                {
                    PlacementStrategy unused;
                    int typeCode = result.Address.Grain.GetTypeCode();
                    GetGrainTypeInfo(typeCode, out type, out unused);
                }
                activatedPromise = InitActivation(result, type);
            } // End lock

            return result;
        }

        private AsyncCompletion InitActivation(ActivationData result, string grainType)
        {
            // We've created a dummy activation, which we'll eventually return, but in the meantime we'll queue up (or perform promptly)
            // the operations required to turn the "dummy" activation into a real activation

            // Fill in the activation's state
            AsyncCompletion statePromise;

            GrainState state = result.GrainInstance.GrainState;

            ActivationAddress address = result.Address;
            ISchedulingContext schedContext = new OrleansContext(result); // Target grain's scheduler context
            if (result.StorageProvider != null)
            {
                Stopwatch sw = Stopwatch.StartNew();
                // Populate state data
                statePromise = scheduler.RunOrQueueAsyncCompletion(() => AsyncCompletion.FromTask(
                    result.StorageProvider.ReadStateAsync(grainType, address.GrainReference, state)),
                    schedContext)
                    .ContinueWith(() =>
                    {
                        sw.Stop();

                        StorageStatisticsGroup.OnStorageActivate(result.StorageProvider, grainType, address.Grain, sw.Elapsed);
                        result.GrainInstance.GrainState = state;
                    },
                    ex =>
                    {
                        StorageStatisticsGroup.OnStorageActivateError(result.StorageProvider, grainType, address.Grain);
                        sw.Stop();

                        if (!(ex.GetBaseException() is KeyNotFoundException))
                            throw ex;

                        result.GrainInstance.GrainState = state; // Just keep original empty state object
                    });
            }
            else
            {
                statePromise = AsyncCompletion.Done;
            }

            // A chain of promises that will have to complete in order to complete the activation
            // Call the Activate method on the new activation, register with the store if necessary, and register with the grain directory
            AsyncCompletion registerPromise = statePromise.ContinueWith(() => RegisterInGrainDirectory(address, ShouldUseSingleActivation(result)));

            // When all of the pending work completes...
            AsyncCompletion activatePromise = registerPromise.ContinueWith(
                () => InvokeActivate(result, schedContext)
            ).ContinueWith(
                () =>
                {
                    // Success!! Log the result, and start processing messages
                    if (logger.IsVerbose2) logger.Verbose2("GetOrCreateActivation created {0}", address);
                }, ex =>
                {
                    // Failure!! Could it be that this grain uses single activation placement, and there already was
                    // an activation?
                    ActivationAddress target = null;
                    Exception dupExc;
                    if (Utils.TryFindException(ex, typeof(DuplicateActivationException), out dupExc))
                    {
                        target = ((DuplicateActivationException)dupExc).ActivationToUse;
                        CounterStatistic.FindOrCreate(StatNames.STAT_CATALOG_DUPLICATE_ACTIVATIONS).Increment();
                    }
                    lock (result)
                    {
                        result.ForwardingAddress = target;

                        if (target != null)
                        {
                            // If this was a duplicate, it's not an error, just a race.
                            // Forward on all of the pending messages, and then forget about this activation.
                            logger.Info(ErrorCode.Catalog_DuplicateActivation,
                                                "Tried to create a duplicate activation {0}, but we'll use {1} instead. Primary Directory partition for this grain is {2}, " +
                                                "full activation address is {3}, GrainInstanceType is {4}. We have {5} messages to forward.",
                                                address,
                                                target,
                                                ((DuplicateActivationException)dupExc).PrimaryDirectoryForGrain,
                                                address.ToFullString(),
                                                result.GrainInstanceType,
                                                result.WaitingCount);

                            result.ReroutePending(
                                message => dispatcher.ProcessRequestToInvalidActivation(message, target, "resending from duplicate activation", false));
                        }
                        else
                        {
                            // Something more serious went wrong. This is an unrecoverable error.
                            logger.Error(ErrorCode.Runtime_Error_100064,
                                            "Attempt to create or register activation " + address.ToString() + " failed", ex);

                            result.ReroutePending(message =>
                            {
                                message.AddToCacheInvalidationHeader(result.Address);
                                dispatcher.RejectMessage(message, Message.RejectionTypes.Unrecoverable, ex);
                            });

                            // Need to undo the registration we just did earlier
                            scheduler.QueueWorkItem(new ClosureWorkItem(
                                    () => directory.UnregisterAsync(address).Ignore(),
                                    () => "LocalGrainDirectory.Unregister"),
                                this.SchedulingContext);
                        }
                        // reject additional messages
                        result.SetState(ActivationState.Invalid);
                        UnregisterMessageTarget(result);
                    }
                    throw ex;
                }
            );
            return activatePromise; // No need to log error again. It is already logged. // LogErrors(logger, ErrorCode.Catalog_RegistrationFailure, "Error registering activation " + address.ToString() + " failed");
        }

        /// <summary>
        /// Perform just the prompt, local part of creating an activation object
        /// Caller is responsible for registering locally, registering with store and calling its activate routine
        /// </summary>
        /// <param name="grainTypeName"></param>
        /// <param name="data"></param>
        /// <param name="genericArguments"></param>
        /// <returns></returns>
        private void CreateGrainInstance(string grainTypeName, ActivationData data, string genericArguments)
        {
            string grainClassName;
            var interfaceToClassMap = GrainTypeManager.GetGrainInterfaceToClassMap();
            if (!interfaceToClassMap.TryGetValue(grainTypeName, out grainClassName))
            {
                // Lookup from grain type code
                var typeCode = data.Grain.GetTypeCode();
                if (typeCode != 0)
                {
                    PlacementStrategy unused;
                    GetGrainTypeInfo(typeCode, out grainClassName, out unused, genericArguments);
                }
                else
                {
                    grainClassName = grainTypeName;
                }
            }
            GrainTypeData grainTypeData = GrainTypeManager[grainClassName];

            Type grainType = grainTypeData.Type;
            Type stateObjectType = grainTypeData.StateObjectType;
            lock (data)
            {
                data.SetGrainInstance((GrainBase) Activator.CreateInstance(grainType));
                if (stateObjectType != null)
                {
                    var state = (GrainState) Activator.CreateInstance(stateObjectType);
                    state.InitState(null);
                    data.GrainInstance.GrainState = state;
                }

                if (!ActivationCollector.IsExemptFromCollection(data))
                {
                    TimeSpan ageLimit = _config.Application.GetCollectionAgeLimit(grainType);
                    data.SetCollectionAgeLimit(ageLimit);
                        _activationCollector.ScheduleCollection(data, ageLimit);
                }
            }

            activations.IncrementGrainCounter(grainClassName);

            // ensure init runs in activation context; scheduler should ensure later tasks are queued after it
            // todo: task context? if this is called from scheduler, should prevent new task from being created?
            // if it is in existing task context, should it delete activation?
            data.GrainInstance._Data = data;
            SetupStorageProvider(data);

            if (logger.IsVerbose) logger.Verbose("CreateGrainInstance {0}{1}", data.Grain, data.ActivationId);
        }

        private void SetupStorageProvider(ActivationData data)
        {
            object[] attrs = data.GrainInstanceType.GetCustomAttributes(typeof(StorageProviderAttribute), true);
            StorageProviderAttribute attr = attrs.Length > 0 ? attrs[0] as StorageProviderAttribute : null;
            if (attr != null)
            {
                string storageProviderName = attr.ProviderName;
                IStorageProvider provider;
                string grainTypeName = data.GrainInstanceType.FullName;
                if (_storageProviderManager == null || _storageProviderManager.GetNumLoadedProviders() == 0)
                {
                    string errMsg = string.Format("No storage providers found loading grain type {0}", grainTypeName);
                    logger.Error(ErrorCode.Provider_CatalogNoStorageProvider_1, errMsg);
                    throw new BadProviderConfigException(errMsg);
                }
                if (string.IsNullOrWhiteSpace(storageProviderName))
                {
                    // Use default storage provider
                    provider = _storageProviderManager.GetDefaultProvider();
                }
                else
                {
                    // Look for MemoryStore provider as special case name
                    bool caseInsensitive = Constants.MEMORY_STORAGE_PROVIDER_NAME.Equals(storageProviderName, StringComparison.OrdinalIgnoreCase);
                    _storageProviderManager.TryGetProvider(storageProviderName, out provider, caseInsensitive);
                    if (provider == null)
                    {
                        string errMsg = string.Format(
                            "Cannot find storage provider with Name={0} for grain type {1}", storageProviderName,
                            grainTypeName);
                        logger.Error(ErrorCode.Provider_CatalogNoStorageProvider_2, errMsg);
                        throw new BadProviderConfigException(errMsg);
                    }
                }
                data.StorageProvider = provider;
                if (logger.IsVerbose2)
                {
                    string msg = string.Format("Assigned storage provider with Name={0} to grain type {1}",
                                               storageProviderName, grainTypeName);
                    logger.Verbose2(ErrorCode.Provider_CatalogStorageProviderAllocated, msg);
                }
            }
        }

        /// <summary>
        /// Try to get runtime data for an activation
        /// </summary>
        /// <param name="activationId"></param>
        /// <param name="data"></param>
        /// <returns></returns>
        public bool TryGetActivationData(ActivationId activationId, out ActivationData data)
        {
            data = null;
            if (!activationId.IsSystem)
            {
                data = activations.FindTarget(activationId);
                return data != null;
            }
            return false;
        }

        // make sure to execute ShutdownActivations on the Catalog context!!!
        internal void QueueShutdownActivations(List<ActivationData> list)
        {
            scheduler.QueueAction(() => ShutdownActivations(list).Ignore(), this.SchedulingContext);
        }

        /// <summary>
        /// Gracefully deletes activations, putting it into a shutdown state to
        /// complete and commit outstanding transactions before deleting it.
        /// </summary>
        /// <param name="list"></param>
        /// <returns></returns>
        private AsyncCompletion ShutdownActivations(List<ActivationData> list)
        {
            if (list == null || list.Count == 0)
            {
                return AsyncCompletion.Done;
            }
            var destroyLater = new List<ActivationData>();
            var destroyNow = new List<ActivationData>();
            foreach (var d in list)
            {
                ActivationData data = d; // capture
                lock (data)
                {
                    if (data.Shutdown == null)
                    {
                        // GK TODO: Do we also need to change the ActivationData state here, since we're about to give up the lock? 
                        // Or do we reject messages sent to an activation with a non-null Shutdown?
                        data.Shutdown = new AsyncCompletionResolver();
                        if (data.IsInactive)
                        {
                            destroyNow.Add(data);
                        }
                        else // busy, so destroy later.
                        {
                            data.AddOnInactive(() => DestroyActivation(data.ActivationId).Ignore());
                            destroyLater.Add(data);
                        }
                    }
                }
            }
            logger.Info(ErrorCode.Catalog_ShutdownActivations, "ShutdownActivations: total {0} to shutdown, out of them {1} prompt.", list.Count, destroyNow.Count);
            CounterStatistic.FindOrCreate(StatNames.STAT_CATALOG_ACTIVATION_COLLECTION_NUMBER_OF_COLLECTED_ACTIVATIONS_PROMPT).IncrementBy(destroyNow.Count);
            CounterStatistic.FindOrCreate(StatNames.STAT_CATALOG_ACTIVATION_COLLECTION_NUMBER_OF_COLLECTED_ACTIVATIONS_DELAYED).IncrementBy(destroyLater.Count);

            AsyncCompletion destroyNowPromise = DestroyActivations(destroyNow);
            List<AsyncCompletion> destroyLaterPromises = destroyLater.Select(data => data.Shutdown.AsyncCompletion).ToList();
            AsyncCompletion destroyLaterPromise = AsyncCompletion.JoinAll(destroyLaterPromises);
            // jt - ToDo: Don't we also need to unregister from grain directory for any destroyLater activations?
            return AsyncCompletion.Join(destroyNowPromise, destroyLaterPromise);
        }

        /// <summary>
        /// Deletes activation immediately regardless of active transactions etc.
        /// For use by grain delete, transaction abort, etc.
        /// </summary>
        /// <param name="activationId"></param>
        private AsyncCompletion DestroyActivation(ActivationId activationId)
        {
            // note: assumes state has already been persisted (or abandoned)
            ActivationData activation;
            if (TryGetActivationData(activationId, out activation))
                return DestroyActivations(new List<ActivationData> { activation });
            return AsyncCompletion.Done;
        }

        /// <summary>
        /// Forcibly deletes activations now, without waiting for any outstanding transactions to complete.
        /// </summary>
        /// <param name="list"></param>
        /// <returns></returns>
        //
        // Overall code flow:
        //
        // set Deactivating
        // CallGrainDeactivate
        // when AsyncDeactivate promise is resolved (NOT when all Deactivate turns are done):
        // unregister in the directory 
        // when all AsyncDeactivate turns are done (Dispatcher.OnActivationCompletedRequest):
        // set Invalid
        // InvalidateCacheEntry
        // UnregisterMessageTarget
        // Reroute pending
        private AsyncCompletion DestroyActivations(List<ActivationData> list)
        {
            if (logger.IsVerbose) logger.Verbose(ErrorCode.Catalog_DestroyActivations, "DestroyActivations {0} activations", list.Count);
            var promises = new List<AsyncCompletion>();
            foreach (ActivationData activation in list)
            {
                promises.Add(DeactivateActivation(activation));
            }
            return AsyncCompletion.JoinAll(promises)
                .Finally(() =>
                    scheduler.RunOrQueueAsyncCompletion(() => 
                        AsyncCompletion.FromTask( directory.UnregisterManyAsync(
                            list.Select(d => ActivationAddress.GetAddress(LocalSilo, d.Grain, d.ActivationId)).ToList())),
                        this.SchedulingContext).Finally(() => 
                        {
                            foreach (ActivationData activation in list)
                            {
                                UnregisterMessageTarget(activation);
                            }
                        }).LogWarnings(logger, ErrorCode.Catalog_UnregisterManyAsync, String.Format("UnregisterManyAsync {0} failed.", list.Count)));
        }

        /// <summary>
        /// Deactivate a single activation.
        /// </summary>
        private AsyncCompletion DeactivateActivation(ActivationData activationData)
        {
            ActivationData activation; // Capture variable
            if (!TryGetActivationData(activationData.ActivationId, out activation))
                return AsyncCompletion.Done;

            if (logger.IsVerbose) logger.Verbose("DestroyActivation - Beginning {0}", activation.ActivationId);

            lock (activation)
            {
                activation.SetState(ActivationState.Deactivating); // Don't accept any new messages
            }

            // ActivateionData will transition out of ActivationState.Deactivating via Dispatcher.OnActivationCompletedRequest, 
            // which will result in call to OnFinishedGrainDeactivate

            return scheduler.QueueTask(() => CallGrainDeactivate(activation), new OrleansContext(activation));
        }

        internal void OnFinishedGrainDeactivate(ActivationData activation)
        {
            if (logger.IsVerbose) logger.Verbose("DestroyActivation - Completing {0}", activation.ActivationId);

            // Just use this opportunity to invalidate local Cache Entry as well. 
            // If this silo is not the grain directory partition for this grain, it may have it in its cache.
            directory.InvalidateCacheEntry(activation.Address.Grain);

            lock (activation)
            {
                // We need first to signal Shutdown promise before we UnregisterMessageTarget
                // for ShutdownActivations() to be able to fisih properly since it does CW on this promise
                // Otherwise, we CW on Shutdown on activation whose workItemGroup has already ben shutdown.
                // Long ter, we shoudl eb able to get rid of Shutdown promise all together.
                if (activation.Shutdown != null)
                {
                    activation.Shutdown.TryResolve();
                }

                ///UnregisterMessageTarget(activation); // Do this after CallGrainDeactivate because grain may want to send messages / scheduler turns during Deactivate

                // GK: VERY VERY IMPORTNANT!!
                // Make sure not to do any CW on this activation context after UnregisterMessageTarget is done (no CE inside Catalog.CallGrainDeactivate after UnregisterMessageTarget)!! 

                activation.ForwardingAddress = null;
                //activation.ReroutePending(InsideGrainClient.Current.RerouteMessage);
                activation.ReroutePending(dispatcher.SendMessage);
            }

            if (logger.IsVerbose) logger.Verbose("DestroyActivation - Unregistered {0}", activation.ActivationId);
        }

        private async Task CallGrainActivate(ActivationData activation)
        {
            string grainType = activation.GrainInstanceType.FullName;

            // Note: This call is being made from within Scheduler.Queue wrapper, so we are already executing on worker thread

            if (logger.IsVerbose)
            {
                logger.Verbose(ErrorCode.Catalog_BeforeCallingActivate, "About to call {1} grain's ActivateAsync() method {0}", activation, grainType);
            }

            // Call ActivateAsync inline, but within try-catch wrapper to safely capture any exceptions thrown from called function
            try
            {
                await activation.GrainInstance.ActivateAsync();

                if (logger.IsVerbose)
                {
                    logger.Verbose(ErrorCode.Catalog_AfterCallingActivate, "Returned from calling {1} grain's ActivateAsync() method {0}", activation, grainType);
                }

                dispatcher.OnActivateDeactivateCompleted(activation);
            }
            catch (Exception exc)
            {
                logger.Error(ErrorCode.Catalog_ErrorCallingActivate,
                    string.Format("Error calling grain's AsyncActivate method - Grain type = {1} Activation = {0}", activation, grainType), exc);

                activation.SetState(ActivationState.Invalid); // Mark this activation as unusable

                throw;
            }
        }

        private async Task CallGrainDeactivate(ActivationData activation)
        {
            string grainType = activation.GrainInstanceType.FullName;

            // Note: This call is being made from within Scheduler.Queue wrapper, so we are already executing on worker thread

            if (logger.IsVerbose)
            {
                logger.Verbose(ErrorCode.Catalog_BeforeCallingDeactivate, "About to call {1} grain's DeactivateAsync() method {0}", activation, grainType);
            }

            // Call DeactivateAsync inline, but within try-catch wrapper to safely capture any exceptions thrown from called function
            try
            {
                await activation.GrainInstance.DeactivateAsync();

                if (logger.IsVerbose)
                {
                    logger.Verbose(ErrorCode.Catalog_AfterCallingDeactivate, "Returned from calling {1} grain's DeactivateAsync() method {0}", activation, grainType);
                }

                dispatcher.OnActivateDeactivateCompleted(activation);
            }
            catch (Exception exc)
            {
                logger.Error(ErrorCode.Catalog_ErrorCallingDeactivate,
                    string.Format("Error calling grain's DeactivateAsync method - Grain type = {1} Activation = {0}", activation, grainType), exc);

                activation.SetState(ActivationState.Invalid); // Mark this activation as unusable

                throw;
            }
        }

        private AsyncCompletion RegisterInGrainDirectory(ActivationAddress address, bool singleActivationMode)
        {
            if (singleActivationMode)
            {
                var promise = scheduler.RunOrQueueTask(() => directory.RegisterSingleActivationAsync(address), this.SchedulingContext);
                return promise.ContinueWith(returnedAddress =>
                {
                    if (!address.Equals(returnedAddress))
                    {
                        throw new DuplicateActivationException { ActivationToUse = returnedAddress, PrimaryDirectoryForGrain = directory.GetPrimaryForGrain(address.Grain) };
                    }
                    return AsyncCompletion.Done;
                });
            }
            else
            {
                return scheduler.RunOrQueueAsyncCompletion(() => AsyncCompletion.FromTask(directory.RegisterAsync(address)), this.SchedulingContext);
            }
        }

        #endregion
        #region Activations - private

        /// <summary>
        /// Invoke the activate method on a newly created activation
        /// </summary>
        /// <param name="activation"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        private AsyncCompletion InvokeActivate(ActivationData activation, ISchedulingContext context)
        {
            // NOTE: This should only be called with the correct schedulering context for the activation to be invoked.
            // This info is already available in calling code in InitActivation, 
            // so optimization is to pass it in here as param rather than create now one here.

            // todo: run each activation in its own entire txn, deactive on abort?

            lock (activation)
            {
                activation.SetState(ActivationState.Activating);
            }

            AsyncCompletion activatePromise = scheduler.QueueTask(() => CallGrainActivate(activation), context);

            // ActivationData will transition out of ActivationState.Activating via Dispatcher.OnActivationCompletedRequest

            return activatePromise;
        }
        #endregion
        #region IPlacementContext

        public Logger Logger
        {
            get { return logger; }
        }

        public bool FastLookup(GrainId grain, out List<ActivationAddress> addresses) // was OrleansController IPlacementContext
        {
            if (directory.LocalLookup(grain, out addresses))
                return true;
            // GK: NOTE: only check with the local directory cache.
            // DO NOT, I repeat, DO NOT, check in the local activations TargetDirectory!!!
            // The only source of truth about which activation should be legit to is the state of the ditributed directory.
            // Everyone should converge to that (that is the meaning of "eventualy consistency - eventualy we converge to one truth").
            // If we keep using the local activation, it may not be registered in th directory any more, but we will never know that and keep using it,
            // thus volaiting the single-activation semantics and not converging even eventualy!
            return false;
        }

        public AsyncValue<List<ActivationAddress>> FullLookup(GrainId grain) // was OrleansController IPlacementContext
        {
            List<ActivationAddress> addresses;
            if (directory.LocalLookup(grain, out addresses))
                return addresses;
            return scheduler.RunOrQueueTask(() => directory.FullLookup(grain), this.SchedulingContext);
        }

        public bool LocalLookup(GrainId grain, out List<ActivationData> addresses) // was OrleansController IPlacementContext
        {
            addresses = activations.FindTargets(grain);
            return addresses != null;
        }

        public List<SiloAddress> AllSilos // was OrleansController IPlacementContext
        {
            get
            {
                var result = SiloStatusOracle.GetApproximateSiloStatuses(true).Select(s => s.Key).ToList();
                if (result.Count > 0)
                    return result;
                logger.Warn(ErrorCode.Catalog_GetApproximateSiloStatuses, "AllSilos SiloStatusOracle.GetApproximateSiloStatuses empty");
                return new List<SiloAddress> { LocalSilo };
            }
        }

        #endregion
        #region Implementation of ICatalog

        public Task CreateSystemGrain(GrainId grainId, string grainType)
        {
            ActivationAddress target = ActivationAddress.NewActivationAddress(LocalSilo, grainId);
            AsyncCompletion activatedPromise;
            ActivationData unused = GetOrCreateActivation(target, SystemPlacement.Singleton, grainType, null, out activatedPromise);
            return activatedPromise != null? activatedPromise.AsTask() : TaskDone.Done;
        }

        public Task DeleteGrainsLocal(List<GrainId> grainIds)
        {
            if (logger.IsVerbose) logger.Verbose("DeleteGrainsLocal {0}", grainIds.ToStrings());
            var promises = new List<AsyncCompletion>();
            foreach (var grainId in grainIds)
            {
                List<ActivationData> targets = activations.FindTargets(grainId);
                if (targets != null)
                {
                    promises.Add(DestroyActivations(targets));
                }
            }
            return AsyncCompletion.JoinAll(promises).AsTask();
        }

        public Task DeleteActivationsLocal(List<ActivationAddress> addresses)
        {
            return DestroyActivations(TryGetActivationDatas(addresses)).AsTask();
        }


        private List<ActivationData> TryGetActivationDatas(List<ActivationAddress> addresses)
        {
            var datas = new List<ActivationData>(addresses.Count);
            foreach (var activationAddress in addresses)
            {
                ActivationData data;
                if (TryGetActivationData(activationAddress.Activation, out data))
                    datas.Add(data);
            }
            return datas;
        }

        public async Task InvalidatePartitionCache(ActivationAddress activationAddress)
        {
            await directory.FlushCachedPartitionEntry(activationAddress);
        }

        #endregion

        #region Implementation of ISiloStatusListener

        public void SiloStatusChangeNotification(SiloAddress updatedSilo, SiloStatus status)
        {
            // ignore joining events and also events on myself.
            if (updatedSilo.Equals(LocalSilo))
            {
                return;
            }

            // We deactivate those activations when silo goes either of ShuttingDown/Stopping/Dead states,
            // since this is what Directory is doing as well. Directory removes a silo based on all those 3 statuses,
            // thus it will only deliver a "remove" notification for a given silo once to us. Therefore, we need to react the fist time we are notified.
            // We may review the directory behaiviour in the future and treat ShuttingDown differently ("drain only") and then this code will have to change a well.
            if (status.Equals(SiloStatus.Dead) || status.Equals(SiloStatus.ShuttingDown) || status.Equals(SiloStatus.Stopping))
            {
                List<ActivationData> activationsToShutdown = new List<ActivationData>();
                try
                {
                    // scan all activations in activation directory and deactivate the ones that the removed silo is their primary partition owner.
                    lock (activations)
                    {
                        foreach (var activation in activations)
                        {
                            try
                            {
                                ActivationData activationData = activation.Value;
                                if (directory.GetPrimaryForGrain(activationData.Grain).Equals(updatedSilo))
                                {
                                    lock (activationData)
                                    {
                                        // adapted from InsideGarinClient.DeactivateOnIdle().
                                        activationData.ResetKeepAliveRequest();
                                        activationsToShutdown.Add(activationData);
                                    }
                                }
                            }
                            catch (Exception exc)
                            {
                                logger.Error(ErrorCode.Catalog_SiloStatusChangeNotification_Exception,
                                        String.Format("Catalog has thrown an exception while executing SiloStatusChangeNotification of silo {0}.", updatedSilo.ToStringWithHashCode()), exc);
                            }
                        }
                    }
                    logger.Info(ErrorCode.Catalog_SiloStatusChangeNotification,
                        String.Format("Catalog is deactivating {0} activations due to a failure of silo {1}, since it is a primary directory partiton to these grain ids.",
                        activationsToShutdown.Count, updatedSilo.ToStringWithHashCode()));
                }
                finally
                {
                    // outside the lock.
                    if (activationsToShutdown.Count > 0)
                    {
                        QueueShutdownActivations(activationsToShutdown);
                    }
                }
            }
        }

        #endregion

        #region Implementation of ISiloShutdownParticipant

        void ISiloShutdownParticipant.BeginShutdown(Action tryFinishShutdown)
        {
            gcTimer.Dispose();
            tryFinishShutdown();
        }

        bool ISiloShutdownParticipant.CanFinishShutdown()
        {
            return true;
        }

        void ISiloShutdownParticipant.FinishShutdown()
        {
            // nothing
        }

        SiloShutdownPhase ISiloShutdownParticipant.Phase
        {
            get { return SiloShutdownPhase.Scheduling; }
        }

        #endregion
    }
}
