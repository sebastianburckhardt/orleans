//#define PARALLEL_INIT_SYS_GRAINS
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.ServiceRuntime;

using Orleans.Runtime.Messaging;
using Orleans.Providers;
using Orleans.Runtime.Providers;
using Orleans.Runtime.Storage;
using Orleans.Scheduler;
using Orleans.Runtime.Coordination;
using Orleans.Runtime.GrainDirectory;
using Orleans.Runtime.Scheduler;

using Orleans.Storage;
using Orleans.Counters;
using Orleans.Serialization;

using Orleans.Runtime.Counters;
using Orleans.Runtime.MembershipService;


namespace Orleans.Runtime
{
    /// <summary>
    /// Orleans silo.
    /// </summary>
    public class Silo : MarshalByRefObject
    {
        /// <summary> Silo Types. </summary>
        public enum SiloType
        {
            None = 0,
            Primary,
            Secondary,
        }

        /// <summary> Type of this silo. </summary>
        public SiloType Type
        {
            get { return siloType; }
        }

        private readonly GlobalConfiguration globalConfig;
        private NodeConfiguration nodeConfig;
        private readonly ISiloMessageCenter messageCenter;
        private readonly OrleansTaskScheduler scheduler;
        private readonly LocalGrainDirectory localGrainDirectory;
        private readonly IConsistentRingProvider consistentRingProvider;
        private readonly TargetDirectory activationDirectory;
        private readonly IncomingMessageAgent incomingAgent;
        private readonly IncomingMessageAgent incomingSystemAgent;
        private readonly IncomingMessageAgent incomingPingAgent;
        private readonly Logger logger;
        private readonly GrainTypeManager typeManager;
        private readonly ManualResetEvent shutdownFinishedEvent;
        private readonly ManualResetEvent siloTerminatedEvent;
        internal readonly string Name;
        private readonly SiloType siloType;
        private readonly SiloStatisticsManager siloStatistics;
        private readonly MembershipFactory membershipFactory;
        private StorageProviderManager storageProviderManager;
        private readonly LocalReminderServiceFactory reminderFactory;
        private IReminderService reminderService;
        private ProviderManagerSystemTarget providerManagerSystemTarget;
        private IMembershipOracle membershipOracle;
        private Watchdog platformWatchdog;

        internal OrleansConfiguration OrleansConfig { get; private set; }
        internal GlobalConfiguration GlobalConfig { get { return globalConfig; } }
        internal NodeConfiguration LocalConfig { get { return nodeConfig; } }
        internal ISiloMessageCenter LocalMessageCenter { get { return messageCenter; } }
        internal OrleansTaskScheduler LocalScheduler { get { return scheduler; } }
        internal GrainTypeManager LocalTypeManager { get { return typeManager; } }
        internal ILocalGrainDirectory LocalGrainDirectory { get { return localGrainDirectory; } }
        internal ISiloStatusOracle LocalSiloStatusOracle { get { return membershipOracle; } }
        internal IConsistentRingProvider ConsistentRingProvider { get { return consistentRingProvider; } }
        internal IStorageProviderManager StorageProviderManager { get { return storageProviderManager; } }
        internal List<IBootstrapProvider> BootstrapProviders { get; private set; }

        internal ISiloPerformanceMetrics Metrics { get { return siloStatistics.MetricsTable; } }

        /// <summary> SiloAddress for this silo. </summary>
        // todo: initialize in all threads
        public SiloAddress SiloAddress { get { return messageCenter.MyAddress; } }

        /// <summary>
        ///  Silo termination event used to signal shutdown of this silo.
        /// </summary>
        public WaitHandle SiloTerminatedEvent { get { return siloTerminatedEvent; } } // one event for all types of termination (shutdown, stop and fast kill).

        /// <summary>
        /// Test hookup connection for white-box testing of silo.
        /// For internal use only.
        /// </summary>
        public TestHookups TestHookup;

        private readonly TimeSpan initTimeout;
        private readonly TimeSpan stopTimeout = TimeSpan.FromMinutes(1);
        //private static readonly TimeSpan initTimeout = Debugger.IsAttached ? TimeSpan.FromMinutes(10) : TimeSpan.FromMinutes(1);

        // todo: specific ip/port parameters? e.g. from OrleansHost

        private readonly Catalog catalog;

        private readonly HashSet<ISiloShutdownParticipant> shutdownParticipants;
        private readonly List<IHealthCheckParticipant> healthCheckParticipants;

        private int notifyingShutdown;

        internal static Silo CurrentSilo { get; private set; }

        /// <summary>
        /// Creates and initializes the silo from standard config file / location search.
        /// </summary>
        /// <param name="name">Name of this silo.</param>
        /// <param name="siloType">Type of this silo.</param>
        public Silo(string name, SiloType siloType)
            : this(name, siloType, GetDefaultConfig())
        {
        }

        /// <summary>
        /// Creates and initializes the silo from the specified config data.
        /// </summary>
        /// <param name="name">Name of this silo.</param>
        /// <param name="siloType">Type of this silo.</param>
        /// <param name="config">Silo config data to be used for this silo.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope",
            Justification = "Should not Dispose of messageCenter in this method because it continues to run / exist after this point.")]
        public Silo(string name, SiloType siloType, OrleansConfiguration config)
        {
            SystemStatus.Current = SystemStatus.Creating;

            CurrentSilo = this;

            DateTime startTime = DateTime.UtcNow;

            this.siloType = siloType;
            this.Name = name;

            this.shutdownFinishedEvent = new ManualResetEvent(false);
            this.siloTerminatedEvent = new ManualResetEvent(false);

            OrleansConfig = config;
            globalConfig = config.Globals;
            config.OnConfigChange("Defaults", () =>
                nodeConfig = config.GetConfigurationForNode(name));
            if (!Logger.IsInitialized) Logger.Initialize(nodeConfig);
            config.OnConfigChange("Defaults/Tracing", () => Logger.Initialize(nodeConfig, true), false);
            LimitManager.Initialize(nodeConfig);
            ActivationData.Init(config);
            StatisticsCollector.Initialize(nodeConfig);
            SerializationManager.Initialize(globalConfig.UseStandardSerializer);
            initTimeout = globalConfig.MaxJoinAttemptTime;
            if (Debugger.IsAttached)
            {
                initTimeout = StandardExtensions.Max(TimeSpan.FromMinutes(10), globalConfig.MaxJoinAttemptTime);
            }

            IPEndPoint here = nodeConfig.Endpoint;
            int generation = nodeConfig.Generation;
            if (generation == 0)
            {
                generation = SiloAddress.AllocateNewGeneration();
                nodeConfig.Generation = generation;
            }
            Logger.MyIPEndPoint = here;
            logger = Logger.GetLogger("Silo", Logger.LoggerType.Runtime);
            logger.Info(ErrorCode.SiloInitializing, "-------------- Initializing {0} silo on {1} at {2}, gen {3} --------------", siloType, nodeConfig.DNSHostName, here, generation);
            logger.Info(ErrorCode.SiloInitConfig, "Starting silo {0} with runtime Version='{1}' Config= \n{2}", name, Version.Current, config.ToString(name));

            shutdownParticipants = new HashSet<ISiloShutdownParticipant>();
            healthCheckParticipants = new List<IHealthCheckParticipant>();

            BufferPool.InitGlobalBufferPool(globalConfig);
            PlacementStrategy.Initialize(nodeConfig);

            UnobservedExceptionsHandlerClass.SetUnobservedExceptionHandler(UnobservedExceptionHandler);
            AppDomain.CurrentDomain.UnhandledException +=
                (object obj, UnhandledExceptionEventArgs ev) => DomainUnobservedExceptionHandler(obj, (Exception)ev.ExceptionObject);

            typeManager = new GrainTypeManager(here.Address.Equals(IPAddress.Loopback));
            shutdownParticipants.Add(typeManager);

            // Performance metrics
            siloStatistics = new SiloStatisticsManager(globalConfig, nodeConfig);
            config.OnConfigChange("Defaults/LoadShedding", () => siloStatistics.MetricsTable.NodeConfig = nodeConfig, false);

            // The scheduler -- TODO: pull the config params from the config file
            scheduler = new OrleansTaskScheduler(globalConfig, nodeConfig);
            OrleansTask.Initialize(scheduler);
            shutdownParticipants.Add(scheduler);
            healthCheckParticipants.Add(scheduler);
            //if (onIdle != null)
            //    scheduler.OnIdle = onIdle.Run;

            // Initialize the message center
            var mc = new MessageCenter(here, generation, globalConfig, siloStatistics.MetricsTable);
            if (nodeConfig.IsGatewayNode)
            {
                mc.InstallGateway(nodeConfig.ProxyGatewayEndpoint);
            }
            messageCenter = mc;

            // Now the router/directory service
            // This has to come after the message center //; note that it then gets injected back into the message center.;
            localGrainDirectory = new LocalGrainDirectory(this); 
            shutdownParticipants.Add(localGrainDirectory);

            // Now the activation directory.
            // This needs to know which router to use so that it can keep the global directory in synch with the local one.
            activationDirectory = new TargetDirectory();
            siloStatistics.MetricsTable.Scheduler = scheduler;
            siloStatistics.MetricsTable.ActivationDirectory = activationDirectory;
            siloStatistics.MetricsTable.MC = messageCenter;

            // Now the consistent ring provider
            consistentRingProvider = new ConsistentRingProvider(SiloAddress);
            //shutdownParticipants.Add((ISiloShutdownParticipant)consistentRingProvider); TODO: TMS make the ring shutdown-able?

            Action<Dispatcher> setDispatcher1;//, setDispatcher2;
            catalog = new Catalog(Constants.CatalogId, SiloAddress, Name, LocalGrainDirectory, typeManager, scheduler, activationDirectory, config, out setDispatcher1);
            var dispatcher = new Dispatcher(scheduler, messageCenter, catalog, config);
            setDispatcher1(dispatcher);
            GrainClient.Current = new InsideGrainClient(dispatcher, catalog, LocalGrainDirectory, SiloAddress, config, nodeConfig, consistentRingProvider, typeManager);
            shutdownParticipants.Add(InsideGrainClient.Current);
            messageCenter.RerouteHandler = InsideGrainClient.Current.RerouteMessage;
            messageCenter.SniffIncomingMessage = InsideGrainClient.Current.SniffIncomingMessage;
            shutdownParticipants.Add((ISiloShutdownParticipant)messageCenter);
            messageCenter.ClientDropHandler = grainIds =>
            {
                catalog.DeleteGrainsLocal(grainIds).Ignore();
                scheduler.RunOrQueueAction(() =>
                {
                    // todo: batch delete
                    foreach (var id in grainIds)
                    {
                        LocalGrainDirectory.DeleteGrain(id).Ignore();
                    }
                }, catalog.SchedulingContext);
            };
#if ALLOW_GRAPH_PARTITION_STRATEGY
            if (Constants.ALLOW_GRAPH_PARTITION_STRATEGY)
            {
                GraphPartitionDirector.Init(Name, siloStatistics, catalog, SiloAddress, scheduler);
            }
#endif

            // Now the incoming message agents
            incomingSystemAgent = new IncomingMessageAgent(Message.Categories.System, messageCenter, activationDirectory, scheduler, dispatcher);
            incomingPingAgent = new IncomingMessageAgent(Message.Categories.Ping, messageCenter, activationDirectory, scheduler, dispatcher);
            incomingAgent = new IncomingMessageAgent(Message.Categories.Application, messageCenter, activationDirectory, scheduler, dispatcher);
            shutdownParticipants.Add(incomingSystemAgent);
            shutdownParticipants.Add(incomingPingAgent);
            shutdownParticipants.Add(incomingAgent);
            shutdownParticipants.Add(new SiloShutdown());

            membershipFactory = new MembershipFactory();
            reminderFactory = new LocalReminderServiceFactory();

            SystemStatus.Current = SystemStatus.Created;

            StringValueStatistic.FindOrCreate(StatNames.STAT_SILO_START_TIME,
                    () =>
                    {
                        return Logger.PrintDate(startTime); // this will help troubleshoot production deployment when looking at MDS logs.
                    });

            TestHookup = new TestHookups(this);

            logger.Info(ErrorCode.SiloInitializingFinished, "-------------- Started silo {0}, ConsistentHashCode {1:X} --------------", SiloAddress.ToLongString(), SiloAddress.GetConsistentHashCode());
        }

        private static OrleansConfiguration GetDefaultConfig()
        {
            // Load the silo configuration
            OrleansConfiguration config = new OrleansConfiguration();
            config.StandardLoad();
            return config;
        }

        private void CreateSystemTargets()
        {
            RegisterSystemTarget(new SiloControl(this));

            RegisterSystemTarget(LocalGrainDirectory.RemGrainDirectory);
            RegisterSystemTarget(LocalGrainDirectory.CacheValidator);

            RegisterSystemTarget(new ClientObserverRegistrar(SiloAddress, LocalMessageCenter, LocalGrainDirectory));
            RegisterSystemTarget(new TypeManager(SiloAddress, LocalTypeManager));

            RegisterSystemTarget((SystemTarget)membershipOracle);
#if ALLOW_GRAPH_PARTITION_STRATEGY
            if (Constants.ALLOW_GRAPH_PARTITION_STRATEGY)
            {
                RegisterSystemTarget(GraphPartitionDirector.LocalSystemTarget);
            }
#endif
        }

        private void InjectDependancies()
        {
            healthCheckParticipants.Add(membershipOracle);
            shutdownParticipants.Add(membershipOracle);

            catalog.SiloStatusOracle = LocalSiloStatusOracle;
            localGrainDirectory.CatalogSiloStatusListener = catalog;
            LocalSiloStatusOracle.SubscribeToSiloStatusEvents(localGrainDirectory);
            messageCenter.SiloDeadOracle = LocalSiloStatusOracle.IsDeadSilo;

            // consistentRingProvider is not a system target per say, but it behaves like the localGrainDirectory, so it is here
            LocalSiloStatusOracle.SubscribeToSiloStatusEvents((ISiloStatusListener)consistentRingProvider);

            // start the reminder service system target
            reminderService = reminderFactory.CreateReminderService(this).WithTimeout(initTimeout).Result;
            //shutdownParticipants.Add((ISiloShutdownParticipant)consistentRingProvider); TODO: TMS make the ring shutdown-able?
            RegisterSystemTarget((SystemTarget)reminderService);
            consistentRingProvider.SubscribeToRangeChangeEvents((IRingRangeListener)reminderService);
            
            RegisterSystemTarget(catalog);
            scheduler.QueueAction(() => catalog.Start(), catalog.SchedulingContext)
                .Wait(initTimeout);

            // SystemTarget for provider init calls
            providerManagerSystemTarget = new ProviderManagerSystemTarget(this);
            RegisterSystemTarget(providerManagerSystemTarget);

            // SystemTarget for unit testing.
            var testSystemTarget = new TestSystemTarget(this);
            RegisterSystemTarget(testSystemTarget);  
        }

        private async Task CreateSystemGrains()
        {
            if (siloType == SiloType.Primary)
            {
#if PARALLEL_INIT_SYS_GRAINS
                AsyncCompletion[] systemGrainInits = new [] {
                    membershipFactory.CreateMembershipTableProvider(catalog, this),
                    reminderFactory.CreateReminderTableProvider(catalog, this)
                };
                AsyncCompletion.JoinAll(systemGrainInits).Wait(initTimeout);
#else
                await membershipFactory.CreateMembershipTableProvider(catalog, this).WithTimeout(initTimeout);
                await reminderFactory.CreateReminderTableProvider(catalog, this).WithTimeout(initTimeout);
#endif
            }
        }

        /// <summary> Perform silo startup operations. </summary>
        public void Start()
        {
            lock (this)
            {
                if (SystemStatus.Current != SystemStatus.Created)
                {
                    throw new InvalidOperationException(String.Format("Calling Silo.Start() on a silo which is not in the Start state. This silo is in the {0} state.", SystemStatus.Current));
                }
                SystemStatus.Current = SystemStatus.Starting;
            }

            logger.Info(ErrorCode.SiloStarting, "Silo Start()");

            // Hook up to receive notification of process exit / Ctrl-C events
            AppDomain.CurrentDomain.ProcessExit += HandleProcessExit;
            Console.CancelKeyPress += HandleProcessExit;
            // Hook up to receive notification of Azure role stopping events
            try
            {
                if (GlobalConfig.RunsInAzure && RoleEnvironment.IsAvailable)
                {
                    RoleEnvironment.Stopping += HandleAzureRoleStopping;
                }
            }
            catch (Exception exc)
            {
                logger.Warn(ErrorCode.CannotCheckRoleEnvironment,
                    "Ignoring problem checking availability of Azure RoleEnvironment - RoleEnvironment.Stopping event handler not registered", exc);
            }

            ConfigureThreadPoolAndServicePointSettings();

            // This has to start first so that the directory system target factory gets loaded before we start the router.
            typeManager.Start();
            InsideGrainClient.Current.Start();

            // The order of these 4 is pretty much arbitrary.
            scheduler.Start();
            messageCenter.Start();
            incomingPingAgent.Start();
            incomingSystemAgent.Start();
            incomingAgent.Start();

            LocalGrainDirectory.Start();

            // Set up an execution context for this thread so that the target creation steps can use asynch values.
            RuntimeContext.InitializeThread();

            // can call SetSiloMetricsTableDataManager only after MessageCenter is created (dependency on this.SiloAddress).
            siloStatistics.SetSiloStatsTableDataManager(globalConfig, nodeConfig, this.Name, this.SiloAddress).WaitWithThrow(initTimeout);
            siloStatistics.SetSiloMetricsTableDataManager(globalConfig, nodeConfig, this.Name, this.SiloAddress).WaitWithThrow(initTimeout);

            membershipOracle = membershipFactory.CreateMembershipOracle(this).WaitForResultWithThrow(initTimeout);
            
            // This has to follow the above steps that start the runtime components
            CreateSystemTargets();

            InjectDependancies();

            // ensure this runs in the grain context, wait for it to complete
            scheduler.QueueTask(CreateSystemGrains, catalog.SchedulingContext)
                .Wait(initTimeout);

            // Initialize storage providers once we have a basic silo runtime environment operating
            storageProviderManager = new StorageProviderManager();
            scheduler.RunOrQueueAsyncCompletion(
                () => AsyncCompletion.FromTask(storageProviderManager.LoadStorageProviders(GlobalConfig.ProviderConfigurations)),
                    providerManagerSystemTarget.SchedulingContext)
                        .Wait(initTimeout);
            catalog.SetStorageManager(storageProviderManager);

            ISchedulingContext statusOracleContext = ((SystemTarget)LocalSiloStatusOracle).SchedulingContext;
            bool waitForPrimaryToStart_1 = (siloType != SiloType.Primary) && GlobalConfig.LivenessType.Equals(GlobalConfiguration.LivenessProviderType.MembershipTableGrain);
            scheduler.RunOrQueueAsyncCompletion(() => AsyncCompletion.FromTask(LocalSiloStatusOracle.Start(waitForPrimaryToStart_1)), statusOracleContext)
                .Wait(initTimeout);
            scheduler.RunOrQueueAsyncCompletion(() =>  AsyncCompletion.FromTask(LocalSiloStatusOracle.BecomeActive()), statusOracleContext)
                .Wait(initTimeout);

            try
            {
                siloStatistics.Start(LocalConfig);

                // Finally, initialize the deployment load collector, for grains with load-based placement
                DeploymentLoadCollector.Initialize(LoadAwareDirector.STATISTICS_FRESHNESS_TIME).WaitWithThrow(initTimeout);

                // Start background timer tick to watch for platform execution stalls, such as when GC kicks in
                platformWatchdog = new Watchdog(nodeConfig.StatisticsLogWriteInterval, healthCheckParticipants);
                platformWatchdog.Start();

                bool waitForPrimaryToStart_2 = (siloType != SiloType.Primary) && GlobalConfig.ReminderServiceType.Equals(GlobalConfiguration.ReminderServiceProviderType.ReminderTableGrain);
                // so, we have the view of the membership in the consistentRingProvider. We can start the reminder service
                scheduler.RunOrQueueAsyncCompletion(() =>
                    AsyncCompletion.FromTask(reminderService.Start(waitForPrimaryToStart_2)),
                    ((SystemTarget)reminderService).SchedulingContext)
                    .Wait(initTimeout);                

#if !DISABLE_STREAMS
                // Initialize stream providers once we have a basic silo runtime environment operating
                var siloStreamProviderManager = new Orleans.Streams.StreamProviderManager();
                scheduler.RunOrQueueAsyncCompletion(
                    () => AsyncCompletion.FromTask(siloStreamProviderManager.LoadStreamProviders(this.GlobalConfig.ProviderConfigurations, SiloProviderRuntime.Instance)),
                        providerManagerSystemTarget.SchedulingContext)
                            .Wait(initTimeout);
                InsideGrainClient.Current.CurrentStreamProviderManager = siloStreamProviderManager;
#endif

                AppBootstrapManager appBootstrapManager = new AppBootstrapManager();
                scheduler.RunOrQueueAsyncCompletion(
                    () => AsyncCompletion.FromTask(appBootstrapManager.LoadAppBootstrapProviders(GlobalConfig.ProviderConfigurations)),
                        providerManagerSystemTarget.SchedulingContext)
                            .Wait(initTimeout);
                this.BootstrapProviders = appBootstrapManager.GetProviders(); // Data hook for testing & diagnotics

                // Now that we're active, we can start the gateway
                var mc = messageCenter as MessageCenter;
                if (mc != null)
                {
                    mc.StartGateway();
                }

                SystemStatus.Current = SystemStatus.Running;
            }
            catch (Exception)
            {
                FastKill(); // if failed after MBR became active, mark itself as dead in MBR abale.
                throw;
            }
        }

        private void ConfigureThreadPoolAndServicePointSettings()
        {
            if (nodeConfig.MinDotNetThreadPoolSize > 0)
            {
                int workerThreads;
                int completionPortThreads;
                ThreadPool.GetMinThreads(out workerThreads, out completionPortThreads);
                if (nodeConfig.MinDotNetThreadPoolSize > workerThreads ||
                    nodeConfig.MinDotNetThreadPoolSize > completionPortThreads)
                {
                    // if at least one of the new values is larger, set the new min values to be the larger of the prev. and new config value.
                    int newWorkerThreads = Math.Max(nodeConfig.MinDotNetThreadPoolSize, workerThreads);
                    int newCompletionPortThreads = Math.Max(nodeConfig.MinDotNetThreadPoolSize, completionPortThreads);
                    bool ok = ThreadPool.SetMinThreads(newWorkerThreads, newCompletionPortThreads);
                    if (ok)
                    {
                        logger.Info(ErrorCode.SiloConfiguredThreadPool,
                                    "Configured ThreadPool.SetMinThreads() to values: {0},{1}. Previous values are: {2},{3}.",
                                    newWorkerThreads, newCompletionPortThreads, workerThreads, completionPortThreads);
                    }
                    else
                    {
                        logger.Warn(ErrorCode.SiloFailedToConfigureThreadPool,
                                    "Failed to configure ThreadPool.SetMinThreads(). Tried to set values to: {0},{1}. Previous values are: {2},{3}.",
                                    newWorkerThreads, newCompletionPortThreads, workerThreads, completionPortThreads);
                    }
                }
            }

            // Set .NET ServicePointManager settings to optimize throughput performance when using Azure storage
            // http://blogs.msdn.com/b/windowsazurestorage/archive/2010/06/25/nagle-s-algorithm-is-not-friendly-towards-small-requests.aspx
            logger.Info(ErrorCode.SiloConfiguredServicePointManager, 
                "Setting .NET ServicePointManager config to Expect100Continue={0} DefaultConnectionLimit={1} UseNagleAlgorithm={2} to improve Azure storage performance",
                nodeConfig.Expect100Continue, nodeConfig.DefaultConnectionLimit, nodeConfig.UseNagleAlgorithm);
            ServicePointManager.Expect100Continue = nodeConfig.Expect100Continue;
            ServicePointManager.DefaultConnectionLimit = nodeConfig.DefaultConnectionLimit;
            ServicePointManager.UseNagleAlgorithm = nodeConfig.UseNagleAlgorithm;
        }

        internal void SetNamingServiceProvider(IMembershipNamingService namingServiceProvider)
        {
            this.membershipFactory.NamingServiceProvider = namingServiceProvider;
        }

        /// <summary>
        /// Gracefully stop the run time system only, but not the application. 
        /// Applications requests would be abruptly terminated, while the internal system state gracefully stopped and saved as much as possible.
        /// </summary>
        //onenote:///\\research\root\orleans\Orleans%20Discussions\Implementation.one#Fast%20Stopping&section-id={D01B6C91-F482-4EE9-9C3A-625552EE1136}&page-id={1337605A-B663-445E-AF12-D5A181563E25}&end
        public void Stop()
        {
            bool shutdownAlreadyInProgress = false;
            lock (this)
            {
                if (SystemStatus.Current == SystemStatus.Stopping || SystemStatus.Current == SystemStatus.ShuttingDown || SystemStatus.Current == SystemStatus.Terminated)
                {
                    shutdownAlreadyInProgress = true;
                    // Drop through to wait below
                }
                else if (SystemStatus.Current != SystemStatus.Running)
                {
                    throw new InvalidOperationException(String.Format("Calling Silo.Stop() on a silo which is not in the Running state. This silo is in the {0} state.", SystemStatus.Current));
                }
                else
                {
                    SystemStatus.Current = SystemStatus.Stopping;
                }
            }

            if (shutdownAlreadyInProgress)
            {
                logger.Info(ErrorCode.SiloShutdownInProgress, "Silo shutdown is in progress - Will wait for shutdown to finish");
                var pause = TimeSpan.FromSeconds(1);
                while (SystemStatus.Current != SystemStatus.Terminated)
                {
                    logger.Info(ErrorCode.WaitingForSiloShutdown, "Waiting {0} for shutdown to complete", pause);
                    Thread.Sleep(pause);
                }
                return;
            }

            try
            {
                try
                {
                    logger.Info(ErrorCode.SiloStopping, "Silo starting to Stop()");
                    // 1: Write "Stopping" state in the table + broadcast gossip msgs to re-read the table to everyone
                    scheduler.RunOrQueueAsyncCompletion(() => AsyncCompletion.FromTask(LocalSiloStatusOracle.Stop()), ((SystemTarget)LocalSiloStatusOracle).SchedulingContext)
                        .Wait(stopTimeout);
                }
                catch (Exception exc)
                {
                    logger.Error(ErrorCode.SiloFailedToStopMBR, "Failed to Stop() LocalSiloStatusOracle. About to FastKill this silo.", exc);
                    return; // will go to finally
                }
            
                // 2: Stop the gateway
                SafeExecute(messageCenter.StopAcceptingClientMessages);

                // 3: Start rejecting all silo to silo application messages
                SafeExecute(messageCenter.BlockApplicationMessages);

                // 4: Stop scheduling/executing application turns
                SafeExecute(scheduler.StopApplicationTurns);

                // 5: Directory: Speed up directory replication
                // will be started automatically when directory receives SiloStatusChangeNotification(Stopping)

                // 6. Stop reminder service
                scheduler.RunOrQueueAsyncCompletion(() => AsyncCompletion.FromTask(reminderService.Stop()), ((SystemTarget)reminderService).SchedulingContext)
                    .Wait(stopTimeout);
                
                // 7
                SafeExecute(() => LocalGrainDirectory.StopPreparationCompletion.WaitWithThrow(TimeSpan.FromSeconds(5)));
            }
            finally
            {
                // 8, 9, 10: Write Dead in the table, Drain scheduler, Stop msg center, ...
                FastKill();
                logger.Info(ErrorCode.SiloStopped, "Silo is Stopped()");
            }
        }

        /// <summary>
        /// Ungracefully stop the run time system and the application running on it. 
        /// Applications requests would be abruptly terminated, and the internal system state quickly stopped with minimal cleanup.
        /// </summary>
        private void FastKill()
        {
            if (!GlobalConfig.LivenessType.Equals(GlobalConfiguration.LivenessProviderType.MembershipTableGrain))
            {
                // do not execute KillMyself if using MembershipTableGrain, since it will fail, as we've already stopped app scheduler turns.
                SafeExecute(
                    () => scheduler.RunOrQueueAsyncCompletion(
                            () => AsyncCompletion.FromTask(LocalSiloStatusOracle.KillMyself()), 
                            ((SystemTarget)LocalSiloStatusOracle).SchedulingContext
                        ).Wait(stopTimeout)
                );
            }

            // incoming messages
            SafeExecute(incomingSystemAgent.Stop);
            SafeExecute(incomingPingAgent.Stop);
            SafeExecute(incomingAgent.Stop);

            // timers
            SafeExecute(InsideGrainClient.Current.Stop);
            SafeExecute(platformWatchdog.Stop);
            SafeExecute(scheduler.Stop);
            SafeExecute(activationDirectory.PrintActivationDirectory);
            SafeExecute(messageCenter.Stop);
            SafeExecute(siloStatistics.Stop);
            SafeExecute(Logger.Close);

            SystemStatus.Current = SystemStatus.Terminated;
            siloTerminatedEvent.Set();

            // Start sequence
            //scheduler;
            //typeManager
            //localGrainDirectory
            //GrainClient.Current
            //messageCenter
            //incomingSystemAgent
            //incomingPingAgent
            //incomingAgent
            //SiloShutdown
            //LocalSiloStatusOracle
            //coordinator
            //platformHeartbeatTimer
            //Logger
        }

        private void SafeExecute(Action action)
        {
            Utils.SafeExecute(action, logger, "Silo.Stop");
        }

        private void HandleAzureRoleStopping(object sender, RoleEnvironmentStoppingEventArgs e)
        {
            // Try to perform gracefull shutdown of Silo when we detect Azure role instance is being stopped
            logger.Info(ErrorCode.SiloStopping, "Silo.HandleAzureRoleStopping() - starting to shutdown silo");
            Stop();
        }

        private void HandleProcessExit(object sender, EventArgs e)
        {
            // !!!!! NOTE: We need to minimize the amount of processing occurring on this code path -- we only have under approx 2-3 seconds before process exit will occur
            logger.Warn(ErrorCode.Runtime_Error_100220, "Process is exiting");
            Logger.Flush();

            try
            {
                lock (this)
                {
                    if (SystemStatus.Current != SystemStatus.Running)
                    {
                        return;
                    }
                    SystemStatus.Current = SystemStatus.Stopping;
                }
                //if (!Debugger.IsAttached)
                {
                    if (TestHookup.ExecuteFastKillInProcessExit)
                    {
                        logger.Info(ErrorCode.SiloStopping, "Silo.HandleProcessExit() - starting to FastKill()");
                        FastKill();
                    }
                }
            }
            finally
            {
                Logger.Close();
            }
        }

        #region Implementation of ShutDown

        /// <summary>
        /// Gracefully shut down the run time system and the application running on it. 
        /// Applications requests would be gracefully allowed to terminate, and the internal system state gracefully stopped and saved as much as possible.
        /// </summary>
        public void ShutDown()
        {
            lock (this)
            {
                if (SystemStatus.Current != SystemStatus.Running)
                {
                    throw new InvalidOperationException(String.Format("Calling Silo.Shutdown() on a silo which is not in the Running state. This silo is in the {0} state.", SystemStatus.Current));
                }
                SystemStatus.Current = SystemStatus.ShuttingDown;
            }

            //if (SystemStatus.Current == SystemStatus.Stopping || SystemStatus.Current == SystemStatus.Stopped)
            //    return;

            // todo: review - use oracle
            //SystemStatus.Current = SystemStatus.Stopping;

            logger.Info(ErrorCode.SiloShuttingDown, "Silo starting to ShutDown()");

            // Start controlled shutdown process.
            // If we are already in the Stopped state, the scheduler was already stopped and ClosureWorkItem will not run.
            notifyingShutdown = shutdownParticipants.Count;
            foreach (var participant in shutdownParticipants)
            {
                var info = String.Format("BeginShutdown {0}", participant.GetType().Name);
                NotifyShutdown(participant,
                    p =>
                    {
                        if (logger.IsVerbose) logger.Verbose(info);
                        p.BeginShutdown(TryFinishShutdown);
                        notifyingShutdown--;
                        if (notifyingShutdown == 0)
                            TryFinishShutdown();
                    }, info, true);
            }
            if (logger.IsVerbose) logger.Verbose("Shutdown waiting for FinishShutdown");
            if (!shutdownFinishedEvent.WaitOne(TimeSpan.FromSeconds(30)))
            {
                if (logger.IsVerbose) logger.Verbose("Timeout waiting for FinishShutdown");
                FinishShutdown(); // ensure shutdown
            }
            SystemStatus.Current = SystemStatus.Terminated;
            siloTerminatedEvent.Set();
        }

        /// <summary>
        /// During shutdown, called by a participant to indicate that it is ready to finish shutdown.
        /// Each participant must eventually invoke this after BeginShutdown() and after CanFinishShutdown()
        /// returns false
        /// </summary>
        /// <remarks>
        /// The shutdown process begins when the silo changes status to ShuttingDown.
        /// The silo calls BeginShutdown() on each participant.
        /// When any participant transitions from "shutting down" to "ready to finish shutdown" (which
        /// might happen multiple times), it calls Silo.TryFinishShutdown().
        /// This will poll all participants with CanFinishShutdown().
        /// If any return false, it does nothing, and waits for the next call to TryFinishShutdown().
        /// If all return true, it will call FinishShutdown() on all participants.
        /// NOTE:
        /// It's still theoretically possible for new work to show up between CanFinishShutdown and
        /// FinishShutdown. A rigorous algorithm would hold a logical lock during the 2-phase commit,
        /// but it doesn't seem worth the complexity. Anything that gets missed should be properly
        /// handled by the code that handles unplanned silo failure.
        /// </remarks>
        private void TryFinishShutdown()
        {
            if (logger.IsVerbose) logger.Verbose("TryFinishShutdown {0} {1}", SystemStatus.Current, notifyingShutdown);
            if (SystemStatus.Current != SystemStatus.ShuttingDown || notifyingShutdown > 0)
                return;

            scheduler.QueueAction(() =>
            {
                if (logger.IsVerbose)
                {
                    var notready = shutdownParticipants.Where(p => !p.CanFinishShutdown()).ToList();
                    if (notready.Count > 0)
                        if (logger.IsVerbose) logger.Verbose("TryFinishShutdown waiting{0}", Utils.IEnumerableToString(notready));
                    else
                        FinishShutdown();
                    return;
                }
                if (shutdownParticipants.All(p => p.CanFinishShutdown()))
                {
                    FinishShutdown();
                }
            }, null).Ignore();
        }

        private void FinishShutdown()
        {
            if (logger.IsVerbose) logger.Verbose("FinishShutdown {0} locking", SystemStatus.Current);
            lock (shutdownParticipants)
            {
                if (logger.IsVerbose) logger.Verbose("FinishShutdown {0} inside lock", SystemStatus.Current);
                if (SystemStatus.Current != SystemStatus.ShuttingDown)
                    return;

                foreach (var participant in shutdownParticipants.OrderBy(p => p.Phase).ToList())
                {
                    var info = String.Format("FinishShutdown {0}", participant.GetType().Name);
                    if (logger.IsVerbose) logger.Verbose(info);
                    NotifyShutdown(participant, p => p.FinishShutdown(), info, true);
                }

                SystemStatus.Current = SystemStatus.Terminated;
            }
            if (logger.IsVerbose) logger.Verbose("FinishShutdown {0} outside lock", SystemStatus.Current);

            logger.Info(ErrorCode.SiloShutDown, "ShutDown done");

            Logger.Flush();

            shutdownFinishedEvent.Set();
        }

        private void NotifyShutdown(ISiloShutdownParticipant participant, Action<ISiloShutdownParticipant> action, string info, bool wait = false)
        {
            Action safeAction = () =>
            {
                try
                {
                    action(participant);
                }
                catch (Exception ex)
                {
                    logger.Warn(ErrorCode.Runtime_Error_100216, String.Format("Error in {0}", info), ex);
                }
            };
            var target = participant as SystemTarget;
            AsyncCompletion ac = scheduler.QueueAction(safeAction, target.SchedulingContext);
            if (!wait)
            {
                //scheduler.QueueWorkItem(new ClosureWorkItem(safeAction), target);
                ac.Ignore();
            }
            else
            {
                try
                {
                    ac.AsTask().WaitWithThrow(TimeSpan.FromSeconds(5));
                }
                catch (TimeoutException)
                {
                    logger.Warn(ErrorCode.Runtime_Error_100217, "Timeout during NotifyShutdown for {0}", info);
                }
            }
        }

        #endregion

        class SiloShutdown : ISiloShutdownParticipant
        {
            #region Implementation of ISiloShutdownParticipant

            public void BeginShutdown(Action tryFinishShutdown)
            {
                Logger.Flush();
                tryFinishShutdown();
            }

            public bool CanFinishShutdown()
            {
                return true;
            }

            public void FinishShutdown()
            {
                // nothing
            }

            public SiloShutdownPhase Phase { get { return SiloShutdownPhase.Middle; } }

            #endregion
        }

        /// <summary>
        /// Test hookup functions for white box testing.
        /// For internal use only.
        /// </summary>
        public class TestHookups : MarshalByRefObject
        {
            private Silo silo;
            internal bool ExecuteFastKillInProcessExit;
            internal IConsistentRingProvider ConsistentRingProvider { get { return silo.consistentRingProvider; } }

            internal TestHookups(Silo s)
            {
                silo = s;
                ExecuteFastKillInProcessExit = true;
            }
                        
            /// <summary>
            /// Get list of providers loaded in this silo.
            /// </summary>
            /// <returns></returns>
            internal IEnumerable<string> GetStorageProviderNames()
            {
                return silo.StorageProviderManager.GetProviderNames();
            }

            /// <summary>
            /// Find the named storage provider loaded in this silo.
            /// </summary>
            /// <returns></returns>
            internal IStorageProvider GetStorageProvider(string name)
            {
                return (IStorageProvider) silo.StorageProviderManager.GetProvider(name);
            }

            internal IBootstrapProvider GetBootstrapProvider(string name)
            {
                return silo.BootstrapProviders.First(p => p.Name == name);
            }

            internal void SuppressFastKillInHandleProcessExit()
            {
                ExecuteFastKillInProcessExit = false;
            }

            // this is only for white box testing - use GrainClient.Current.SendRequest instead
            internal void SendMessageInternal(Message message)
            {
                silo.messageCenter.SendMessage(message);
            }

            // For white-box testing only
            internal int UnregisterGrainForTesting(GrainId grain)
            {
                return silo.catalog.UnregisterGrainForTesting(grain);
            }

            // For white-box testing only
            internal void SetDirectoryLazyDeregistrationDelay_ForTesting(TimeSpan timeSpan)
            {
                silo.OrleansConfig.Globals.DirectoryLazyDeregistrationDelay = timeSpan;
            }
            // For white-box testing only
            internal void SetMaxForwardCount_ForTesting(int val)
            {
                silo.OrleansConfig.Globals.MaxForwardCount = val;
            }
        }

        private void UnobservedExceptionHandler(ISchedulingContext context, Exception exception)
        {
            OrleansContext orleansContext = context as OrleansContext;
            if (orleansContext == null)
            {
                if (context == null)
                    logger.Error(ErrorCode.Runtime_Error_100102, String.Format("Silo caught an UnobservedException with context==null."), exception);
                else
                    logger.Error(ErrorCode.Runtime_Error_100103, String.Format("Silo caught an UnobservedException with context of type different than OrleansContext. The type of the context is {0}. The context is {1}",
                        context.GetType(), context), exception);
            }
            else
            {
                logger.Error(ErrorCode.Runtime_Error_100104, String.Format("Silo caught an UnobservedException thrown by {0}.", orleansContext.Activation), exception);
            }   
        }

        private void DomainUnobservedExceptionHandler(object context, Exception exception)
        {
            if (context is ISchedulingContext)
            {
                UnobservedExceptionHandler(context as ISchedulingContext, exception);
            }
            else
            {
                logger.Error(ErrorCode.Runtime_Error_100324, String.Format("Called DomainUnobservedExceptionHandler with context {0}.", context), exception);
            }
        }

        internal void RegisterSystemTarget(SystemTarget target)
        {
            scheduler.RegisterWorkContext(target.SchedulingContext);
            activationDirectory.RecordNewSystemTarget(target);
        }

        internal void UnregisterSystemTarget(SystemTarget target)
        {
            activationDirectory.RemoveSystemTarget(target);
            scheduler.UnregisterWorkContext(target.SchedulingContext);
        }

        /// <summary> Return dump of diagnostic data from this silo. </summary>
        /// <param name="all"></param>
        /// <returns>Debug data for this silo.</returns>
        public string GetDebugDump(bool all = true)
        {
            var sb = new StringBuilder();            
            foreach (SystemTarget target in activationDirectory.AllSystemTargets())
            {
                sb.AppendFormat("System target {0}:", Constants.SystemTargetName(target.Grain)).AppendLine();               
            }

            var enumerator = activationDirectory.GetEnumerator();
            while(enumerator.MoveNext())
            {
                ActivationData activationData = enumerator.Current.Value;
                var workItemGroup = scheduler.GetWorkItemGroup(new OrleansContext(activationData));
                if (workItemGroup == null)
                {
                    sb.AppendFormat("Activation with no work item group!! Grain {0}, activation {1}.", activationData.Grain,
                                    activationData.ActivationId);
                    sb.AppendLine();
                    continue;
                }

                if (all || activationData.IsUsable)
                {
                    sb.AppendLine(workItemGroup.DumpStatus());

                    sb.AppendLine(activationData.DumpStatus());
                }
            }
            logger.Info(ErrorCode.SiloDebugDump, sb.ToString());
            return sb.ToString();
        }

        //public static string GetSiloDebugDump(bool all = true, bool fork = false)
        //{
        //    if (!fork)
        //        return CurrentSilo.GetDebugDump(all);
        //    AsyncCompletion.StartNew(() => CurrentSilo.GetDebugDump(all)).Ignore();
        //    return "Pending...";
        //}

        /// <summary> Object.ToString override -- summary info for this silo. </summary>
        public override string ToString()
        {
            return localGrainDirectory.ToString();
        }
    }

    // A dummy system target to use for scheduling context for provider Init calls, to allow them to make grain calls
    internal class ProviderManagerSystemTarget : SystemTarget
    {
        public ProviderManagerSystemTarget(Silo currentSilo)
            : base(Constants.ProviderManagerSystemTargetId, currentSilo.SiloAddress)
        {
        }
    }
}

