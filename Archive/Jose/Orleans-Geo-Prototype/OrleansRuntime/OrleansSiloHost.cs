using System;
using System.IO;
using System.Net;
using System.Runtime;
using System.Threading;
using Orleans.Runtime;

using System.Globalization;


namespace Orleans.Host.SiloHost
{
    /// <summary>
    /// Allows programmatically hosting an Orleans silo in the curent app domain.
    /// </summary>
    public class OrleansSiloHost : MarshalByRefObject, IDisposable
    {
        /// <summary> Name of this silo. </summary>
        public string SiloName { get; set; }
        /// <summary> Type of this silo - either <c>Primary</c> or <c>Secondary</c>. </summary>
        public Silo.SiloType SiloType { get; set; }
        /// <summary>
        /// Configuration file used for this silo.
        /// Changing this after the silo has started (when <c>ConfigLoaded == true</c>) will have no effect.
        /// </summary>
        public string ConfigFileName { get; set; }
        /// <summary>
        /// Directory to use for the trace log file written by this silo.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The values of <c>null</c> or <c>"None"</c> mean no log file will be written by Orleans Logger manager.
        /// </para>
        /// <para>
        /// When deciding The values of <c>null</c> or <c>"None"</c> mean no log file will be written by Orleans Logger manager.
        /// </para>
        /// </remarks>
        public string TraceFilePath { get; set; }
        /// <summary> Configuration data for the Orleans system. </summary>
        public OrleansConfiguration Config { get; set; }
        /// <summary> Configuration data for this silo. </summary>
        public NodeConfiguration NodeConfig { get; private set; }
        /// <summary> 
        /// Silo Debug flag. 
        /// If set to <c>true</c> then additional diagnostic info will be written during silo startup.
        ///  </summary>
        public bool Debug { get; set; }
        /// <summary>
        /// Whether the silo config has been loaded and initializing it's runtime config.
        /// </summary>
        /// <remarks>
        /// Changes to silo config properties will be ignored after <c>ConfigLoaded == true</c>.
        /// </remarks>
        public bool ConfigLoaded { get; private set; }
        /// <summary> Deployment Id (if any) for the cluster this silo is running in. </summary>
        public string DeploymentId { get; set; }
        /// <summary>
        /// Whether a missing config file should cause the silo startup process to fail, 
        /// or whether some default silo configs should be used.
        /// </summary>
        public bool FailOnMissingConfigFile { get; set; }
        /// <summary>
        /// Verbose flag. 
        /// If set to <c>true</c> then additional status and diagnostics info will be written during silo startup.
        /// </summary>
        public int Verbose { get; set; }

        /// <summary> Whether this silo started successfully and is currently running. </summary>
        public bool IsStarted { get; private set; }

        private Logger logger;
        private Silo orleans;
        private EventWaitHandle startupEvent;
        private bool disposed;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="siloName">Name of this silo.</param>
        public OrleansSiloHost(string siloName)
        {
            this.SiloName = siloName;
            this.SiloType = Silo.SiloType.Secondary; // Default
            this.IsStarted = false;
        }

        /// <summary>
        /// Initialize this silo.
        /// </summary>
        public void InitializeOrleansSilo()
        {
    #if DEBUG
            AssemblyLoaderUtils.EnableAssemblyLoadTracing();
    #endif

            try
            {
                if (!this.ConfigLoaded) LoadOrleansConfig();

                logger.Info(
                    ErrorCode.SiloInitializing,
                    "Initializing Silo {0} on host={1} CPU count={2} running .NET version='{3}' Is .NET 4.5={4} OS version='{5}'",
                    SiloName, Environment.MachineName, Environment.ProcessorCount, Environment.Version, OrleansConfiguration.IsNet45OrNewer(), Environment.OSVersion);

                logger.Info(ErrorCode.SiloGcSetting, "Silo running with GC settings: ServerGC={0} GCLatencyMode={1}", GCSettings.IsServerGC, Enum.GetName(typeof(GCLatencyMode), GCSettings.LatencyMode));
                if (!GCSettings.IsServerGC)
                {
                    logger.Warn(
                        ErrorCode.SiloGcWarning,
                        "Note: Silo not running with ServerGC turned on - recommend checking app config : <configuration>-<runtime>-<gcServer>");
                }

                this.orleans = new Silo(SiloName, SiloType, Config);
            }
            catch (Exception exc)
            {
                ReportStartupError(exc);
                this.orleans = null;
            }
        }

        /// <summary>
        /// Uninitialize this silo.
        /// </summary>
        public void UnInitializeOrleansSilo()
        {
            Utils.SafeExecute(() => UnobservedExceptionsHandlerClass.ResetUnobservedExceptionHandler());
            Utils.SafeExecute(Logger.UnInitialize);
        }

        /// <summary>
        /// Start this silo.
        /// </summary>
        /// <returns></returns>
        public bool StartOrleansSilo()
        {
            try
            {
                if (string.IsNullOrEmpty(Thread.CurrentThread.Name))
                {
                    Thread.CurrentThread.Name = this.GetType().Name;
                }

                if (this.orleans != null)
                {
                    this.orleans.Start();
                    
                    string startupEventName = SiloName;
                    logger.Info(ErrorCode.SiloStartupEventName, "Silo startup event name: {0}", startupEventName);

                    bool createdNew;
                    startupEvent = new EventWaitHandle(true, EventResetMode.ManualReset, startupEventName, out createdNew);
                    if (!createdNew)
                    {
                        logger.Info(ErrorCode.SiloStartupEventOpened, "Opened existing startup event. Setting the event {0}", startupEventName);
                        startupEvent.Set();
                    }
                    else
                    {
                        logger.Info(ErrorCode.SiloStartupEventCreated, "Created and set startup event {0}", startupEventName);
                    }

                    logger.Info(ErrorCode.SiloStarted, "Silo {0} started successfully", SiloName);
                    this.IsStarted = true;
                }
                else
                {
                    throw new InvalidOperationException("Cannot start silo " + this.SiloName +
                                                        " due to prior initialization error");
                }
            }
            catch (Exception exc)
            {
                ReportStartupError(exc);
                this.orleans = null;
                this.IsStarted = false;
                return false;
            }

            return true;
        }

        /// <summary>
        /// Stop this silo.
        /// </summary>
        public void StopOrleansSilo()
        {
            this.IsStarted = false;
            if (this.orleans != null) orleans.Stop();
        }

        /// <summary>
        /// Wait for this silo to shutdown.
        /// </summary>
        /// <remarks>
        /// Note: This method call will block execution of current thread, 
        /// and will not return control back to the caller until the silo is shutdown.
        /// </remarks>
        public void WaitForOrleansSiloShutdown()
        {
            if (!IsStarted)
            {
                throw new InvalidOperationException("Cannot wait for silo " + this.SiloName +
                                                    " since it was not started successfully previously.");
            }

            if (startupEvent != null)
            {
                startupEvent.Reset();
            }
            else
            {
                throw new InvalidOperationException("Cannot wait for silo " + this.SiloName +
                                                    " due to prior initialization error");
            }
            if (this.orleans != null)
            {
                this.orleans.SiloTerminatedEvent.WaitOne();
            }
            else
            {
                throw new InvalidOperationException("Cannot wait for silo " + this.SiloName +
                                                    " due to prior initialization error");
            }
        }

        /// <summary>
        /// Set the DeploymentId for this silo, 
        /// as well as the Azure connection string to use the silo system data, 
        /// such as the cluster membership table..
        /// </summary>
        /// <param name="deploymentId">DeploymentId this silo is part of.</param>
        /// <param name="connectionString">Azure connection string to use the silo system data.</param>
        public void SetDeploymentId(string deploymentId, string connectionString)
        {
            logger.Info(ErrorCode.SiloSetDeploymentId, "Setting Deployment Id to {0} and data connection string to {1}", deploymentId, ConfigUtilities.PrintDataConnectionInfo(connectionString));
            this.Config.Globals.DeploymentId = deploymentId;
            this.Config.Globals.DataConnectionString = connectionString;
        }
        
        /// <summary>
        /// Set the main endpoint address for this silo,
        /// plus the silo generation value to be used to distinguish this silo instance
        /// from any previous silo instances previously running on this endpoint.
        /// </summary>
        /// <param name="endpoint">IP address and port of the main inter-silo socket connection.</param>
        /// <param name="generation">Generation number for this silo.</param>
        public void SetSiloEndpoint(IPEndPoint endpoint, int generation)
        {
            logger.Info(ErrorCode.SiloSetSiloEndpoint, "Setting silo endpoint address to {0}:{1}", endpoint, generation);
            this.NodeConfig.HostNameOrIPAddress = endpoint.Address.ToString();
            this.NodeConfig.Port = endpoint.Port;
            this.NodeConfig.Generation = generation;
        }

        /// <summary>
        /// Set the gateway proxy endpoint address for this silo.
        /// </summary>
        /// <param name="endpoint">IP address of the gateway socket connection.</param>
        public void SetProxyEndpoint(IPEndPoint endpoint)
        {
            logger.Info(ErrorCode.SiloSetProxyEndpoint, "Setting silo proxy endpoint address to {0}", endpoint);
            this.NodeConfig.ProxyGatewayEndpoint = endpoint;
        }

        /// <summary>
        /// Set the seed node endpoint address to be used by silo.
        /// </summary>
        /// <param name="endpoint">IP address of the inter-silo connection socket on the seed node silo.</param>
        public void SetSeedNodeEndpoint(IPEndPoint endpoint)
        {
            logger.Info(ErrorCode.SiloSetSeedNode, "Adding seed node address={0} port={1}", endpoint.Address, endpoint.Port);
            this.Config.Globals.SeedNodes.Clear();
            this.Config.Globals.SeedNodes.Add(endpoint);
        }

        /// <summary>
        /// Set the set of seed node endpoint addresses to be used by silo.
        /// </summary>
        /// <param name="endpoints">IP addresses of the inter-silo connection socket on the seed node silos.</param>
        public void SetSeedNodeEndpoints(IPEndPoint[] endpoints)
        {
            // Add all silos as seed nodes
            this.Config.Globals.SeedNodes.Clear();
            foreach (IPEndPoint endpoint in endpoints)
            {
                logger.Info(ErrorCode.SiloAddSeedNode, "Adding seed node address={0} port={1}", endpoint.Address, endpoint.Port);

                this.Config.Globals.SeedNodes.Add(endpoint);
            }
        }

        /// <summary>
        /// Set the endpoint addresses for the Primary silo (if any).
        /// This silo may be Primary, in which case this address should match 
        /// this silo's inter-silo connection socket address.
        /// </summary>
        /// <param name="endpoint">The IP address for the inter-silo connection socket on the Primary silo.</param>
        public void SetPrimaryNodeEndpoint(IPEndPoint endpoint)
        {
            logger.Info(ErrorCode.SiloSetPrimaryNode, "Setting primary node address={0} port={1}", endpoint.Address, endpoint.Port);
            this.Config.PrimaryNode = endpoint;
            //if (this.NodeConfig.Endpoint.Equals(endpoint))
            //{
            //    this.SiloType = Silo.SiloType.Primary;
            //}
            //else
            //{
            //    this.SiloType = Silo.SiloType.Secondary;
            //}
        }

        /// <summary>
        /// Set the type of this silo. Default is Secondary.
        /// </summary>
        /// <param name="siloType">Type of this silo.</param>
        public void SetSiloType(Silo.SiloType siloType)
        {
            logger.Info(ErrorCode.SiloSetSiloType, "Setting silo type {0}", siloType);
            this.SiloType = siloType;
        }

        /// <summary>
        /// Set the local directory location of any local storage space that is available 
        /// for use by this silo.
        /// (Currently not used.)
        /// </summary>
        /// <param name="path"></param>
        internal void SetSiloLocalStorageLocation(string path)
        {
            logger.Info(ErrorCode.SiloSetWorkingDir, "Setting silo {0} working directory location={1}", this.SiloName, path);
            this.NodeConfig.WorkingStorageDirectory = path;
        }

        /// <summary>
        ///  Set the membership liveness type to be used by this silo.
        /// </summary>
        /// <param name="livenessType">Liveness type for this silo</param>
        public void SetSiloLivenessType(GlobalConfiguration.LivenessProviderType livenessType)
        {
            logger.Info(ErrorCode.SetSiloLivenessType, "Setting silo Liveness Provider Type={0}", livenessType);
            this.Config.Globals.LivenessType = livenessType;
        }

        /// <summary>
        ///  Set the membership liveness type to be used by this silo.
        /// </summary>
        /// <param name="livenessType">Liveness type for this silo</param>
        public void SetReminderServiceType(GlobalConfiguration.ReminderServiceProviderType reminderType)
        {
            logger.Info(ErrorCode.SetSiloLivenessType, "Setting silo Reminder Service Provider Type={0}", reminderType);
            this.Config.Globals.ReminderServiceType = reminderType;
        }

        /// <summary>
        ///  Set the membership naming service provider to be used by this silo.
        /// </summary>
        /// <param name="namingServiceProvider">Naming service provider for this silo</param>
        internal void SetNamingServiceProvider(IMembershipNamingService namingServiceProvider)
        {
            logger.Info(ErrorCode.SiloSetNamingServiceProvider, "Setting Naming Service Provider for silo {0}", this.SiloName);
            this.orleans.SetNamingServiceProvider(namingServiceProvider);
        }

        /// <summary>
        /// Report an error during silo startup.
        /// </summary>
        /// <remarks>
        /// Information on the silo startup issue will be logged to any attached Loggers,
        /// then a timestamped StartupError text file will be written to 
        /// the current working directory (if possible).
        /// </remarks>
        /// <param name="exc">Exception which caused the silo startup issue.</param>
        public void ReportStartupError(Exception exc)
        {
            if (string.IsNullOrWhiteSpace(SiloName))
                SiloName = "Silo";

            string errMsg = "ERROR starting Orleans silo name=" + SiloName + " Exception=" + exc;

            if (logger != null)  logger.Error(ErrorCode.Runtime_Error_100105, errMsg, exc);

    #if false
            AssemblyLoaderUtils.DumpLog();

            if (Debug)
            {
                // Break in Debugger so that we can check what is going on
                System.Diagnostics.Debugger.Break();
            }
    #endif

            // Dump Startup error to a log file
            //string startupLog = this.SiloName + "-StartupError.txt";
            const string dateFormat = "yyyy-MM-dd-HH.mm.ss.fffZ";
            string startupLog = SiloName + "-StartupError-" + DateTime.UtcNow.ToString(dateFormat, CultureInfo.InvariantCulture) + ".txt";

            try
            {
                File.AppendAllText(startupLog, Logger.PrintTime(DateTime.UtcNow) + "Z" + "\r\n" + errMsg);
            }
            catch (Exception exc2)
            {
                if (logger != null) logger.Error(ErrorCode.Runtime_Error_100106, "Error writing log file " + startupLog, exc2);
            }

            Logger.Flush();
        }

        /// <summary>
        /// Search for and load the config file for this silo.
        /// </summary>
        public void LoadOrleansConfig()
        {
            if (this.ConfigLoaded)
            {
                //logger.Warn("Config already loaded");
                return;
            }

            OrleansConfiguration config = this.Config ?? new OrleansConfiguration();

            if (this.ConfigFileName == null)
            {
                bool found = config.StandardLoad();
                if (!found)
                {
                    if (FailOnMissingConfigFile)
                    {
                        throw new FileNotFoundException("Unable to find Config file");
                    }
                    if (logger != null)
                    {
                        logger.Warn(ErrorCode.Runtime_Error_100107,
                            "Warning: Unable to find Config file -- Continuing with defaults.");
                    }
                }
            }
            else
            {
                try
                {
                    config.LoadFromFile(ConfigFileName);
                }
                catch (Exception ex)
                {
                    throw new AggregateException("Error loading Config file: " + ex.Message, ex);
                }
            }

            SetOrleansConfig(config);
        }

        /// <summary>
        /// Allows silo config to be programmatically set.
        /// </summary>
        /// <param name="config">Configuration data for this silo & cluster.</param>
        public void SetOrleansConfig(OrleansConfiguration config)
        {
            this.Config = config;

            if (this.Verbose > 0)
            {
                Config.Defaults.DefaultTraceLevel =
                    (OrleansLogger.Severity.Verbose - 1 + this.Verbose);
            }

            if (!String.IsNullOrEmpty(this.DeploymentId))
            {
                Config.Globals.DeploymentId = this.DeploymentId;
            }

            if (string.IsNullOrWhiteSpace(SiloName))
                throw new ArgumentException("SiloName not defined - cannot initialize config");

            this.NodeConfig = Config.GetConfigurationForNode(SiloName);
            this.SiloType = NodeConfig.IsPrimaryNode ? Silo.SiloType.Primary : Silo.SiloType.Secondary;

            if (this.TraceFilePath != null)
            {
                string traceFileName = Config.GetConfigurationForNode(SiloName).TraceFileName;
                if (traceFileName != null && !Path.IsPathRooted(traceFileName))
                {
                    Config.GetConfigurationForNode(SiloName).TraceFileName = this.TraceFilePath + "\\" + traceFileName;
                }
            }

            this.ConfigLoaded = true;

            InitializeLogger(config.GetConfigurationForNode(SiloName));
        }

        private void InitializeLogger(NodeConfiguration nodeCfg)
        {
            Logger.Initialize(nodeCfg);
            logger = Logger.GetLogger("OrleansSiloHost", Logger.LoggerType.Runtime);
        }

        /// <summary>
        /// Called when this silo is being Disposed by .NET runtime.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposed)
            {
                if (disposing)
                {
                    if (startupEvent != null)
                    {
                        startupEvent.Dispose();
                        startupEvent = null;
                    }
                    this.IsStarted = false;
                }
            }
            disposed = true;
        }
    }
}
