using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using Orleans.Host.Azure.Utils;
using Microsoft.WindowsAzure.ServiceRuntime;


namespace Orleans.Host.Azure.Client
{
    /// <summary>
    /// Utility class for initializing an Orleans client running inside Azure.
    /// </summary>
    public static class OrleansAzureClient
    {
        /// <summary>Number of retry attempts to make when searching for gateway silos to connect to.</summary>
        public static int MaxRetries { get; set; }
        /// <summary>Amount of time to pause before each retry attempt.</summary>
        public static TimeSpan StartupRetryPause { get; set; }

        static OrleansAzureClient()
        {
            StartupRetryPause = OrleansAzureConstants.STARTUP_TIME_PAUSE; // 5 seconds
            MaxRetries = OrleansAzureConstants.MAX_RETRIES;  // 120 x 5s = Total: 10 minutes
        }

        /// <summary>
        /// Whether the Orleans Azure client runtime has already been initialized
        /// </summary>
        /// <returns><c>true</c> if client runtime is already initialized</returns>
        public static bool IsInitialized { get { return OrleansClient.IsInitialized; } }

        /// <summary>
        /// Initialise the Orleans client runtime in this Azure process
        /// </summary>
        public static void Initialize()
        {
            InitializeImpl(null);
        }

        /// <summary>
        /// Initialise the Orleans client runtime in this Azure process
        /// </summary>
        /// <param name="orleansClientConfigFile">Location of the Orleans client config file to use for base config settings</param>
        /// <remarks>Any silo gateway address specified in the config file is ignored, and gateway endpoint info is read from the silo instance table in Azure storage instead.</remarks>
        public static void Initialize(FileInfo orleansClientConfigFile)
        {
            InitializeImpl(orleansClientConfigFile);
        }

        /// <summary>
        /// Initialise the Orleans client runtime in this Azure process
        /// </summary>
        /// <param name="clientConfigFilePath">Location of the Orleans client config file to use for base config settings</param>
        /// <remarks>Any silo gateway address specified in the config file is ignored, and gateway endpoint info is read from the silo instance table in Azure storage instead.</remarks>
        public static void Initialize(string clientConfigFilePath)
        {
            InitializeImpl(new FileInfo(clientConfigFilePath));
        }

        /// <summary>
        /// Initializes the Orleans client runtime in this Azure process from the provided client configuration object. 
        /// If the configuration object is null, the initialization fails. 
        /// </summary>
        /// <param name="config">A ClientConfiguration object.</param>
        public static void Initialize(ClientConfiguration config)
        {
            var instanceIndex = AzureConfigUtils.GetMyInstanceIndex();
            InitializeImpl(config, instanceIndex);
        }

        /// <summary>
        /// Initializes the Orleans client runtime in this Azure process from the provided client configuration object. 
        /// If the configuration object is null, the initialization fails. 
        /// </summary>
        /// <param name="config">A ClientConfiguration object.</param>
        /// <param name="instanceIndex">Index offset for this client instance. Value must be >= zero.</param>
        public static void Initialize(ClientConfiguration config, int instanceIndex)
        {
            InitializeImpl(config, instanceIndex);
        }

        /// <summary>
        /// Uninitializes the Orleans client runtime in this Azure process. 
        /// </summary>
        public static void Uninitialize()
        {
            if (OrleansClient.IsInitialized)
            {
                Trace.TraceInformation("Uninitializing connection to Orleans gateway silo.");
                OrleansClient.Uninitialize();
            }
        }

        #region Internal implementation of client initialization processing

        private static void InitializeImpl(FileInfo configFile)
        {
            if (OrleansClient.IsInitialized)
            {
                Trace.TraceInformation("Connection to Orleans gateway silo already initialized.");
                return;
            }

            ClientConfiguration config;
            try
            {
                if (configFile == null)
                {
                    Trace.TraceInformation("Looking for standard Orleans client config file");
                    config = ClientConfiguration.StandardLoad();
                }
                else
                {
                    string configFileLocation = configFile.FullName;
                    Trace.TraceInformation("Loading Orleans client config file {0}", configFileLocation);
                    config = ClientConfiguration.LoadFromFile(configFileLocation);
                }
            }
            catch (Exception ex)
            {
                var msg = String.Format("Error loading Orleans client configuration file {0} {1} -- unable to continue. {2}", configFile, ex.Message, Logger.PrintException(ex));
                Trace.TraceError(msg);
                throw new AggregateException(msg, ex);
            }

            Trace.TraceInformation("Overriding Orleans client config from Azure runtime environment.");
            config.DeploymentId = RoleEnvironment.DeploymentId;
            try
            {
                config.DataConnectionString = RoleEnvironment.GetConfigurationSettingValue(OrleansAzureConstants.DataConnectionConfigurationSettingName);
            }
            catch (RoleEnvironmentException ex)
            {
                var msg = string.Format("ERROR: No OrleansAzureClient role setting value '{0}' specified for this role -- unable to continue", OrleansAzureConstants.DataConnectionConfigurationSettingName);
                Trace.TraceError(msg);
                throw new AggregateException(msg, ex);
            }
            var instanceIndex = AzureConfigUtils.GetMyInstanceIndex();

            InitializeImpl(config, instanceIndex);
        }

        private static void InitializeImpl(ClientConfiguration config, int instanceIndex)
        {
            if (OrleansClient.IsInitialized)
            {
                Trace.TraceInformation("Connection to Orleans gateway silo already initialized.");
                return;
            }

            //// Find endpoint info for the gateway to this Orleans silo cluster
            //Trace.WriteLine("Searching for Orleans gateway silo via Orleans instance table...");
            var deploymentId = config.DeploymentId;
            var connectionString = config.DataConnectionString;
            if (String.IsNullOrEmpty(deploymentId))
            {
                throw new ArgumentException("Cannot connect to Azure silos with null deploymentId", "config.DeploymentId");
            }
            if (String.IsNullOrEmpty(connectionString))
            {
                throw new ArgumentException("Cannot connect to Azure silos with null connectionString", "config.DataConnectionString");
            }

            bool initSucceeded = false;
            Exception lastException = null;
            for (int i = 0; i < MaxRetries; i++)
            {
                try
                {
                    // Initialize will throw if cannot find GWs
                    OrleansClient.Initialize(config);
                    initSucceeded = true;
                    break;
                }
                catch (Exception exc) 
                {
                    lastException = exc;
                    Trace.TraceError("OrleansClient.Initialize failed with exc -- {0}. Will try again", exc.Message);
                }
                // Pause to let Primary silo start up and register
                Trace.TraceInformation("Pausing {0} awaiting silo and gateways registration for Deployment={1}", StartupRetryPause, deploymentId);
                Thread.Sleep(StartupRetryPause);
            }
            if (!initSucceeded)
            {
                OrleansException err;
                if (lastException != null)
                {
                    err = new OrleansException(String.Format("Could not Initialize OrleansClient for DeploymentId={0}. Last exception={1}", deploymentId, lastException.Message), lastException);
                }
                else
                {
                    err = new OrleansException(String.Format("Could not Initialize OrleansClient for DeploymentId={0}.", deploymentId));
                }
                Trace.TraceError("Error starting Orleans Azure client application -- {0} -- bailing. {1}", err.Message, Logger.PrintException(err));
                throw err;
            }
            // TODO: Should we be keeping a reference to OrleansClient or something related, to avoid GC problems?
        }

        #endregion
    }
}
