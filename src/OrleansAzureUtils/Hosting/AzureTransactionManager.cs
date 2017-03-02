using System;
using System.Diagnostics;
using Orleans.Runtime;
using Orleans.Runtime.Host;
using System.IO;


namespace Orleans.Transactions.Host
{
    /// <summary>
    /// Wrapper class for an Orleans Transaction Manager running in the current host process.
    /// </summary>
    public class AzureTransactionManager
    {
        /// <summary>
        /// The name of the configuration key value for locating the DataConnectionString setting from the Azure configuration for this role.
        /// Defaults to <c>DataConnectionString</c>
        /// </summary>
        public string DataConnectionConfigurationSettingName { get; set; }

        
        private TransactionManagerHost host;
        private readonly Logger logger;
        private readonly IServiceRuntimeWrapper serviceRuntimeWrapper = new ServiceRuntimeWrapper();

        /// <summary>
        /// Constructor
        /// </summary>
        public AzureTransactionManager()
        {
            DataConnectionConfigurationSettingName = AzureConstants.DataConnectionConfigurationSettingName;

            logger = LogManager.GetLogger("OrleansAzureTransactionManager", LoggerType.Runtime);
        }

        #region Azure RoleEntryPoint methods

        /// <summary>
        /// Initialize this Orleans TM for execution with the current Azure deploymentId and role instance
        /// </summary>
        /// <returns><c>true</c> is the TM startup was successfull</returns>
        public bool Start()
        {
            return Start(null);
        }


        /// <summary>
        /// Initialize this Orleans TM for execution with the specified Azure deploymentId and role instance
        /// </summary>
        /// <param name="deploymentId">Azure DeploymentId this TM is running under</param>
        /// <param name="config">If null, Config data will be read from TM config file as normal, otherwise use the specified config data.</param>
        /// <returns><c>true</c> is the silo startup was successfull</returns>
        public bool Start(FileInfo config, string deploymentId = null)
        {
            
            // Program ident
            Trace.TraceInformation("Starting {0} v{1}", this.GetType().FullName, RuntimeVersion.Current);

            // Check if deployment id was specified
            if (deploymentId == null)
                deploymentId = serviceRuntimeWrapper.DeploymentId;

            // Read endpoint info for this instance from Azure config
            string instanceName = serviceRuntimeWrapper.InstanceName;

            // Configure this Orleans TM instance

            if (config == null)
            {
                host = new TransactionManagerHost(instanceName);
                host.LoadOrleansConfig(); // Load config from file + Initializes logger configurations
            }
            else
            {
                host = new TransactionManagerHost(instanceName, config); // Use supplied config file + Initializes logger configurations
            }

            var connectionString = serviceRuntimeWrapper.GetConfigurationSettingValue(DataConnectionConfigurationSettingName);

            AzureClient.Initialize();

            // Initialise this Orleans silo instance
            host.SetDeploymentId(deploymentId, connectionString);

            host.InitializeOrleansTM();
            logger.Info("Successfully initialized Orleans TM '{0}'.", host.Name);
            return StartTM();
        }

        /// <summary>
        /// Makes this Orleans TM begin executing and become active.
        /// Note: This method call will only return control back to the caller when the TM is shutdown.
        /// </summary>
        public void Run()
        {
            logger.Info("OrleansAzureHost entry point called");

            // Hook up to receive notification of Azure role stopping events
            serviceRuntimeWrapper.SubscribeForStoppingNotification(this, HandleAzureRoleStopping);

            if (host.IsStarted)
                host.WaitForOrleansTMShutdown();
            
            else
                throw new ApplicationException("TM failed to start correctly - aborting");
        }

        /// <summary>
        /// Stop this Orleans TM executing.
        /// </summary>
        public void Stop()
        {
            logger.Info("Stopping {0}", this.GetType().FullName);
            serviceRuntimeWrapper.UnsubscribeFromStoppingNotification(this, HandleAzureRoleStopping);
            host.StopOrleansTM();
            logger.Info("Orleans TM '{0}' shutdown.", host.Name);
        }

        #endregion

        private bool StartTM()
        {
            logger.Info("Starting Orleans Transaction Manager");

            bool ok = host.StartOrleansTM();

            if (ok)
                logger.Info("Successfully started Orleans Transaction Manager '{0}'.", host.Name);
            else
                logger.Error(ErrorCode.TransactionManager_TMFailedToStart, string.Format("Failed to start Orleans Transaction Manager '{0}'.", host.Name));
            
            return ok;
        }

        private void HandleAzureRoleStopping(object sender, object e)
        {
            // Try to perform gracefull shutdown of TM when we detect Azure role instance is being stopped
            logger.Info("HandleAzureRoleStopping - starting to shutdown Transaction Manager");
            host.StopOrleansTM();
        }
    }
}
