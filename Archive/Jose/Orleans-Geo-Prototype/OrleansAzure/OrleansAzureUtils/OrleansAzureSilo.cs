using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Threading;
using Microsoft.WindowsAzure.ServiceRuntime;
using Orleans.AzureUtils;
using Orleans.Host.Azure.Utils;
using Orleans.Host.SiloHost;
using Orleans.Runtime;


namespace Orleans.Host.Azure
{
    /// <summary>
    /// Wrapper class for an Orleans silo running in the current host process.
    /// </summary>
    public class OrleansAzureSilo
    {
        /// <summary>
        /// Amount of time to pause before retrying if a secondary silo is unable to connect to the primary silo for this deployment.
        /// Defaults to 5 seconds.
        /// </summary>
        public TimeSpan StartupRetryPause { get; set; }
        /// <summary>
        /// Number of times to retrying if a secondary silo is unable to connect to the primary silo for this deployment.
        /// Defaults to 120 times.
        /// </summary>
        public int MaxRetries { get; set; }
        /// <summary>
        /// The name of the configuration key value for locating the DataConnectionString setting from the Azure configuration for this role.
        /// Defaults to <c>DataConnectionString</c>
        /// </summary>
        public string DataConnectionConfigurationSettingName { get; set; }
        /// <summary>
        /// The name of the configuration key value for locating the OrleansSiloEndpoint setting from the Azure configuration for this role.
        /// Defaults to <c>OrleansSiloEndpoint</c>
        /// </summary>
        public string SiloEndpointConfigurationKeyName { get; set; }
        /// <summary>
        /// The name of the configuration key value for locating the OrleansProxyEndpoint setting from the Azure configuration for this role.
        /// Defaults to <c>OrleansProxyEndpoint</c>
        /// </summary>
        public string ProxyEndpointConfigurationKeyName { get; set; }
        /// <summary>
        /// The name of the configuration key value for locating the LocalStoreDirectory setting from the Azure configuration for this role.
        /// Defaults to <c>LocalStoreDirectory</c>
        /// </summary>
        public string SiloLocalStoreConfigurationKeyName { get; set; }

        private OrleansSiloHost host;
        private bool primaryNodeIsRequired;
        private OrleansSiloInstanceManager siloInstanceManager;
        private SiloInstanceTableEntry myEntry;
        private readonly Logger logger;

        /// <summary>
        /// Constructor
        /// </summary>
        public OrleansAzureSilo()
        {
            DataConnectionConfigurationSettingName = OrleansAzureConstants.DataConnectionConfigurationSettingName;
            SiloEndpointConfigurationKeyName = OrleansAzureConstants.SiloEndpointConfigurationKeyName;
            ProxyEndpointConfigurationKeyName = OrleansAzureConstants.ProxyEndpointConfigurationKeyName;
            SiloLocalStoreConfigurationKeyName = OrleansAzureConstants.SiloLocalStoreConfigurationKeyName;

            StartupRetryPause = OrleansAzureConstants.STARTUP_TIME_PAUSE; // 5 seconds
            MaxRetries = OrleansAzureConstants.MAX_RETRIES;  // 120 x 5s = Total: 10 minutes

            this.logger = Logger.GetLogger("OrleansAzureSilo", Logger.LoggerType.Runtime);
        }

        #region Azure RoleEntryPoint methods

        /// <summary>
        /// Initialize this Orleans silo for execution with the specified Azure deploymentId and role instance
        /// </summary>
        /// <param name="deploymentId">Azure DeploymentId this silo is running under</param>
        /// <param name="myRoleInstance">Azure role instance info this silo is running under</param>
        /// <returns><c>true</c> is the silo startup was successfull</returns>
        public bool Start(string deploymentId, RoleInstance myRoleInstance)
        {
            return Start(deploymentId, myRoleInstance, null);
        }

        /// <summary>
        /// Initialize this Orleans silo for execution with the specified Azure deploymentId and role instance
        /// </summary>
        /// <param name="deploymentId">Azure DeploymentId this silo is running under</param>
        /// <param name="myRoleInstance">Azure role instance info this silo is running under</param>
        /// <param name="config">If null, Config data will be read from silo config file as normal, otherwise use the specified config data.</param>
        /// <returns><c>true</c> is the silo startup was successfull</returns>
        public bool Start(string deploymentId, RoleInstance myRoleInstance, OrleansConfiguration config)
        {
            // Program ident
            Trace.TraceInformation("Starting {0} v{1}", this.GetType().FullName, Version.Current);

            // Read endpoint info for this instance from Azure config
            string instanceName = AzureConfigUtils.GetInstanceName(deploymentId, myRoleInstance);

            // Configure this Orleans silo instance

            this.host = new OrleansSiloHost(instanceName);
            if (config == null)
            {
                host.LoadOrleansConfig(); // Load config from file + Initializes logger configurations
            }
            else
            {
                host.SetOrleansConfig(config); // Use supplied config data + Initializes logger configurations
            }
            this.primaryNodeIsRequired = host.Config.Globals.PrimaryNodeIsRequired;

            RoleInstanceEndpoint myEndpoint = GetEndpointInfo(myRoleInstance, this.SiloEndpointConfigurationKeyName);
            RoleInstanceEndpoint proxyEndpoint = GetEndpointInfo(myRoleInstance, this.ProxyEndpointConfigurationKeyName);
            LocalResource myWorkingDirLocation = RoleEnvironment.GetLocalResource(this.SiloLocalStoreConfigurationKeyName);

            // Let silo #1 be the Primary
            bool isPrimarySilo = (primaryNodeIsRequired && AzureConfigUtils.GetMyInstanceIndex() == 0);
            host.SetSiloType(isPrimarySilo ? Silo.SiloType.Primary : Silo.SiloType.Secondary);

            int generation = SiloAddress.AllocateNewGeneration();

            // Bootstrap this Orleans silo instance

            myEntry = new SiloInstanceTableEntry
            {
                DeploymentId = deploymentId,
                Address = myEndpoint.IPEndpoint.Address.ToString(),
                Port = myEndpoint.IPEndpoint.Port.ToString(CultureInfo.InvariantCulture),
                Generation = generation.ToString(CultureInfo.InvariantCulture),

                HostName = host.Config.GetConfigurationForNode(host.SiloName).DNSHostName,
                //Status = INSTANCE_STATUS_REGISTERED,
                ProxyPort = (proxyEndpoint != null ? proxyEndpoint.IPEndpoint.Port : 0).ToString(CultureInfo.InvariantCulture),
                Primary = isPrimarySilo.ToString(),

                RoleName = myRoleInstance.Role.Name, 
                InstanceName = instanceName,
                UpdateZone = myRoleInstance.UpdateDomain.ToString(CultureInfo.InvariantCulture),
                FaultZone = myRoleInstance.FaultDomain.ToString(CultureInfo.InvariantCulture),
                StartTime = Logger.PrintDate(DateTime.UtcNow),

                PartitionKey = deploymentId,
                RowKey = myEndpoint.IPEndpoint.Address + "-" + myEndpoint.IPEndpoint.Port + "-" + generation
            };

            string connectionString = RoleEnvironment.GetConfigurationSettingValue(this.DataConnectionConfigurationSettingName);
            try
            {
                this.siloInstanceManager = OrleansSiloInstanceManager.GetManager(deploymentId, connectionString).WithTimeout(AzureTableDefaultPolicies.TableCreation_TIMEOUT).Result;
            }
            catch (Exception exc)
            {
                string error = String.Format("Failed to create OrleansSiloInstanceManager. This means CreateTableIfNotExist for silo instance table has failed with {0}", Logger.PrintException(exc));
                Trace.TraceError(error);
                logger.Error(ErrorCode.AzureTable_34, error, exc);
                throw exc;
            }
            ////// Always use Azure table for membership when running silo in Azure
            ////host.Config.Globals.LivenessType = GlobalConfiguration.LivenessProviderType.AzureTable;
            host.SetSiloLivenessType(GlobalConfiguration.LivenessProviderType.AzureTable);
            host.SetReminderServiceType(GlobalConfiguration.ReminderServiceProviderType.AzureTable);

            siloInstanceManager.RegisterSiloInstance(myEntry);

            // Initialise this Orleans silo instance

            host.SetDeploymentId(deploymentId, connectionString);
            host.SetSiloEndpoint(myEndpoint.IPEndpoint, generation);
            host.SetProxyEndpoint(proxyEndpoint.IPEndpoint);
            //host.SetSiloLocalStorageLocation(myWorkingDirLocation.RootPath);

            if (primaryNodeIsRequired)
            {
                IPEndPoint primarySilo = null;
                if (isPrimarySilo)
                {
                    primarySilo = myEndpoint.IPEndpoint;
                }
                else
                {
                    for (int i = 0; i < this.MaxRetries && primarySilo == null; i++)
                    {
                        // Pause to let Primary silo start up and register
                        logger.Info(ErrorCode.Runtime_Error_100286, "Secondary silo -- pausing {0} awaiting Primary silo registration", StartupRetryPause);
                        Thread.Sleep(StartupRetryPause);

                        // Find primary silo, and add it as the seed node
                        primarySilo = siloInstanceManager.FindPrimarySiloEndpoint();
                    }

                    if (primarySilo == null)
                    {
                        var err = new KeyNotFoundException("Could not find Primary silo address");
                        logger.Error(ErrorCode.Runtime_Error_100287, string.Format("Error starting Secondary silo -- {0} -- bailing", err.Message), err);
                        throw err;
                    }
                }
                host.SetPrimaryNodeEndpoint(primarySilo);
                host.SetSeedNodeEndpoint(primarySilo);
                // Find other active silos, and add them as seed nodes
                //Dictionary<string, IPEndPoint> siloEndpoints = siloInstanceManager.FindSiloEndpoints();
                //host.SetSeedNodeEndpoints(siloEndpoints.Values.ToArray<IPEndPoint>());
            }

            host.InitializeOrleansSilo();

            logger.Info(ErrorCode.Runtime_Error_100288, "Successfully initialized Orleans silo '{0}' as a {1} node.", host.SiloName, host.SiloType);

            if (host.SiloType == Silo.SiloType.Primary || !primaryNodeIsRequired)
            {
                // The silo is usually started in the Start() method of the role.

                // Only in a special case of needing a primary silo: 
                // in such a case the primary silo is started in the Start() and all other non-primary silos are started in Run.
                // We want to start this silo here in OnStart 
                // so that it is ready to accept connections at the start of the Run state 
                // when the Secondary silos will be started.

                return StartSilo();
            }

            return true;
        }

        private RoleInstanceEndpoint GetEndpointInfo(RoleInstance roleInstance, string endpointName)
        {
            try
            {
                return roleInstance.InstanceEndpoints[endpointName];
            }
            catch (Exception exc)
            {
                string errorMsg = string.Format(
                    "Unable to obtain endpoint info for role {0} from role config parameter {1} -- Endpoints defined = [{2}]",
                    roleInstance.Role.Name, endpointName, string.Join(", ", roleInstance.InstanceEndpoints)); 

                logger.Error(ErrorCode.SiloEndpointConfigError, errorMsg, exc);
                throw new OrleansException(errorMsg, exc);
            }
        }

        /// <summary>
        /// Makes this Orleans silo begin executing and become active.
        /// Note: This method call will only return control back to the caller when the silo is shutdown.
        /// </summary>
        public void Run()
        {
            logger.Info(ErrorCode.Runtime_Error_100289, "OrleansAzureHost entry point called");

            bool ok;

            if (host.SiloType == Silo.SiloType.Primary || !primaryNodeIsRequired)
            {
                // This is the Primary silo, so we must have started correctly if we got to here.
                ok = host.IsStarted;
            }
            else
            {
                // This is a secondary silo - start it in the Run phase
                ok = StartSilo();
            }

            if (ok)
            {
                host.WaitForOrleansSiloShutdown();
            }
            else
            {
                throw new ApplicationException("Silo failed to start correctly - aborting");
            }
        }

        /// <summary>
        /// Stop this Orleans silo executing.
        /// </summary>
        public void Stop()
        {
            logger.Info(ErrorCode.Runtime_Error_100290, "Stopping {0}", this.GetType().FullName);

            host.StopOrleansSilo();

            if (host.Config.Globals.LivenessType != GlobalConfiguration.LivenessProviderType.AzureTable)
            {
                siloInstanceManager.UnregisterSiloInstance(myEntry);
            }

            logger.Info(ErrorCode.Runtime_Error_100291, "Orleans silo '{0}' shutdown.", host.SiloName);
        }

        #endregion

        private bool StartSilo()
        {
            logger.Info(ErrorCode.Runtime_Error_100292, "Starting Orleans silo '{0}' as a {1} node.", host.SiloName, host.SiloType);

            bool ok = host.StartOrleansSilo();

            if (ok)
            {
                // if we are running with AzureTable based liveness, the silo will activate itself as part of membership protocol and write into Azure table.
                // but if we run with other types of liveness, the role itself needs to actiavte the silo in the Azure table.
                if (host.Config.Globals.LivenessType != GlobalConfiguration.LivenessProviderType.AzureTable)
                {
                    siloInstanceManager.ActivateSiloInstance(myEntry);
                }

                logger.Info(ErrorCode.Runtime_Error_100293, "Successfully started Orleans silo '{0}' as a {1} node.", host.SiloName, host.SiloType);
            }
            else
            {
                logger.Error(ErrorCode.Runtime_Error_100285, string.Format("Failed to start Orleans silo '{0}' as a {1} node.", host.SiloName, host.SiloType));
            }
            return ok;
        }

    }
}
