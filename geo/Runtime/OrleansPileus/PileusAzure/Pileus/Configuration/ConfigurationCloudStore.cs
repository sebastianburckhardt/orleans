using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Pileus.Utils;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.Storage.Pileus.Configuration
{
    /// <summary>
    /// Stores replication configurations in the cloud (i.e. in Azure Storage).
    /// </summary>
    public class ConfigurationCloudStore
    {
        // Local cache of cloud-resident configuration
        private ReplicaConfiguration config;
        
        // Task for periodically refreshing local configuration; only one need be running
        private Task refreshConfigurationTask = null;

        // Client used for reading/writing the configuration from the Azure storage.
        private CloudBlobClient configurationCloudBlobClient;

        // Container holding the configuration blob
        private CloudBlobContainer configurationContainer; 

        // Blob storing the current configuration
        private ICloudBlob configurationBlob;

        /// <summary>
        /// Constructor for ConfigurationCloudStore
        /// </summary>
        /// <param name="account">the Azure account where configuration data is stored</param>
        public ConfigurationCloudStore(CloudStorageAccount account, ReplicaConfiguration configuration)
        {
            config = configuration;
            try
            {
                configurationCloudBlobClient = account.CreateCloudBlobClient();
                configurationContainer = configurationCloudBlobClient.GetContainerReference(ConstPool.CONFIGURATION_CONTAINER_PREFIX + config.Name);
                configurationContainer.CreateIfNotExists();
                configurationBlob = configurationContainer.GetBlockBlobReference(ConstPool.CURRENT_CONFIGURATION_BLOB_NAME);
                if (!configurationBlob.Exists())
                {
                    CreateConfiguration(false);
                }
            }
            catch (StorageException ex)
            {
                Console.WriteLine(ex.ToString());
                throw ex;
            }
        }

        #region Public Methods

        /// <summary>
        /// Create in Azure a configuration blob to store the local configuration.
        /// </summary>
        /// <param name="autoRefresh">Whether to periodically check for changes in the cloud</param>
        public void CreateConfiguration(bool autoRefresh = true)
        {
            InitializeConfigurationBlob();
            StartNewEpoch();

            if (autoRefresh && refreshConfigurationTask == null)
            {
                refreshConfigurationTask = Task.Factory.StartNew(() => RefreshConfigurationPeriodically());
            }
        }

        /// <summary>
        /// Save the local configuration in the Azure's configuration blob.
        /// </summary>
        /// <param name="autoRefresh">Whether to periodically check for changes in the cloud</param>
        public void UpdateConfiguration(bool autoRefresh = true)
        {
            EndCurrentEpoch();  
            StartNewEpoch();

            if (autoRefresh && refreshConfigurationTask == null)
            {
                refreshConfigurationTask = Task.Factory.StartNew(() => RefreshConfigurationPeriodically());
            }
        }

        /// <summary>
        /// Delete the configuration container from the azure storage.
        /// WARNING: Only call this if no other clients are using the configuration.
        /// </summary>
        public void DeleteConfiguration()
        {
            configurationContainer.DeleteIfExists();
        }

        /// <summary>
        /// Read an already created configuration blob from Azure and store in the local configuration.
        /// </summary>
        /// <param name="capCloudBlobContainerName">Name of the CapCloudBlobContainer</param>
        /// <param name="sessionState">session state of the SLAEngine. This state is used for registering, and unregistering newly added/removed replicas</param>
        /// <param name="autoRefresh">if true, it will automatically refresh the configuration by reading it periodically from the cloud.</param>
        /// <returns></returns>
        public bool ReadConfiguration(bool autoRefresh = true)
        {
            bool result = DownloadConfiguration();
            if (autoRefresh && refreshConfigurationTask == null)
            {
                refreshConfigurationTask = Task.Factory.StartNew(() => RefreshConfigurationPeriodically());
            }
            return result;
        }

        #endregion

        #region private Methods

        /// <summary>
        /// Initializes a blob by writing an empty stream to the azure.
        /// </summary>
        private void InitializeConfigurationBlob()
        {
            try
            {
                using (MemoryStream stream = new MemoryStream())
                {
                    BinaryFormatter formatter = new BinaryFormatter();
                    formatter.Serialize(stream, "");
                    stream.Position = 0;
                    configurationBlob.UploadFromStream(stream);
                }
            }
            catch (StorageException ex)
            {
                Console.WriteLine(ex.ToString());
                throw ex;
            }
        }

        /// <summary>
        /// Periodically refresh the configuration.
        /// </summary>
        private void RefreshConfigurationPeriodically()
        {
            Stopwatch w = new Stopwatch();
            int timeToSleep;

            while (true)
            {
                // start a clock to determine the time spent refreshing the configuration
                w.Restart();

                // renew the lease if possible
                int currentEpoch = RenewLease();
                if (currentEpoch > 0)
                {
                    // renewal succeeded
                    Console.WriteLine("Lease refresh succeeded.");
                    timeToSleep = ConstPool.CACHED_CONFIGURATION_VALIDITY_DURATION;
                }
                else
                {
                    // cached configuration is changing, we need to wait for the meantime. 
                    timeToSleep = ConstPool.CONFIGURATION_ACTION_DURATION;
                    Console.WriteLine("Lease refresh waiting for reconfiguration in progress...");

                    // check that the reconfiguration is not taking too long, 
                    // i.e. that the configurator did not crash in the middle
                    TimeSpan waiting = DateTime.Now - expirationTime;
                    if (waiting.TotalMilliseconds > ConstPool.CONFIGURATION_ACTION_TIMEOUT)
                    {
                        // start a new epoch number but do not upload a new configuration
                        // this allows all clients to reacquire leases
                        StartNewEpoch(false);
                        Console.WriteLine("Lease refresh detected failed reconfiguration after " + waiting.TotalMilliseconds + "ms and is starting new epoch...");
                        timeToSleep = ConstPool.CACHED_CONFIGURATION_VALIDITY_DURATION;
                    }
                }

                // sleep until just before the new lease expires
                // we double the time taken to renew the lease just to have some room for variance in the execution time
                w.Stop();
                timeToSleep = timeToSleep - 2 * Convert.ToInt32(w.ElapsedMilliseconds);
                if (timeToSleep > 0)
                {
                    Thread.Sleep(timeToSleep);
                }
            }
        }

        #endregion

        #region Internal methods to download/upload configurations

        /// <summary>
        /// Read the configuration blob from Azure storage and update the local configuration.
        /// </summary>
        internal bool DownloadConfiguration()
        {
            try
            {
                using (MemoryStream stream = new MemoryStream())
                {
                    configurationBlob.DownloadToStream(stream);
                    stream.Seek(0, 0);
                    BinaryFormatter formatter = new BinaryFormatter();
                    ReplicaConfiguration cloudConfig = (ReplicaConfiguration)formatter.Deserialize(stream);

                    if (!configurationBlob.Metadata.ContainsKey(ConstPool.EPOCH_NUMBER))
                    {
                        Console.WriteLine("No Epoch in configuration metadata!");  // this should not happen
                        return false;  // stay with current configuration for now
                    }

                    int currentEpoch = Convert.ToInt32(configurationBlob.Metadata[ConstPool.EPOCH_NUMBER]);
                    if (currentEpoch > config.Epoch)
                    {
                        config.PrimaryServers = cloudConfig.PrimaryServers;
                        config.PrimaryServers.Sort();
                        config.SecondaryServers = cloudConfig.SecondaryServers ?? new List<string>();
                        config.WriteOnlyPrimaryServers = cloudConfig.WriteOnlyPrimaryServers ?? new List<string>();
                        foreach (string server in config.SecondaryServers)
                        {
                            config.SetSyncPeriod(server, cloudConfig.GetSyncPeriod(server));
                        }
                        
                        config.Epoch = currentEpoch;
                        if (configurationBlob.Metadata[ConstPool.EPOCH_MODIFIED_TIME] != null)
                            config.EpochModifiedTime = DateTimeOffset.Parse(configurationBlob.Metadata[ConstPool.EPOCH_MODIFIED_TIME]);
                        else
                            config.EpochModifiedTime = DateTimeOffset.MinValue;
                    }
                }
            }
            catch (StorageException se)
            {
                Console.WriteLine(se.ToString());
                Console.WriteLine(se.StackTrace);
                throw se;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                Console.WriteLine(ex.StackTrace);
                throw ex;
            }
            return true;
        }

        /*
         * Writing a new configuration involves four steps:
         *   1. Write RECONFIGURATION_IN_PROGRESS as the epoch number (as discussed below under leases).
         *   2. Wait for all existing client leases to expire.
         *   3. Write the new configuration to the special configuration blob using the ETag received in step 1.
         *   4. Write the new epoch number to the blob's metadata using the ETag received in step 3.
         * 
         * Steps 1 and 2 are invoked by called EndCurrentEpoch().
         * Steps 3 and 4 are invoked by calling StartNewEpoch().
         * 
         * Note that no actual Azure leases are used.  Instead, optimistic concurrency control uses ETags.
         * The writes in steps 1, 3, and 4 are linked together using ETags.  All three succeed if no other
         * process is concurrently writing a new configuration.  If step 3 or 4 fail, then that  
         * reconfiguration is aborted.  Among a set of concurrent reconfigurations, the last one to perform
         * step 1 will win and the others will all give up.  Note that an aborted reconfiguration may have
         * written a new configuration in step 3 and then failed in step 4.  That's okay since the 
         * surviving concurrent reconfiguration will overwrite the configuration blob. 
         * 
         * If a reconfiguration process crashes after performing step 1 but before completing step 4,
         * then the system will be left in a state where clients see a reconfiguration in progress that 
         * never terminates.  The next reconfiguration will get the system out of this state, but may
         * not be scheduled for awhile.  A simple recovery is for any client to write a new epoch number.
         * That is, any client can perform step 4.  This aborts or completes any reconfigurations that  
         * are in progress and allows clients to once again obtain leases on the current configuration.
         */

        private string reconfigurationETag = null;

        /// <summary>
        /// Writes a negative epoch number in the configuration blob metadata to indicate that a configuration is in progress.
        /// Then waits until all clients have failed to renew their leases.
        /// </summary>
        internal void EndCurrentEpoch()
        {
            try
            {
                configurationBlob.Metadata[ConstPool.EPOCH_NUMBER] = ConstPool.RECONFIGURATION_IN_PROGRESS;
                configurationBlob.SetMetadata();
                reconfigurationETag = configurationBlob.Properties.ETag;
                Console.WriteLine("Wrote configuration in progress, now waiting for client leases to expire...");
                Thread.Sleep(ConstPool.CACHED_CONFIGURATION_VALIDITY_DURATION);
                Console.WriteLine("Proceeding with reconfiguration...");
            }
            catch (StorageException ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Starts a new configuration epoch and uploads the local configuration.
        /// </summary>
        /// <param name="updateConfiguration">If true, the configuration is also uploaded to the azure storage.</param>
        /// <returns></returns>
        internal void StartNewEpoch(bool uploadConfiguration = true)
        {
            if (uploadConfiguration)
            {
                Console.WriteLine("Uploading new configuration to the cloud...");
                UploadConfiguration();
                reconfigurationETag = configurationBlob.Properties.ETag;
            }

            try
            {
                int newEpoch = ++config.Epoch;
                configurationBlob.Metadata[ConstPool.EPOCH_NUMBER] = "" + newEpoch;
                configurationBlob.Metadata[ConstPool.EPOCH_MODIFIED_TIME] = DateTimeOffset.Now.ToString();
                AccessCondition access = (reconfigurationETag != null) ? AccessCondition.GenerateIfMatchCondition(reconfigurationETag) : null;
                configurationBlob.SetMetadata(access);
                reconfigurationETag = null;

                Console.WriteLine("Epoch " + config.Epoch + " written to the azure and started.");
            }
            catch (StorageException ex)
            {
                reconfigurationETag = null;
                if (StorageExceptionCode.PreconditionFailed(ex))
                {
                    // ETag condition was not met; there must be a concurrent reconfiguration and we lost
                    // Although our proposed configuration was already uploaded, we need to do nothing since 
                    // it will be overwritten by the winning configurator.
                    Console.WriteLine("Reconfiguration aborted due to concurrent activity");
                }
                else
                {
                    throw ex;
                }
            }
        }

        /// <summary>
        /// Uploads the configuration to the cloud
        /// </summary>
        /// <returns></returns>
        internal void UploadConfiguration()
        {
            try
            {
                using (MemoryStream stream = new MemoryStream())
                {
                    BinaryFormatter formatter = new BinaryFormatter();
                    formatter.Serialize(stream, this.config);
                    stream.Position = 0;
                    AccessCondition access = (reconfigurationETag != null) ? AccessCondition.GenerateIfMatchCondition(reconfigurationETag) : null;
                    configurationBlob.UploadFromStream(stream, access);
                }
            }
            catch (StorageException ex)
            {
                reconfigurationETag = null;
                if (StorageExceptionCode.PreconditionFailed(ex))
                {
                    // ETag condition was not met; there must be a concurrent reconfiguration and we lost
                    // so do nothing
                    Console.WriteLine("Reconfiguration aborted due to concurrent activity");
                }
                else
                {
                    throw ex;
                }
            }
        }

        #endregion

        #region Leases

        /*
         * Clients obtain "leases" on the current configuration as follows.  
         * A client periodically reads the configuration blob.  If the blob's epoch is positive, then the client knows
         * that that configuration will not change for at least CACHED_CONFIGURATION_VALIDITY_DURATION milliseconds.
         * When the configurator wants to change the current configuration, it writes RECONFIGURATION_IN_PROGRESS 
         * as the blob's epoch, and then it waits for CACHED_CONFIGURATION_VALIDITY_DURATION milliseconds.  
         * This ensures that all clients will discover that a reconfiguration is in progress.  
         * Clients receiving the RECONFIGURATION_IN_PROGRESS epoch, know that their configuration may be incorrect.
         * The configurator, after waiting, can write the new configuration to the cloud blob.
         * Clients will then read the new configuration and once again hold a lease on it.
         * Note that this process does not rely on any actual Azure leases.
         */

        private DateTime expirationTime = DateTime.Now;

        /// <summary>
        /// Renews a non-exclusive lease on the configuration.
        /// If this returns a positive epoch number, then the lease as been acquired.
        /// If a negative number is returned, then the lease was not renewed because either
        /// there is a reconfiguration in progress or a configuration blob was not found.
        /// </summary>
        /// <returns>The epoch of the current leased configuration.</returns>
        public int RenewLease()
        {
            DateTime startRPC = DateTime.Now;
            try
            {
                configurationBlob.FetchAttributes();
            }
            catch (StorageException ex)
            {
                //304 is azure's not modified exception.
                if (StorageExceptionCode.NotModified(ex))
                {
                    // the configuration is not modified since last renewal
                }

                //404 is Azure's not-found exception
                if (StorageExceptionCode.NotFound(ex))
                {
                    return -1;
                }
                throw ex;
            }

            int currentEpoch = Convert.ToInt32(configurationBlob.Metadata[ConstPool.EPOCH_NUMBER]);
            if (currentEpoch > 0)
            {
                // no reconfiguration in progress so renewal was successful
                expirationTime = startRPC.AddMilliseconds(ConstPool.CACHED_CONFIGURATION_VALIDITY_DURATION);
                if (currentEpoch > config.Epoch)
                {
                    // configuration has changed
                    DownloadConfiguration();
                    Console.WriteLine("New configuration for Epoch " + config.Epoch + " is primaries: " + String.Join(", ", config.PrimaryServers.ToList()) + " secondaries:" + String.Join(", ", config.SecondaryServers.ToList()));
                }
            }
            return currentEpoch;
        }

        public bool HasLease()
        {
            return (DateTime.Now < expirationTime);
        }

        public int TimeToLeaseExpiration()
        {
            TimeSpan remaining = expirationTime - DateTime.Now;
            return (int) remaining.TotalMilliseconds;
        }

        #endregion

    }
}
