using Microsoft.WindowsAzure.Storage.Pileus.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.Storage.Pileus.Configuration
{
    /*
     * IMPORTANT NOTE: Although this class allows clients to modify the set of primary and secondary replicas, 
     * doing so is generally a bad idea.  For example, simply adding a replica to the list of primaries does
     * not cause data to be replicated at that site and does not ensure that this replica is up-to-date.
     * So, modifying the configuration through this interface could cause errors or consistency violations.
     * The client's view of the configuration must remain consistent with the actual replication state.
     * The safe way to change a configuration is through the Configurator class.
     */
    
    /// <summary>
    /// Maintains a list of primary and secondary replicas, as well as non-replica servers
    /// and the frequency with which replicas synchronize.
    /// Each "server" is really an Azure storage site using three-way replication on unknown physical machines.
    /// Note that this class is independent of the type of data being replicated.
    /// </summary>
    [Serializable()]
    public class ReplicaConfiguration
    {
        #region Properties and locals

        // Name of the replicated collection with this configuration (AKA tablet's name in the context of [SOSP'13])
        public string Name { get; private set; }

        // List of primary severs
        public List<string> PrimaryServers { get; set; }

        // List of write-only primary servers (a subset of PrimaryServers)
        // These servers are newly added servers and must be used only for update operations, 
        // but cannot be used for strong read operations because they are not yet fully up-to-date. 
        public List<string> WriteOnlyPrimaryServers { get; set; }

        // List of secondary servers
        public List<string> SecondaryServers { get; set; }

        // List of read-only secondary servers (a subset of SecondaryServers)
        // For example, this list might contain geo-replicated storage servers in Azure
        public List<string> ReadOnlySecondaryServers { get; set; }

        // List of non-replicating servers, i.e. those that could possibly hold replicas but currently do not
        public List<string> NonReplicaServers { get; set; }

        // Maps secondary server name to corresponding frequency of synchronization.
        private Dictionary<string, int> syncPeriod;

        // Configuration's epoch number.
        public int Epoch { get; set; }

        // The time when the epoch was modified. 
        public DateTimeOffset EpochModifiedTime { get; set; }

        // If true, then the configuration is not going to change.
        // If false, then the configuration could be modified by other clients; hence, new configurations must be downloaded
        // from the cloud, and the client must be prepared to deal with dynamic reconfigurations.
        // Note: if the configuration is stable and not cloud backed, then the client must ensure that its local
        // configuration is correct, i.e. that it matches the actual configuration of replicas.
        [NonSerialized()]
        private bool isStable = true;

        // If set, then a copy of this configuration resides in the cloud where it can be shared with other clients.
        [NonSerialized()]
        private ConfigurationCloudStore backingStore = null;

        #endregion

        /// <summary>
        /// Creates a new empty configuration.
        /// </summary>
        /// <param name="name">name of the replicated collection</param>
        public ReplicaConfiguration (string name)
        {
            Name = name;
            Epoch = 0;
            EpochModifiedTime = DateTimeOffset.Now;
            
            PrimaryServers = new List<string>();
            SecondaryServers = new List<string>();
            WriteOnlyPrimaryServers = new List<string>();
            ReadOnlySecondaryServers = new List<string>();
            NonReplicaServers = new List<string>();
            syncPeriod = new Dictionary<string, int>();
            isStable = true;
        }

        /// <summary>
        /// Creates a configuration.
        /// </summary>
        /// <param name="name">name of the replicated collection</param>
        public ReplicaConfiguration(string name, List<string> primaries, List<string> secondaries, List<string> nonreplicas = null, List<string> readonlyReplicas = null, bool isCloudBacked = true, bool isStable = false)
            : this(name)
        {
            // set local configuration
            if (primaries != null)
            {
                PrimaryServers = primaries.ToList();
            }
            if (secondaries != null)
            {
                SecondaryServers = secondaries.ToList();
            }
            if (nonreplicas != null)
            {
                NonReplicaServers = nonreplicas.ToList();
            }
            if (readonlyReplicas != null)
            {
                ReadOnlySecondaryServers = readonlyReplicas.ToList();
            }
            this.isStable = isStable;
            if (isCloudBacked)
            {
                // get configuration from cloud (or write configuration to cloud if it does not exist)
                bool autoRefresh = (!isStable);
                SyncWithCloud(ClientRegistry.GetConfigurationAccount(), autoRefresh);
            }
        }

        public bool IsInFastMode(bool renew = false)
        {
            bool result = isStable;
            if (!isStable)
            {
                if (backingStore != null)
                {
                    result = backingStore.HasLease();
                    if (!result && renew)
                    {
                        result = backingStore.RenewLease() > 0;
                    }
                }
            }
            return result;
        }

        public int GetSyncPeriod(string server)
        {
            int period = ConstPool.DEFAULT_SYNC_INTERVAL;
            if (syncPeriod.ContainsKey(server))
            {
                period = syncPeriod[server];
            }
            return period;
        }

        public void SetSyncPeriod(string server, int period)
        {
            syncPeriod[server] = period;
        }

        public List<string> GetServers(bool primary = true, bool secondary = true, bool nonreplica = true)
        {
            List<string> servers = new List<string>();
            if (primary)
            {
                foreach (string server in PrimaryServers)
                {
                    servers.Add(server);
                }
            }
            if (secondary)
            {
                foreach (string server in SecondaryServers)
                {
                    servers.Add(server);
                }
            }
            if (nonreplica)
            {
                foreach (string server in NonReplicaServers)
                {
                    servers.Add(server);
                }
            }
            return servers;
        }

        public override string ToString()
        {
            string result;
            result = "Container: " + this.Name;
            result += " Primaries: ";
            PrimaryServers.ForEach(e => result += e + ",");
            result += " Secondaries: ";
            SecondaryServers.ForEach(e => result += e + ",");
            return result;
        }

        #region Cloud Store

        /// <summary>
        /// Gets the configuration from the cloud. 
        /// If the configuration exists in the cloud and has a larger epoch, then it overwrites any local configuration.
        /// If it does not exist, then the local configuration is uploaded to the cloud.
        /// </summary>
        /// <param name="autoRefresh">Whether to periodically check the cloud for updated configurations</param>
        public void SyncWithCloud (CloudStorageAccount account, bool autoRefresh = true)
        {
            if (backingStore == null)
            {
                backingStore = new ConfigurationCloudStore(account, this);
            }
            bool exists = backingStore.ReadConfiguration(autoRefresh);
            if (!exists)
            {
                backingStore.CreateConfiguration(autoRefresh);
            }
        }

        /// <summary>
        /// Ends the current epoch.  
        /// If the configuration is not being shared with other clients through the cloud, then this does nothing.
        /// </summary>
        public void EndCurrentEpoch()
        {
            if (!isStable && backingStore != null)
            {
                backingStore.EndCurrentEpoch();
            }
        }

        /// <summary>
        /// Starts a new configuration epoch and uploads the configuration to the cloud.
        /// This should be called whenever the configuration changes.
        /// </summary>
        public void StartNewEpoch()
        {
            if (!isStable && backingStore != null)
            {
                backingStore.StartNewEpoch();
            }
            else
            {
                ++Epoch;
            }
        }

        #endregion

    }
}
