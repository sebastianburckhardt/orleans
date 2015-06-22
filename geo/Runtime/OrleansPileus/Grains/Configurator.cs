using GeoOrleans.Runtime.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Diagnostics;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Pileus;
using Microsoft.WindowsAzure.Storage.Pileus.Configuration;
using Microsoft.WindowsAzure.Storage.Pileus.Configuration.Actions;
using Microsoft.WindowsAzure.Storage.Pileus.Utils;
using Microsoft.WindowsAzure.Storage.Pileus.Configuration.Constraint;
using Orleans;
using System.Diagnostics;
using Orleans.Runtime;

namespace GeoOrleans.Runtime.OrleansPileus.Grains
{
    /// <summary>
    /// Grain implementation class Pileus Configurator Grain.
    /// </summary>
    public class PileusConfiguration : Grain, OrleansPileus.Interfaces.IConfigurator, Orleans.IRemindable
    {
        private Configurator configurator { get; set; }
        private Replicator replicator { get; set; }

        private string containerName = "";

        private Dictionary<string, CloudStorageAccount> storageAccounts;

        private Dictionary<string,CloudBlobContainer> containerList= new Dictionary<string,CloudBlobContainer>();

        private ConfigurationCloudStore backingStore;

        //TODO Parametrise
        private List<string> PrimaryServers = new List<string>() { "devstoreaccount1" };
        private List<string> SecondaryServers = new List<string>() { "devstoreaccount1" };
        private List<string> NonReplicaServers = new List<string>() { "devstoreaccount1" };
        private List<string> ReadOnlySecondaryServers = new List<string>() { "devstoreaccount1" };

        int MIN_PRIMARIES  = 2;
        int MAX_PRIMARIES = 2;

        private List<ConfigurationConstraint> defaultConfigConstraints = new List<ConfigurationConstraint>();

        private Dictionary<string,ClientUsageData> clientUsage = null;

        private ReplicaConfiguration replicaConfig = null;

        //TODO Parametrise
        string configStorageSite = "devstoreaccount1";

        /// <summary>
        /// Forces a reconfiguration, this is currently a 
        /// temporary interface. This should be called
        /// after a certain number of 
        /// TODO: figure out the right API
        /// </summary>
        public  async Task<bool> forceReconfigure(List<string> pFailedServers)
        {
            Trace.TraceInformation("Forcing Reconfiguration");

            int nbServers = replicaConfig.PrimaryServers.Count;
            int currentServers = nbServers - pFailedServers.Count;
            List<ConfigurationAction> actions = new List<ConfigurationAction>();

            // Remove the failing servers.
            foreach (string currentFailedServer in pFailedServers) {
                ConfigurationAction c = new RemovePrimaryServer(containerName, currentFailedServer, null);
                actions.Add(c);
            }

           // Add however many servers needed to get to MIN
            foreach (string nonReplicaServer in replicaConfig.NonReplicaServers)
            {
                if (currentServers-- == MIN_PRIMARIES) break;
                ConfigurationAction c = new AddPrimaryServer(containerName,nonReplicaServer,null);
                actions.Add(c);
            }

            // if still not enough, try to upgrade a secondary
            foreach (string nonPrimaryServer in replicaConfig.SecondaryServers)
            {
                if (currentServers-- == MIN_PRIMARIES) break;
                ConfigurationAction c = new AddPrimaryServer(containerName, nonPrimaryServer,null);
                actions.Add(c);
            }

            if (currentServers < MIN_PRIMARIES)
            {
                // Configuration failed
                return false;
            }


            // Actually install new configuration
            try
            {
                configurator.InstallNewConfiguration(actions    );
            }
            catch (StorageException e)
            {
                if (StorageExceptionCode.PreconditionFailed(e))
                {
                    // Etag failure, reconfiguration in progress, do nothing
                }
                else throw e;
            }

            return true;
        }

        /// <summary>
        /// Task whose only purpose is to start the configurator
        /// </summary>
        /// <returns></returns>
        public async Task startConfigurator()
        {
            Trace.TraceInformation("Starting configurator");
            await TaskDone.Done;
        }

        /// <summary>
        /// Returns list of containers 
        /// </summary>
        /// <returns></returns>
         public  Task<Dictionary<string,CloudBlobContainer>> getContainers()
        {
            Trace.TraceInformation("GetContainers");
             return Task.FromResult<Dictionary<string,CloudBlobContainer>>(containerList);
        }
        /// <summary>
        /// Initialises the current grain by creating a logical
        /// configurator and starting the replicator
        /// </summary>
        /// <returns></returns>
        public override Task OnActivateAsync()
        {
            try
            {
                Trace.TraceInformation("Starting Pileus Configurator");

                this.containerName = this.GetPrimaryKeyString();
                this.configurator = new Configurator(containerName);
                this.replicator = new Replicator(containerName);

                this.clientUsage= new Dictionary<string, ClientUsageData>();

                /* TODO, populate set of Servers according to config file */



                /* Initialise accounts. There is duplicate code here because can't
                 guarantee configurator will be on same Silo*/

                Trace.TraceInformation("Initialise storage accounts");
                storageAccounts = OrleansPileus.Common.Utils.GetStorageAccounts(false);

                ClientRegistry.Init(storageAccounts, storageAccounts[configStorageSite]);

                /* Initialise Replica Configuration. If a configuration exists it will get that
                 one, otherwise it will create a completely new config*/


                /*
                ReplicaConfiguration defaultConfig = new ReplicaConfiguration(containerName);
                backingStore = new ConfigurationCloudStore(storageAccounts[configStorageSite], defaultConfig, true);
                backingStore.DeleteConfiguration();
                this.replicaConfig = new ReplicaConfiguration(containerName, PrimaryServers, SecondaryServers, NonReplicaServers, ReadOnlySecondaryServers,true,true);
                ClientRegistry.AddConfiguration(replicaConfig);
                replicaConfig.isStable = false;
                backingStore = replicaConfig.backingStore; */

                
                // Creates a default config in case it does not exist
                ReplicaConfiguration defaultConfig = new ReplicaConfiguration(containerName, PrimaryServers, SecondaryServers, NonReplicaServers, ReadOnlySecondaryServers, true, true);
                backingStore = new ConfigurationCloudStore(storageAccounts[configStorageSite], defaultConfig, false);

                replicaConfig = backingStore.getCachedConfiguration();
                ClientRegistry.AddConfiguration(replicaConfig);
                replicaConfig.isStable = false;

                // Initialises Configuration Constraint
                ConfigurationConstraint c = new PrimaryReplicationFactorConstraint(containerName, replicaConfig, MIN_PRIMARIES, MAX_PRIMARIES);
                defaultConfigConstraints.Add(c);


                RegisterOrUpdateReminder("syncconfig", TimeSpan.FromMilliseconds(10000), TimeSpan.FromMilliseconds(10000));

                Trace.TraceInformation("Creating replicated container for each storage account\n");

                /* Initialise all containers */
                foreach (string site in storageAccounts.Keys)
                {
                    CloudBlobClient blobClient = storageAccounts[site].CreateCloudBlobClient();
                    IEnumerable<CloudBlobContainer> containers = blobClient.ListContainers();
                    IEnumerable<IListBlobItem> blobs = blobClient.ListBlobs("");

                    CloudBlobContainer blobContainer = blobClient.GetContainerReference(containerName);
                    bool created = blobContainer.CreateIfNotExists();
                    Trace.TraceInformation("Incorrect " + created);
                    containerList.Add(site, blobContainer);
                }
                
                Trace.TraceInformation("Grain activation successful");

                return base.OnActivateAsync();
            } catch (Exception e) { 
                Trace.TraceError(e.ToString());
            }
            return TaskDone.Done;
        }

        /// <summary>
        /// Reads Client Usage Data
        /// </summary>
        /// <returns></returns>
        private async Task readUsageData()
        {
            Trace.TraceInformation("Read Usage Data {0} ", containerName);
            clientUsage = await backingStore.readUsageData();
        }

        /// <summary>
        /// Reads Client Usage Data
        /// </summary>
        /// <returns></returns>
        private async Task writeUsageData(ClientUsageData pNewData)
        {
            Trace.TraceInformation("Write Usage Data {0} ", containerName);
            await backingStore.writeUsageData(pNewData);
            throw new NotImplementedException();
        }

        /// <summary>
        /// Sequenced Grain associated with this instance of the configurator
        /// periodically sends it its updated value
        /// </summary>
        /// <param name="clientData"></param>
        /// <returns></returns>
        public async Task receiveUsageData(ClientUsageData pClientData)
        {
            Trace.TraceInformation("Receive Usage Data {0}", pClientData.ClientName);
            await writeUsageData(pClientData);

        }

        /// <summary>
        /// On each reminder, refreshes configuration. This happens even if configurator is 
        /// not active. This is to account for silo failures.
        /// 
        /// </summary>
        /// <param name="pReminderName"></param>
        /// <param name="status"></param>
        /// <returns></returns>
        Task IRemindable.ReceiveReminder(string pReminderName, TickStatus status)
        {
            Trace.TraceInformation("Received reminder, refreshing config");
            // Updates its view of usage data
            readUsageData().Wait();

            // Select Configuration constraints TODO: parametrise
            List<ConfigurationConstraint> configConstraints = new List<ConfigurationConstraint>();

            configConstraints.Union(defaultConfigConstraints);

            // Pick new actions
            List<ConfigurationAction> configActions =
                       configurator.ChooseReconfigActions(clientUsage, configConstraints,replicaConfig);

            // Actually install new configuration
            try
            {
                configurator.InstallNewConfiguration(configActions);
            }
            catch (StorageException e)
            {
                if (StorageExceptionCode.PreconditionFailed(e))
                {
                    // Etag failure, reconfiguration in progress, do nothing
                }
                else throw e ;
            }
            return TaskDone.Done;

        } 
        


    }
}
