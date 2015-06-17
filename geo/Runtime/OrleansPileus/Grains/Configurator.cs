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

namespace GeoOrleans.Runtime.OrleansPileus.Grains
{
    /// <summary>
    /// Grain implementation class Grain1.
    /// </summary>
    public class PileusConfiguration : Grain, OrleansPileus.Interfaces.IConfigurator
    {
        private Configurator configurator { get; set; }
        private Replicator replicator { get; set; }

        private string containerName = "";

        private Dictionary<string, CloudStorageAccount> storageAccounts;

        private Dictionary<string,CloudBlobContainer> containerList= new Dictionary<string,CloudBlobContainer>();


        private List<string> PrimaryServers = new List<string>() { "devstoreaccount1" };
        private List<string> SecondaryServers = new List<string>() { "devstoreaccount1" };
        private List<string> NonReplicaServers = new List<string>() { "devstoreaccount1" };
        private List<string> ReadOnlySecondaryServers = new List<string>() { "devstoreaccount1" };

        private ReplicaConfiguration replicaConfig = null;

        string configStorageSite = "devstoreaccount1";

        /// <summary>
        /// Forces a reconfiguration, this is currently a 
        /// temporary interface. This should be called
        /// after a certain number of 
        /// TODO: figure out the right API
        /// </summary>
        public  Task forceReconfigure()
        {
            throw new NotImplementedException();
        }

        public async Task startConfigurator()
        {
            Trace.TraceInformation("Starting configurator");
            await TaskDone.Done;
        }

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
         //       replicator.Start();


                /* TODO, populate set of Servers according to config file */


                /* Initialise accounts*/

                Trace.TraceInformation("Initialise storage accounts");
                storageAccounts = OrleansPileus.Common.Utils.GetStorageAccounts(false);

                ClientRegistry.Init(storageAccounts, storageAccounts[configStorageSite]);

                /* Initialise Replica Configuration */
                ReplicaConfiguration configToDelete = new ReplicaConfiguration(containerName);
                ConfigurationCloudStore backingStore = new ConfigurationCloudStore(storageAccounts[configStorageSite], configToDelete);
                backingStore.DeleteConfiguration();
                this.replicaConfig = new ReplicaConfiguration(containerName, PrimaryServers, SecondaryServers, NonReplicaServers, ReadOnlySecondaryServers, false, true);
                ClientRegistry.AddConfiguration(replicaConfig);

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


    }
}
