using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;
using System.IO;
using Microsoft.WindowsAzure.Storage.Pileus.Utils;

namespace Microsoft.WindowsAzure.Storage.Pileus.Configuration.Actions
{
    /// <summary>
    /// Adds a new secondary replica, and increment the epoch number.
    /// This operation is non-blocking. 
    /// </summary>
    internal class AddSecondaryServer : ConfigurationAction
    {

        public AddSecondaryServer(string containerName, string serverName, float? utility, string slaId = null, ConfigurationActionSource source = ConfigurationActionSource.Constraint, int numberOfReads = 0, int numberOfWrites = 0)
            : base(containerName, serverName, utility, slaId,source,numberOfReads,numberOfWrites)
        {

        }

        public override void Execute()
        {

            AppendToLogger("Start Synchronization");
            CloudBlobContainer primaryContainer = ClientRegistry.GetCloudBlobContainer(Configuration.PrimaryServers.First(), ModifyingContainer.Name);
            SynchronizeContainer synchronizer = new SynchronizeContainer(primaryContainer, ModifyingContainer);
            synchronizer.SyncContainers();
            AppendToLogger("Synchronization finished.");
            //Update the configuration
            Configuration.SecondaryServers.Add(ServerName);

            AppendToLogger("Starting the new Epoch");
            Configuration.StartNewEpoch();
        }

        public override ReplicaConfiguration IfApply(ReplicaConfiguration current)
        {
            List<string> primary = new List<string>();
            List<string> secondary = new List<string>();

            current.SecondaryServers.ForEach(s => { secondary.Add(s); });
            current.PrimaryServers.ForEach(s => { primary.Add(s); });

            current.NonReplicaServers.ForEach(s => { if (s == ServerName) secondary.Add(s); });

            return new ReplicaConfiguration(current.Name, primary, secondary);
        }

        public override int NumberOfAddingReplica()
        {
            return 1;
        }

        public override double ComputeCost()
        {
            double result = 0;
            double transactionalCost = CostModel.GetSecondaryTransactionalCost(numberOfReads);

            double syncCost = CostModel.GetSyncCost(numberOfWrites, ConstPool.DEFAULT_SYNC_INTERVAL);
            double StorageCost=CostModel.GetStorageCost(ClientRegistry.GetMainPrimaryContainer(Configuration.Name));
            result = transactionalCost + syncCost + StorageCost ;
            return result;
        }

    }
}
