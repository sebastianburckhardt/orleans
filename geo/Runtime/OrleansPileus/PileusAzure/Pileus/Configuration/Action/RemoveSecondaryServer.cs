using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Threading;
using System.IO;
using Microsoft.WindowsAzure.Storage.Pileus.Utils;

namespace Microsoft.WindowsAzure.Storage.Pileus.Configuration.Actions
{
    /// <summary>
    /// Removes the secondary server from the container.
    /// </summary>
    internal class RemoveSecondaryServer : ConfigurationAction
    {

        public RemoveSecondaryServer(string containerName, string serverName, float? utility, string slaId = null, ConfigurationActionSource source = ConfigurationActionSource.Constraint, int numberOfReads = 0, int numberOfWrites = 0)
            : base(containerName, serverName, utility, slaId, source,numberOfReads,numberOfWrites)
        {

        }

        public override void Execute()
        {
            Configuration.EndCurrentEpoch();

            //We then update the configuration service
            Configuration.SecondaryServers.Remove(ServerName);

            AppendToLogger("Starting the new Epoch");
            Configuration.StartNewEpoch();

            //we first delete the container.
            ModifyingContainer.DeleteIfExists();
            
        }

        public override ReplicaConfiguration IfApply(ReplicaConfiguration current)
        {
            List<string> primary = new List<string>();
            List<string> secondary = new List<string>();

            current.SecondaryServers.ForEach(s => { if (s != ServerName) secondary.Add(s); });
            current.PrimaryServers.ForEach(s => { primary.Add(s); });

            return new ReplicaConfiguration(current.Name, primary, secondary);
        }

        public override int NumberOfAddingReplica()
        {
            return -1;
        }

        public override double ComputeCost()
        {
            double result = 0;
            result = CostModel.GetSecondaryTransactionalCost(numberOfReads) + CostModel.GetSyncCost(numberOfWrites, ConstPool.DEFAULT_SYNC_INTERVAL) + CostModel.GetStorageCost(ClientRegistry.GetMainPrimaryContainer(Configuration.Name));
            return result;
        }
    }
}
