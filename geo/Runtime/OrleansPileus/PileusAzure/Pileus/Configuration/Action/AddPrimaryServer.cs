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
    /// Makes the given server, primary.
    /// Hence, upon execution of this action, there will be at least 2 primary servers in the system.
    /// </summary>
    internal class AddPrimaryServer : ConfigurationAction
    {


        public AddPrimaryServer(string containerName, string serverName, float? utility, string slaId = null, ConfigurationActionSource source = ConfigurationActionSource.Constraint, int numberOfReads = 0, int numberOfWrites = 0)
            : base(containerName, serverName, utility, slaId, source, numberOfReads, numberOfWrites)
        {

        }

        public override void Execute()
        {
            CloudBlobContainer primaryContainer = ClientRegistry.GetCloudBlobContainer(Configuration.PrimaryServers.First(), ModifyingContainer.Name);
            SynchronizeContainer synchronizer = new SynchronizeContainer(primaryContainer, ModifyingContainer);
            synchronizer.BeginSyncContainers();

            Configuration.EndCurrentEpoch();

            Configuration.PrimaryServers.Add(ServerName);

            //we add the new primary also to the list of write_only primaries. 
            //Servers in this list are not registered in session state, hence a get operation cannot be issued for them.
            //This makes put operations to go to all primaries (including the new one), but get_primary only goes to old primaries (those that does not exist in not register list).
            Configuration.WriteOnlyPrimaryServers.Add(ServerName);

            Configuration.StartNewEpoch();

            //We wait until sync is finihsed
            synchronizer.EndSyncContainers();

            // TODO: check that the new primary server did indeed get all of the writes.
            
            Configuration.EndCurrentEpoch();

            //We clear the not register primary list, so the new primary can also be registered and used for get_primary operations.
            Configuration.WriteOnlyPrimaryServers.Clear();

            //Finally, we update the lookup service by writing the new configuration to the cloud, so every other client can read it.
            Configuration.StartNewEpoch();
        }

        public override ReplicaConfiguration IfApply(ReplicaConfiguration current)
        {
            List<string> primary = new List<string>();
            List<string> secondary = new List<string>();

            current.SecondaryServers.ForEach(s => { if (s != ServerName) secondary.Add(s); else primary.Add(s); });
            current.PrimaryServers.ForEach(s => { primary.Add(s); });

            current.NonReplicaServers.ForEach(s => { if (s == ServerName) primary.Add(s); });

            return new ReplicaConfiguration(current.Name, primary, secondary);
        }

        public override int NumberOfAddingReplica()
        {
            if (Configuration.SecondaryServers.Contains(ServerName))
            {
                //secondary replica will become primary. Hence, no new replica will be added.
                return 0;
            }
            else
            {
                return 1;
            }
        }

        public override double ComputeCost()
        {
            double result = 0;
            if (NumberOfAddingReplica() == 0)
            {
                int secondarySyncPeriod = Configuration.GetSyncPeriod(ServerName);
                result = CostModel.GetPrimaryTransactionalCost(numberOfReads, numberOfWrites) - (CostModel.GetSecondaryTransactionalCost(numberOfReads) + CostModel.GetSyncCost(numberOfWrites, secondarySyncPeriod));
            }
            else
            {
                result = CostModel.GetPrimaryTransactionalCost(numberOfReads, numberOfWrites) + CostModel.GetStorageCost(ClientRegistry.GetMainPrimaryContainer(Configuration.Name));
            }

            return result;
        }
    }
}
