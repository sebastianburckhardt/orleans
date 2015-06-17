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
    /// Makes the given server the only primary server, hence all other primaries become secondary servers.
    /// </summary>
    internal class MakeSoloPrimaryServer: ConfigurationAction
    {
        public MakeSoloPrimaryServer(string containerName, string serverName, float? utility, string slaId = null, ConfigurationActionSource source = ConfigurationActionSource.Constraint, int numberOfReads = 0, int numberOfWrites = 0)
            : base(containerName, serverName, utility, slaId, source,numberOfReads,numberOfWrites)
        {

        }

        public override void Execute()
        {
            if (!Configuration.PrimaryServers.Contains(ServerName))
            {
                AppendToLogger("Start Asynchronous Synchronization.");
                CloudBlobContainer primaryContainer = ClientRegistry.GetCloudBlobContainer(Configuration.PrimaryServers.First(), ModifyingContainer.Name);
                SynchronizeContainer synchronizer = new SynchronizeContainer(primaryContainer, ModifyingContainer);
                // let's try avoiding asynchronous sync to see if that makes things run faster
                // synchronizer.BeginSyncContainers();

                AppendToLogger("Ending Epoch " + Configuration.Epoch);
                Configuration.EndCurrentEpoch();

                Configuration.PrimaryServers.Add(ServerName);

                //add the new primary also to the list of write_only primaries. 
                //Servers in this list are not registered in session state, hence a get operation cannot be issued for them.
                //This makes put operations to go to all primaries (including the new one), but get_primary only goes to old primaries (those that does not exist in not register list).
                Configuration.WriteOnlyPrimaryServers.Add(ServerName);

                AppendToLogger("Starting the new Epoch");
                Configuration.StartNewEpoch();

                //wait until sync is finihsed
                AppendToLogger("Wait to finish Synchronization");
                // synchronizer.EndSyncContainers();
                synchronizer.SyncContainers();
            }

            AppendToLogger("Synchronization finished. Ending current epoch again.");
            Configuration.EndCurrentEpoch();

            //clear the not registered primary list, so the new primary can also be registered and used for get_primary operations.
            Configuration.WriteOnlyPrimaryServers.Clear();
            
            //all primaries will become secondaries, hence they are added to secondary list, and their sync period is set.
            Configuration.PrimaryServers.ForEach(s => { if (!s.Equals(ServerName)) Configuration.SecondaryServers.Add(s); Configuration.SetSyncPeriod(s, ConstPool.DEFAULT_SYNC_INTERVAL); });
            Configuration.PrimaryServers.Clear();

            //new primary is added to primary list.
            Configuration.PrimaryServers.Add(ServerName);

            //if the new primary has been a secondary, it is removed from the secondary list.
            if (Configuration.SecondaryServers.Contains(ServerName))
            {
                Configuration.SecondaryServers.Remove(ServerName);
            }

            //Finally, update the lookup service by writing the new configuration to the cloud, so every other client can read it.
            AppendToLogger("Starting the new Epoch with One Primary");
            Configuration.StartNewEpoch();
        }

        public override ReplicaConfiguration IfApply(ReplicaConfiguration current)
        {
            List<string> primary = new List<string>();
            List<string> secondary = new List<string>();

            current.SecondaryServers.ForEach(s => { if (s != ServerName) secondary.Add(s); else primary.Add(s); });
            current.PrimaryServers.ForEach(s => { if (s == ServerName) primary.Add(s); else secondary.Add(s); });
            current.NonReplicaServers.ForEach(s => { if (s == ServerName) primary.Add(s); });

            ReplicaConfiguration newConfig = new ReplicaConfiguration(current.Name, primary, secondary, null, null, false, true);
            return newConfig;
        }

        public override int NumberOfAddingReplica()
        {
            if (Configuration.SecondaryServers.Contains(ServerName) || Configuration.PrimaryServers.Contains(ServerName))
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
            DowngradePrimary p = new DowngradePrimary(Configuration.Name, ServerName, 0,null,ConfigurationActionSource.Constraint,numberOfReads,numberOfWrites);
            if (Configuration.SecondaryServers.Contains(ServerName))
            {
                //all primaries will become secondary, 
                result = Configuration.PrimaryServers.Count * p.ComputeCost();

                //a secondary will become primary:
                int secondarySyncPeriod = Configuration.GetSyncPeriod(ServerName);
                result += CostModel.GetPrimaryTransactionalCost(numberOfReads, numberOfWrites) - (CostModel.GetSecondaryTransactionalCost(numberOfReads) + CostModel.GetSyncCost(numberOfWrites, secondarySyncPeriod));
            }
            else if (Configuration.PrimaryServers.Contains(ServerName))
            {
                //all primaries except one will become secondary
                result = (Configuration.PrimaryServers.Count-1) * p.ComputeCost();
            }
            else
            {
                // all primaries will become secondary
                result = Configuration.PrimaryServers.Count * p.ComputeCost();

                //one non-replica becomes primary
                result += CostModel.GetPrimaryTransactionalCost(numberOfReads, numberOfWrites) + CostModel.GetStorageCost(ClientRegistry.GetMainPrimaryContainer(Configuration.Name));
            }

            return result;
        }
    }
}
