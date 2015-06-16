using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Pileus.Utils;

namespace Microsoft.WindowsAzure.Storage.Pileus.Configuration.Actions
{
    /// <summary>
    /// Makes the given primary server, a secondary serve. 
    /// </summary>
    internal class DowngradePrimary : ConfigurationAction
    {
        public DowngradePrimary(string containerName, string serverName, float? utility, string slaId = null, ConfigurationActionSource source = ConfigurationActionSource.Constraint, int numberOfReads = 0, int numberOfWrites = 0)
            : base(containerName, serverName, utility, slaId, source,numberOfReads,numberOfWrites)
        {

        }

        public override void Execute()
        {
            if (Configuration.PrimaryServers.Count == 1)
                throw new Exception("There is only one primary in the system.");

            AppendToLogger("Ending Epoch " + Configuration.Epoch);
            Configuration.EndCurrentEpoch();

            Configuration.PrimaryServers.Remove(ServerName);
            Configuration.SecondaryServers.Add(ServerName);

            AppendToLogger("Starting the new Epoch");
            Configuration.StartNewEpoch();
        }

        public override ReplicaConfiguration IfApply(ReplicaConfiguration current)
        {
            List<string> primary = new List<string>();
            List<string> secondary = new List<string>();

            current.SecondaryServers.ForEach(s => { secondary.Add(s); });
            current.PrimaryServers.ForEach(s => { if (s != ServerName) primary.Add(s); else secondary.Add(s); });

            return new ReplicaConfiguration(current.Name, primary, secondary);
        }

        public override int NumberOfAddingReplica()
        {
            return 0;
        }

        public override double ComputeCost()
        {
            double result = 0;
            double secondaryTransactionalCost = CostModel.GetSecondaryTransactionalCost(numberOfReads);
            double syncCost=CostModel.GetSyncCost(numberOfWrites, ConstPool.DEFAULT_SYNC_INTERVAL);
            double primaryTransactionalCost=CostModel.GetPrimaryTransactionalCost(numberOfReads, numberOfWrites);
            result = (secondaryTransactionalCost + syncCost) - primaryTransactionalCost;

            return result;
        }
    }
}
