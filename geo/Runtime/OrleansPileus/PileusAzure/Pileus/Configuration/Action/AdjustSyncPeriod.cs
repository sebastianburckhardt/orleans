using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Pileus.Utils;

namespace Microsoft.WindowsAzure.Storage.Pileus.Configuration.Actions
{
    /// <summary>
    /// Adjust sync period between a primary replica and the given secondary replica.
    /// </summary>
    internal class AdjustSyncPeriod : ConfigurationAction
    {
        public AdjustSyncPeriod(string containerName, string serverName, float? utility, string slaId = null, ConfigurationActionSource source = ConfigurationActionSource.Constraint, int numberOfReads = 0, int numberOfWrites = 0)
            : base(containerName, serverName, utility, slaId, source,numberOfReads,numberOfWrites)
        {
        }

        public override void Execute()
        {
            Configuration.SetSyncPeriod(ServerName, Convert.ToInt32(OldSyncPeriod() * ConstPool.ADJUSTING_SYNC_INTERVAL_MULTIPLIER));
            AppendToLogger("Starting the new Epoch");
            Configuration.StartNewEpoch();
        }

        public override ReplicaConfiguration IfApply(ReplicaConfiguration current)
        {
            //return new SessionState(sessionState.GetSecondaryReplicaServers().Values.ToList(), sessionState.GetPrimaryReplicaServers().Values.ToList());
            return current;
        }

        public override int NumberOfAddingReplica()
        {
            return 0;
        }

        public override double ComputeCost()
        {
            double newCost = CostModel.GetSyncCost(numberOfWrites, Convert.ToInt32(OldSyncPeriod() * ConstPool.ADJUSTING_SYNC_INTERVAL_MULTIPLIER));
            double oldCost=CostModel.GetSyncCost(numberOfWrites, OldSyncPeriod());
            return newCost-oldCost;
        }

        private int OldSyncPeriod()
        {
            return Configuration.GetSyncPeriod(ServerName);
        }
    }
}
