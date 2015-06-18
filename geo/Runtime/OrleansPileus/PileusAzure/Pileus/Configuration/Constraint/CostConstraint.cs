using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Pileus.Configuration.Actions;


namespace Microsoft.WindowsAzure.Storage.Pileus.Configuration.Constraint
{
    public class CostConstraint: ConfigurationConstraint
    {
        private double costLimit;

        public CostConstraint(string containerName, ReplicaConfiguration currentConfig, double costLimit)
            : base(containerName, currentConfig)
        {
            this.costLimit = costLimit;
        }

        internal override void Apply(List<ConfigurationAction> newActions, List<ConfigurationConstraint> constraints, SortedSet<ServiceLevelAgreement> SLAs, Dictionary<string, ClientUsageData> clientData)
        {
            List<ConfigurationAction> toRemove=new List<ConfigurationAction>();
            foreach (ConfigurationAction action in newActions)
            {
                if (action.ComputeCost() > costLimit)
                    toRemove.Add(action);
            }

            toRemove.ForEach(a => newActions.Remove(a));
        }

        public override bool Compatible(ConfigurationAction action)
        {
            return true;
        }

        public override int GetPriority()
        {
            return 1;
        }

    }
}
