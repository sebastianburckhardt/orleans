using Microsoft.WindowsAzure.Storage.Pileus.Configuration.Actions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.Storage.Pileus.Configuration.Constraint
{
    /// <summary>
    /// Prvents the primary replica (or set of primaries) from being modified.
    /// </summary>
    public class FixedPrimaryConstraint : ConfigurationConstraint
    {

        private string containerName;

        public FixedPrimaryConstraint(string containerName, ReplicaConfiguration currentConfig)
            : base(containerName, currentConfig)
        {
            this.containerName = containerName;
        }

        internal override void Apply(List<ConfigurationAction> newActions, List<ConfigurationConstraint> constraints, SortedSet<ServiceLevelAgreement> SLAs, Dictionary<string, ClientUsageData> clientData)
        {
            // TODO: not implemented.
        }

        public override bool Compatible(ConfigurationAction action)
        {
            // TODO: not implemented.
            return true;
        }

        public override int GetPriority()
        {
            return 1;
        }

    }
}
