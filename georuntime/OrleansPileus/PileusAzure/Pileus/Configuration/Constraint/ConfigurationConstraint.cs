using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Pileus.Configuration.Actions;

namespace Microsoft.WindowsAzure.Storage.Pileus.Configuration.Constraint
{
    public abstract class ConfigurationConstraint
    {
        public ConfigurationConstraint(string containerName, ReplicaConfiguration currentConfig)
        {
            this.Configuration = currentConfig;
        }

        // Container's configuration
        protected ReplicaConfiguration Configuration { get; set; }

        /// <summary>
        /// Applies the constraint to the list of actions. Hence, it might add or remove some actions.
        /// </summary>
        /// <param name="actions"></param>
        internal abstract void Apply(List<ConfigurationAction> newActions, List<ConfigurationConstraint> constraints, SortedSet<ServiceLevelAgreement> SLAs, Dictionary<string, ClientUsageData> clientData);

        /// <summary>
        /// Checks whether the given action is compatible (applicable) with this constraint.
        /// </summary>
        /// <param name="action"></param>
        /// <returns>true if the action is compatible (applicable) with this constraint, false otherwise.</returns>
        public abstract bool Compatible(ConfigurationAction action);

        /// <summary>
        /// states the priority of this constraint compare to others.
        /// This priority is used for applying the constraint on selected actions (the smaller it is the higher it is).
        /// Hence, a constraint with the lowest posible priority will be applied to the list of actions first, and so on. 
        /// </summary>
        /// <returns></returns>
        public abstract int GetPriority();

    }

    public class ConfigurationConstraintComparer : IComparer<ConfigurationConstraint>
    {
        public int Compare(ConfigurationConstraint arg0, ConfigurationConstraint arg1)
        {
            if (arg0.GetPriority() < arg1.GetPriority())
            {
                return -1;
            }
            else
            {
                return 1;
            }
        }
    } 
}
