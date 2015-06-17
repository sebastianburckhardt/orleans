using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Pileus.Configuration.Actions;

namespace Microsoft.WindowsAzure.Storage.Pileus.Configuration.Constraint
{
    public class LocationConstraint : ConfigurationConstraint
    {
        public LocationConstraint(string containerName, ReplicaConfiguration currentConfig, string serverName, LocationConstraintType type)
            : base(containerName, currentConfig)
        {
            this.Type = type;
            this.ConstrainedContainer = ClientRegistry.GetCloudBlobContainer(serverName, containerName);
            this.ServerName = serverName;
        }

        private LocationConstraintType Type {get; set;}

        private CloudBlobContainer ConstrainedContainer { get; set; }

        private string ServerName { get; set; }

        internal override void Apply(List<ConfigurationAction> newActions, List<ConfigurationConstraint> constraints, SortedSet<ServiceLevelAgreement> SLAs, Dictionary<string, ClientUsageData> clientData)
        {
            if (Type == LocationConstraintType.Replicate)
            {
                //first, we check if the tablet is already replicated in the particular server or not.
                if (Configuration.SecondaryServers.Contains(ServerName) || Configuration.PrimaryServers.Contains(ServerName))
                {
                    //the constraint is already in place. we just have to make sure that no new action in actions list remove it.
                    ConfigurationAction toRemove = null;
                    foreach (ConfigurationAction action in newActions)
                    {
                        if (action.GetType() == typeof(RemoveSecondaryServer) && action.ServerName.Equals(this.ServerName))
                        {
                            toRemove = action;
                            break;
                        }
                    }
                    if (toRemove != null)
                    {
                        newActions.Remove(toRemove);
                        return;
                    }
                }
                else
                {
                    //the constraint is not being enforced. We need to enforce it by creating a new replica.
                    //First we see if the new replica is being created in the new actions
                    foreach (ConfigurationAction action in newActions)
                    {
                        if ((action.GetType() == typeof(AddPrimaryServer) || action.GetType() == typeof(AddSecondaryServer) || action.GetType() == typeof(MakeSoloPrimaryServer)) && action.ServerName.Equals(this.ServerName))
                        {
                            return;
                        }
                    }
                    //we could not find an action enforcing the this constraint.
                    //Hence, we add an action with the minimum cost ourselves. I.e., adding secondary server.
                    newActions.Add(new AddSecondaryServer(Configuration.Name, ServerName, 0,null,ConfigurationActionSource.Constraint));
                }
            }
            else
            {
                //we need to avoid replicating in a particular server.
                //first, we check if the tablet is already replicated in the particular server or not.
                if (Configuration.SecondaryServers.Contains(ServerName) || Configuration.PrimaryServers.Contains(ServerName))
                {
                    //we have to enforce the constraint because it is already being replicated.
                    //We first check to see if there exists a new action removing the container or not.
                    List<ConfigurationAction> toRemove = new List<ConfigurationAction>();
                    foreach (ConfigurationAction action in newActions)
                    {
                        if (action.ModifyingContainer.Name.Equals(ConstrainedContainer.Name) && action.ServerName.Equals(this.ServerName))
                        {
                            toRemove.Add(action);
                        }
                    }
                    //we have to remove all actions on a particular container of this server
                    newActions.RemoveAll(o => toRemove.Contains(o));

                    //we finally add a remove action.
                    if (Configuration.PrimaryServers.Contains(ServerName))
                    {
                        //we first downgrade the primary
                        newActions.Add(new DowngradePrimary(Configuration.Name, ServerName, 0,null,ConfigurationActionSource.Constraint));
                    }
                    //we totally remove the replica
                    newActions.Add(new RemoveSecondaryServer(Configuration.Name, ServerName, 0, null, ConfigurationActionSource.Constraint));

                }
                else
                {
                    //The container is not being replicated in the server.
                    //We just need to make sure that a new action does not replicate it. 
                    List<ConfigurationAction> toRemove = new List<ConfigurationAction>();
                    foreach (ConfigurationAction action in newActions)
                    {
                        if (action.ModifyingContainer.Name.Equals(ConstrainedContainer.Name) && action.ServerName.Equals(this.ServerName))
                        {
                            toRemove.Add(action);
                        }
                    }
                    //we have to remove all actions on this container
                    newActions.RemoveAll(o => toRemove.Contains(o));
                }
            }
        }

        public override bool Compatible(ConfigurationAction action)
        {
            if (!action.ModifyingContainer.Name.Equals( this.ConstrainedContainer.Name) || !action.ServerName.Equals(this.ServerName))
            {
                return true;
            }

            if (Type == LocationConstraintType.Replicate && action.GetType() == typeof(RemoveSecondaryServer))
            {
                return false;
            }
            else if (Type == LocationConstraintType.DoNotReplicate && (action.GetType() == typeof(AddPrimaryServer) || action.GetType() == typeof(AddSecondaryServer) || action.GetType() == typeof(MakeSoloPrimaryServer)))
            {
                return false;
            }

            return true;
        }

        public override int GetPriority()
        {
            return 0;
        }
    }

    public enum LocationConstraintType{
        Replicate,
        DoNotReplicate
    }
}
