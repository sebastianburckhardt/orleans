using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Pileus.Configuration.Actions;
using System.Diagnostics;

namespace Microsoft.WindowsAzure.Storage.Pileus.Configuration.Constraint
{
    /// <summary>
    /// Ensures that replication factor for the given container remains between the given min and max. 
    /// 
    /// TODO: the apply function is not still very clever. So, it cannot pick several actions.
    /// One major problem is as follows:
    /// consider this scenario: one primary replica is in Asia, and one secondary is in Europe. 
    /// Replication factor is less than or equals to 2. There is also a constraint that says always keeps a replica in Asia.
    /// We would like to add a replica in U.S.. In this case, thic class only is able to remove the secondary replica from europe, and add it to U.S.
    /// However, the best approach is downgrade Asia to secondary, and add U.S. primary.
    /// But currently, this is not implemented. 
    /// </summary>
    public class ReplicationFactorConstraint: ConfigurationConstraint
    {
        private string containerName;

        public ReplicationFactorConstraint(string containerName, ReplicaConfiguration currentConfig, int minReplicationFactor, int MaxReplicationFactor)
            : base(containerName, currentConfig)
        {
            this.MinReplicationFactor = minReplicationFactor;
            this.MaxReplicationFactor = MaxReplicationFactor;
            this.containerName = containerName;

            if (MaxReplicationFactor < minReplicationFactor)
                throw new ArgumentException("Minimum is greater than maximum!");
        }

        public int MinReplicationFactor{get; private set;}
        public int MaxReplicationFactor{get; private set;}

        /// <summary>
        /// Applies the constraint to the given actions.
        /// Note that this function can add several new actions to the list of new actions. Those actions that are added by this constraint set their source field to Constraint.
        /// So, the reconfigurator will notice whether the action is added by the Apply from the constraint or is computed by itself. 
        /// </summary>
        /// <param name="newActions"></param>
        /// <param name="constraints"></param>
        /// <param name="SLAs"></param>
        /// <param name="sessionStates"></param>
        internal override void Apply(List<ConfigurationAction> newActions, List<ConfigurationConstraint> constraints, SortedSet<ServiceLevelAgreement> SLAs, Dictionary<string, ClientUsageData> clientData)
        {
            int currentReplicaFactor = Configuration.PrimaryServers.Count + Configuration.SecondaryServers.Count;
            newActions.ForEach(a => currentReplicaFactor += a.NumberOfAddingReplica());

            if (currentReplicaFactor >= MinReplicationFactor && currentReplicaFactor <= MaxReplicationFactor)
                return;
            
            if (currentReplicaFactor < MinReplicationFactor)
            {
                //We have to add some secondary replica
                //We assume here that configurator does not add any remove replica action because configurator has a tendency of adding replicas
                //Without this assumption, we first need to see if there is such an action, and erase that action from newActions list instead of adding a new replica.
                
                // Make copy of list of non-replica servers
                List<string> availableServers = new List<string>();
                foreach (string server in Configuration.NonReplicaServers)
                {
                    availableServers.Add(server);
                }
                
                int mustAdd = MinReplicationFactor - currentReplicaFactor;

                foreach (ConfigurationAction action in newActions)
                {
                    if (action.NumberOfAddingReplica() > 0)
                        availableServers.Remove(action.ServerName);
                    else if (action.NumberOfAddingReplica() < 0)
                        availableServers.Add(action.ServerName);
                }


                if (availableServers.Count < mustAdd)
                    throw new Exception("There are not enough servers to enforce this constraint.");

                List<ConfigurationAction> toBeAdded = new List<ConfigurationAction>();
                while (mustAdd > 0)
                {
                    //we should add a new replica
                    string serverName=availableServers.First();
                    ConfigurationAction action = new AddSecondaryServer(Configuration.Name, serverName, 0, null, ConfigurationActionSource.Constraint);
                    bool compatible = true;
                    foreach (ConfigurationConstraint constraint in constraints)
                    {
                        if (!constraint.Compatible(action))
                        {
                            compatible = false;
                            break;
                        }
                    }

                    if (compatible)
                    {
                        toBeAdded.Add(action);
                        mustAdd--;
                    }

                    availableServers.RemoveAt(0);
                    if (mustAdd > 0 && availableServers.Count == 0)
                        throw new Exception("There are not enough servers to enforce this constraint.");
                }

                toBeAdded.ForEach(a => newActions.Add(a));
            }

            if (currentReplicaFactor > MaxReplicationFactor)
            {
                //The asumption for the minimum replication factor does not hold here.
                //I.e., there can be an action in newActions list that is adding a new replica, hence total  number of replicas has become more than the maximum allowed.
                //Therefore, instead of creating a new action to remove a replica, we first need to remove addReplica action from the newActions list. 

                int mustRemove = currentReplicaFactor - MaxReplicationFactor;

                List<ConfigurationAction> toBeRemoved = new List<ConfigurationAction>();

                //first, we try to remove a secondary replica to meet the constraint.
                List<RemoveSecondaryServer> removeActions = new List<RemoveSecondaryServer>();
                
                // TODO: Make sure that this is doing the right thing.  This code seems strange to me.  
                // Why only the first SLA?  And why remove all secondaries?

                ServiceLevelAgreement firstSLA = SLAs.First();
                string clientName = "";
                foreach (ClientUsageData usage in clientData.Values)
                {
                    if (usage.SLAs.Contains(firstSLA))
                    {
                        clientName = usage.ClientName;
                        break;
                    }
                }
                
                foreach (string server in Configuration.SecondaryServers)
                {
                    RemoveSecondaryServer action = new RemoveSecondaryServer(containerName, server, 0, null, ConfigurationActionSource.Constraint, clientData[clientName].NumberOfReads, clientData[clientName].NumberOfWrites);
                    action.Clients.Add(clientName);
                    removeActions.Add(action);
                }

                foreach (string client in clientData.Keys)
                {
                    foreach (ServiceLevelAgreement sla in clientData[client].SLAs)
                    {
                        foreach (ConfigurationAction action in removeActions)
                        {
                            if (!action.OriginatingSLAs.Contains(sla.Id) || !action.Clients.Contains(client))
                            {
                                ActionSelector.CheckAction(action, sla, clientData[client], Configuration);
                            }

                        }
                    }
                }

                removeActions.Sort(new ConfigurationActionComparer());
                float totalLostUtility=0;
                float totalGainUtility=0;
                newActions.ForEach(a => totalGainUtility += a.GainedUtility);

                foreach (RemoveSecondaryServer action in removeActions)
                {
                    if (action.GainedUtility + totalLostUtility + totalGainUtility > 0)
                    {
                        //we need one final test.
                        //we need to make sure that this action does not violate any other constraint.
                        bool compatible=true;
                        foreach (ConfigurationConstraint cc in constraints)
                        {
                            if (!cc.Compatible(action))
                            {
                                compatible = false;
                                break;
                            }

                        }
                        if (compatible)
                        {
                            totalLostUtility += action.GainedUtility;
                            mustRemove--;
                            newActions.Add(action);
                        }
                    }
                    if (mustRemove == 0)
                        return;
                }


                //then, we adding a new replica in newactions
                foreach (ConfigurationAction action in newActions)
                {
                    if (action.Source == ConfigurationActionSource.NonConstraint && action.NumberOfAddingReplica() > 0)
                    {
                        toBeRemoved.Add(action);
                        mustRemove--;
                    }

                    if (mustRemove == 0)
                        break;
                }

                newActions.RemoveAll(r => toBeRemoved.Contains(r));

                if (mustRemove == 0)
                    return;

                List<ConfigurationAction> toBeAdded = new List<ConfigurationAction>();
                //Removing configurator actions does not suffice to ensure the constraint.
                //Hence, we need to start removing secondary replicas. 
                while (mustRemove > 0 && Configuration.SecondaryServers.Count>0)
                {
                    string serverName = Configuration.SecondaryServers.First();
                    ConfigurationAction action = new RemoveSecondaryServer(Configuration.Name, serverName, 0, null, ConfigurationActionSource.Constraint);
                    bool compatible = true;
                    foreach (ConfigurationConstraint constraint in constraints)
                    {
                        if (!constraint.Compatible(action))
                        {
                            compatible = false;
                            break;
                        }
                    }

                    if (compatible)
                    {
                        toBeAdded.Add(action);
                        mustRemove--;
                    }
                }
                toBeAdded.ForEach(a => newActions.Add(a));

                if (mustRemove == 0)
                    return;

                //Removing secondaries was not enough either. we have to start removing primaries.
                toBeAdded = new List<ConfigurationAction>();
                while (mustRemove > 0 && Configuration.PrimaryServers.Count > 1)
                {
                    string serverName = Configuration.PrimaryServers.Last();
                    ConfigurationAction action1 = new DowngradePrimary(Configuration.Name, serverName, 0, null, ConfigurationActionSource.Constraint);
                    ConfigurationAction action2 = new RemoveSecondaryServer(Configuration.Name, serverName, 0, null, ConfigurationActionSource.Constraint);
                    bool compatible = true;
                    foreach (ConfigurationConstraint constraint in constraints)
                    {
                        if (!constraint.Compatible(action1) || !constraint.Compatible(action2))
                        {
                            compatible = false;
                            break;
                        }
                    }

                    if (compatible)
                    {
                        toBeAdded.Add(action1);
                        toBeAdded.Add(action2);
                        mustRemove--;
                    }
                }
                Debug.Assert(mustRemove == 0, "mustRemove should be zero by now. Something is wrong");
                toBeAdded.ForEach(a => newActions.Add(a));
            }

        }

        public override bool Compatible(ConfigurationAction action)
        {
            return true;
        }

        public override int GetPriority()
        {
            return 2;
        }

    }
}
