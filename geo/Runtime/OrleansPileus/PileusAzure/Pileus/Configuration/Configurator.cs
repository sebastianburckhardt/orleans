using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Pileus.Utils;
using Microsoft.WindowsAzure.Storage.Pileus.Configuration.Actions;
using Microsoft.WindowsAzure.Storage.Pileus.Configuration.Constraint;

namespace Microsoft.WindowsAzure.Storage.Pileus.Configuration
{
    public class Configurator
    {

        public static string Logs = "";

        private string containerName { get; set; }

        public Configurator(string containerName)
        {
            this.containerName=containerName;
        }

        public void Configure(CloudStorageAccount account, int currentConfigurationEpoch, ReplicaConfiguration currentConfig, List<ConfigurationConstraint> constraints)
        {
            List<ConfigurationAction> pickedAction = PickNewConfiguration(account, currentConfigurationEpoch, currentConfig, constraints);

            InstallNewConfiguration(pickedAction);
        }

        /// <summary>
        /// Picks a new configuration given a single SLA and single client's session state
        /// </summary>
        /// <param name="sla">The SLA for which the new configuration should be tailored</param>
        /// <param name="ss">The session state</param>
        /// <param name="constraints">The constraints</param>
        /// <returns>a set of reconfiguration actions</returns>
        public List<ConfigurationAction> PickNewConfiguration(string containerName, ServiceLevelAgreement sla, SessionState ss, ServerMonitor monitor, ReplicaConfiguration config, List<ConfigurationConstraint> constraints)
        {
            Dictionary<string, ClientUsageData> clientData = new Dictionary<string,ClientUsageData>();
            ClientUsageData usage = new ClientUsageData("local");

            // Convert args into client data
            usage.SLAs.Add(sla);
            usage.NumberOfReads = ss.GetNumberOfReadsPerMonth();
            usage.NumberOfWrites = ss.GetNumberOfWritesPerMonth();
            usage.ServerRTTs = new Dictionary<string, LatencyDistribution>();
            foreach (ServerState server in monitor.GetAllServersState())
            {
                usage.ServerRTTs.Add(server.Name, server.RTTs);
            }

            // Use client data for a single user
            clientData.Add(usage.ClientName, usage);

            // Choose actions to produce a better configuration
            return ChooseReconfigActions(clientData, constraints, config);
        }

        public List<ConfigurationAction> PickNewConfiguration(CloudStorageAccount account, int currentConfigurationEpoch, ReplicaConfiguration config, List<ConfigurationConstraint> constraints)
        {
            // Read client's data
            Dictionary<string, ClientUsageData> clientData;
            ClientUsageCloudStore store = new ClientUsageCloudStore(account, "configurator");
            clientData = store.ReadClientData(containerName, currentConfigurationEpoch);

            // Choose actions to produce a better configuration
            return ChooseReconfigActions(clientData, constraints, config);
        }

        public List<ConfigurationAction> SetConfigurationPrimary(string containerName, string primary)
        {
            List<ConfigurationAction> result = new List<ConfigurationAction>();
            result.Add(ActionSelector.GetMakeSoloPrimaryAction(containerName, primary));
            return result;
        }

        private List<ConfigurationAction> ChooseReconfigActions(Dictionary<string, ClientUsageData> clientData, List<ConfigurationConstraint> constraints, ReplicaConfiguration config)
        {
             Dictionary<string, ConfigurationAction> actions = new Dictionary<string, ConfigurationAction>();
            
            // build sorted list of all SLAs
            SortedSet<ServiceLevelAgreement> SLAs = new SortedSet<ServiceLevelAgreement>(new ServiceLevelAgreementComparer());
            foreach (ClientUsageData client in clientData.Values)
            {
                SLAs.UnionWith(client.SLAs);
            }

            AppendToLogger("Start Computing Actions");
            foreach (string clientName in clientData.Keys)
            {
                foreach (ServiceLevelAgreement sla in clientData[clientName].SLAs)
                {

                    Dictionary<string, ConfigurationAction> tmp = ActionSelector.ComputeActions(this.containerName, config, sla, clientData[clientName]);
                    foreach (string id in tmp.Keys)
                    {
                        if (!actions.ContainsKey(id))
                        {
                            actions[id] = tmp[id];
                        }
                        else
                        {
                            actions[id].Merge(tmp[id]);
                        }

                        //We keep track of clients. We will use this list in the checking phase (below)
                        actions[id].Clients.Add(clientName);

                        AppendToLogger(tmp[id].ToString());
                    }
                }
            }
            
            AppendToLogger("Start Checking Actions");
            foreach (string clientName in clientData.Keys)
            {
                foreach (ServiceLevelAgreement sla in clientData[clientName].SLAs)
                {
                    foreach (ConfigurationAction action in actions.Values)
                    {
                        if (!action.OriginatingSLAs.Contains(sla.Id) || !action.Clients.Contains(clientName))
                        {
                            ActionSelector.CheckAction(action, sla, clientData[clientName], config);
                        }

                    }
                }
            }

            foreach (ConfigurationAction action in actions.Values)
            {
                AppendToLogger(action.ToString());
            }

            List<ConfigurationAction> pickedAction = new List<ConfigurationAction>();
            List<ConfigurationAction> sortedActions = actions.Values.ToList();

            //we do not even consider actions with negative gained utility. 
            sortedActions.RemoveAll(e => e.GainedUtility < 0);
            sortedActions.Sort(new ConfigurationActionComparer());

            // we sort constraints based on their priority.
            constraints.Sort(new ConfigurationConstraintComparer());

            bool foundAction = false;
            for (int i = sortedActions.Count - 1; i >= 0; i--)
            {
                pickedAction.Clear();
                pickedAction.Add(sortedActions[i]);

                if (constraints.Count() == 0)
                {
                    foundAction = true;
                    break;
                }

                //Remove actions that are not satisfying constraints. 
                foreach (ConfigurationConstraint c in constraints)
                {
                    c.Apply(pickedAction, constraints, SLAs, clientData);
                    if (pickedAction.All(e => e.Source == ConfigurationActionSource.Constraint))
                    {
                        //the action is removed because of conflict with other constraints. 
                        foundAction = false;
                        break;
                    }
                    else
                    {
                        foundAction = true;
                    }
                }

                if (foundAction)
                {
                    break;
                }
                
            }

            if (!foundAction)
            {
                //no action is applicable with all constraints. so, we just try to apply constraints. 
                //hence, no non-constraint action will be executed to improve utility. 
                pickedAction.Clear();
                foreach (ConfigurationConstraint c in constraints)
                {
                    c.Apply(pickedAction, constraints, SLAs, clientData);
                }
            }

            return pickedAction;
        }

        public void InstallNewConfiguration(List<ConfigurationAction> pickedAction)
        {
            // execute all actions in pickedAction
            foreach (ConfigurationAction action in pickedAction)
            {
                AppendToLogger("Chosen Action : " + action.ToString());
                action.Execute();
                Console.WriteLine("Action "  + action.Id + " finished.");
                Logs += action.Logs;
            }
        }

        /// <summary>
        /// Picks a new configuration given a single SLA and single client's session state
        /// </summary>
        /// <param name="sla">The SLA for which the new configuration should be tailored</param>
        /// <param name="ss">The session state</param>
        /// <param name="constraints">The constraints</param>
        /// <returns>a set of reconfiguration actions</returns>
        public float ComputeUtilityGainFromNewConfiguration(string containerName, ServiceLevelAgreement sla, SessionState ss, ServerMonitor monitor, ReplicaConfiguration config, List<ConfigurationAction> actions)
        {
            Dictionary<string, ClientUsageData> clientData = new Dictionary<string, ClientUsageData>();
            ClientUsageData usage = new ClientUsageData("local");

            // Convert args into client data
            usage.SLAs.Add(sla);
            usage.NumberOfReads = ss.GetNumberOfReadsPerMonth();
            usage.NumberOfWrites = ss.GetNumberOfWritesPerMonth();
            usage.ServerRTTs = new Dictionary<string, LatencyDistribution>();
            foreach (ServerState server in monitor.GetAllServersState())
            {
                usage.ServerRTTs.Add(server.Name, server.RTTs);
            }

            // Use client data for a single user
            clientData.Add(usage.ClientName, usage);

            return ComputeUtilityGain(config, actions, clientData);
        }

        private float ComputeUtilityGain(ReplicaConfiguration config, List<ConfigurationAction> actions, Dictionary<string, ClientUsageData> clientData)
        {
            ReplicaConfiguration newConfig = config;
            foreach (ConfigurationAction action in actions)
            {
                newConfig = action.IfApply(newConfig);
            }

            float gain = ExpectedUtilityForClients(newConfig, clientData) - DeliveredUtilityForClients(config, clientData);
            return gain;
        }

        // Compute actual delivered utility as a weighted average over all of the SLAs used by all clients
        private float DeliveredUtilityForClients(ReplicaConfiguration config, Dictionary<string, ClientUsageData> clientData)
        {
            float avgDeliveredUtility = 0;
            int totalReads = clientData.Values.Sum(client => client.NumberOfReads);
            float totalUtility = 0;
            foreach (string clientName in clientData.Keys)
            {
                float totalClientUtility = 0;
                foreach (ServiceLevelAgreement sla in clientData[clientName].SLAs)
                {
                    float slaUtility = sla.GetAverageDeliveredUtility();
                    totalClientUtility += slaUtility;
                }
                float avgClientUtility = 0;
                if (totalClientUtility > 0)
                {
                    avgClientUtility = totalClientUtility / clientData[clientName].SLAs.Count;
                }
                totalUtility += avgClientUtility * clientData[clientName].NumberOfReads;
            }
            if (totalReads != 0)
            {
                avgDeliveredUtility = totalUtility / totalReads;
            }
            return avgDeliveredUtility;
        }

        // Compute expected utility as a weighted average over all of the SLAs used by all clients
        private float ExpectedUtilityForClients(ReplicaConfiguration config, Dictionary<string, ClientUsageData> clientData)
        {
            float avgExpectedUtility = 0;
            int totalReads = clientData.Values.Sum(client => client.NumberOfReads);
            float totalUtility = 0;
            foreach (string clientName in clientData.Keys)
            {
                float totalClientUtility = 0;
                foreach (ServiceLevelAgreement sla in clientData[clientName].SLAs)
                {
                    float slaUtility = ExpectedUtilityForSla(config, sla, clientData[clientName].ServerRTTs);
                    totalClientUtility += slaUtility;
                }
                float avgClientUtility = 0;
                if (totalClientUtility > 0) {
                    avgClientUtility = totalClientUtility / clientData[clientName].SLAs.Count;
                }
                totalUtility += avgClientUtility * clientData[clientName].NumberOfReads;
            }
            if (totalReads != 0)
            {
                avgExpectedUtility = totalUtility / totalReads;
            }
            return avgExpectedUtility;
        }

        private float ExpectedUtilityForSla(ReplicaConfiguration config, ServiceLevelAgreement sla, Dictionary<string, LatencyDistribution> serverRTTs)
        {
            float utility = 0;
            float cumulativeProb = 0;
            float prob = 0;
            List<string> allServers = config.GetServers(true, true, false);
            foreach (SubSLA sub in sla)
            {
                foreach (string server in allServers)
                {
                    prob = allServers.Max(s => serverRTTs[s].ProbabilityOfFindingValueLessThanGiven(sub.Latency) 
                        * ProbabilityOfServerConsistency(config, s, sub.Consistency, sub.Bound));
                }
                float subUtility = (1 - cumulativeProb) * prob * sub.Utility;
                utility += subUtility;
                cumulativeProb += (1 - cumulativeProb) * prob;
            }
            return utility;
        }

        // Returns the likelihood that a given server is sufficiently up-to-date to meet the given consistency
        // For many consistencies, this is hard to predict, and better predicitve models are possible.
        public float ProbabilityOfServerConsistency(ReplicaConfiguration config, string server, Consistency consistency, int bound)
        {
            float prob = 0;

            switch (consistency)
            {
                case Consistency.Strong:
                    if (config.PrimaryServers.Contains(server) && !config.WriteOnlyPrimaryServers.Contains(server))
                    {
                        prob = 1.0F;
                    }
                    break;

                case Consistency.ReadMyWrites:
                case Consistency.Session:
                    // could possibly compute probability based on past performance and read/write ratio
                    // for now, arbitrarily return 0.5
                    prob = 0.5F;
                    break;

                case Consistency.MonotonicReads:
                    // almost always will read from the closest server
                    prob = 1.0F;
                    break;

                case Consistency.Causal:
                    // could possibly compute probability based on past performance and read/write ratio
                    // for now, arbitrarily return 0.25
                    prob = 0.25F;
                    break;

                case Consistency.Bounded:
                case Consistency.BoundedMonotonicReads:
                    // depends on the sync frequency
                    if (bound >= config.GetSyncPeriod(server))
                    {
                        prob = 1.0F;
                    }
                    else
                    {
                        prob = bound / config.GetSyncPeriod(server);
                    }
                    break;

                case Consistency.BoundedReadMyWrites:
                case Consistency.BoundedSession:
                    // depends on the sync frequency
                    if (bound >= config.GetSyncPeriod(server))
                    {
                        prob = 0.5F;
                    }
                    else
                    {
                        prob = bound / config.GetSyncPeriod(server);
                    }
                    break;

                case Consistency.Eventual:
                    if (config.PrimaryServers.Contains(server) || config.SecondaryServers.Contains(server))
                    {
                        prob = 1.0F;
                    }
                    break;

                default:
                    prob = 0;
                    break;
            }

            return prob;
        }

        protected void AppendToLogger(string log)
        {
            long curSecond = DateTime.Now.ToUniversalTime().Ticks / 10000000;
            Logs += curSecond + "," + log + "\n";
        }
    }
}
