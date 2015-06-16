using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Pileus.Utils;

namespace Microsoft.WindowsAzure.Storage.Pileus.Configuration.Actions
{
    /// <summary>
    /// Chooses actions for the provided SLA
    /// 
    /// </summary>
    static class ActionSelector
    {
        private static int numberOfReads;
        private static int numberOfWrites;

        /// <summary>
        /// Computes new actions to improve the utility of the given SLA
        /// </summary>
        /// <param name="ContainerName"></param>
        /// <param name="SLA"></param>
        /// <param name="state"></param>
        /// <returns></returns>
        public static Dictionary<string, ConfigurationAction> ComputeActions(string ContainerName, ReplicaConfiguration currentConfig, ServiceLevelAgreement SLA, ClientUsageData usage)
        {
            numberOfReads = usage.NumberOfReads;
            numberOfWrites = usage.NumberOfWrites;

            Dictionary<string, ConfigurationAction> result = new Dictionary<string, ConfigurationAction>();
            
            foreach (SubSLA s in SLA)
            {
                if (s.NumberOfMisses == 0)
                    return result;

                if (s.Consistency == Consistency.Strong)
                    GetActionsForStrongConsistency(ContainerName, currentConfig, s, usage, SLA.Id).ForEach(i => { if (!result.ContainsKey(i.Id)) result[i.Id] = i; else result[i.Id].Merge(i); });
                else if (s.Consistency == Consistency.Eventual)
                    GetActionsForEventualConsistency(ContainerName, currentConfig, s, usage, SLA.Id).ForEach(i => { if (!result.ContainsKey(i.Id)) result[i.Id] = i; else result[i.Id].Merge(i); });
                else GetActionsFor_RMW_MON_CAUSAL_BOUNDED_Consistency(ContainerName, currentConfig, s, usage, SLA.Id).ForEach(i => { if (!result.ContainsKey(i.Id)) result[i.Id] = i; else result[i.Id].Merge(i); });
            }

            return result;

        }

        /// <summary>
        /// Returns the action needed to move the primary replica
        /// </summary>
        /// <param name="ContainerName">name of the container</param>
        /// <param name="primary">name of the primary site</param>
        /// <returns>a single action</returns>
        public static ConfigurationAction GetMakeSoloPrimaryAction(string containerName, string primary)
        {
            ConfigurationAction result;
            result = new MakeSoloPrimaryServer(containerName, primary, 0, null, ConfigurationActionSource.NonConstraint, 0, 0);
            return result;
        }

        public static void CheckAction(ConfigurationAction action, ServiceLevelAgreement SLA, ClientUsageData usage, ReplicaConfiguration currentConfig)
        {
            ReplicaConfiguration newConfig = action.IfApply(currentConfig);
            foreach (SubSLA s in SLA)
            {
                float currentProbability = GetApplicableServers(s, currentConfig, usage).Max(server => server.ProbabilityOfFindingValueLessThanGiven(s.Latency));
                //probability of new state should be computed in pessimistic mode. I.e., if there is no entry in the distribution of session state, instead of returning 1, we return 0.
                float newProbability = GetApplicableServers(s, newConfig, usage).Max(server => server.ProbabilityOfFindingValueLessThanGiven(s.Latency, false));
                /* 
                // Not sure why the utility is multiplied by hits or misses
                if (newProbability > currentProbability)
                {
                    action.AddUtility((newProbability - currentProbability) * s.NumberOfMisses * s.Utility);
                }
                else
                {
                    action.AddUtility((newProbability - currentProbability) * s.NumberOfHits * s.Utility);
                }
                 * */
                action.AddUtility((newProbability - currentProbability) * s.Utility);
            }
        }


        private static HashSet<LatencyDistribution> GetApplicableServers(SubSLA s, ReplicaConfiguration config, ClientUsageData usage)
        {
            HashSet<LatencyDistribution> result = new HashSet<LatencyDistribution>();
            foreach (string name in config.PrimaryServers)
            {
                if (usage.ServerRTTs.ContainsKey(name))
                {
                    result.Add(usage.ServerRTTs[name]);
                }
            }
            if (s.Consistency != Consistency.Strong)
            {
                foreach (string name in config.SecondaryServers)
                {
                    if (usage.ServerRTTs.ContainsKey(name))
                    {
                        result.Add(usage.ServerRTTs[name]);
                    }
                }
            }
            return result;
        }

        private static List<ConfigurationAction> GetActionsForStrongConsistency(string ContainerName, ReplicaConfiguration config, SubSLA subSLA, ClientUsageData usage, string slaId)
        {
            List<ConfigurationAction> result=new List<ConfigurationAction>();

            int desiredLatency = subSLA.Latency;

            // we first compute the maximum probability of meeting the latency with primary servers. 
            float currentMaxProbability = usage.ServerRTTs
                .Where(sr => config.PrimaryServers.Contains(sr.Key))
                .Max(sr => sr.Value.ProbabilityOfFindingValueLessThanGiven(desiredLatency));

            // for any secondary replica with higher probability than the primary, we create an action to make that secondary the solo primary.
            // TODO: shouldn't this just pick one secondary with the max probability, i.e. why try to make multiple solo primaries?
            IEnumerable<string> chosenSecondaries = usage.ServerRTTs
                 .Where(sr => config.SecondaryServers.Contains(sr.Key)
                     && !config.ReadOnlySecondaryServers.Contains(sr.Key)
                     && sr.Value.ProbabilityOfFindingValueLessThanGiven(desiredLatency) > currentMaxProbability)
                 .Select(sr => sr.Key);
            
            foreach (string server in chosenSecondaries)
            {
                float utilityGain = (usage.ServerRTTs[server].ProbabilityOfFindingValueLessThanGiven(desiredLatency) - currentMaxProbability) * subSLA.Utility; /* * subSLA.NumberOfMisses*/
                result.Add(new MakeSoloPrimaryServer(ContainerName, server, utilityGain, slaId, ConfigurationActionSource.NonConstraint, numberOfReads, numberOfWrites));
            }

            // for any server with higher probabiliy than the primary, we create an action to make that server one of the primaries.
            IEnumerable<string> chosenServers = usage.ServerRTTs
                 .Where(sr => (config.SecondaryServers.Contains(sr.Key) || config.NonReplicaServers.Contains(sr.Key))
                     && !config.ReadOnlySecondaryServers.Contains(sr.Key)
                     && sr.Value.ProbabilityOfFindingValueLessThanGiven(desiredLatency) > currentMaxProbability)
                 .Select(sr => sr.Key);
            
            foreach (string server in chosenServers)
            {
                float utilityGain = (usage.ServerRTTs[server].ProbabilityOfFindingValueLessThanGiven(desiredLatency) - currentMaxProbability) * subSLA.Utility; /* * subSLA.NumberOfMisses*/
                result.Add(new AddPrimaryServer(ContainerName, server, utilityGain, slaId, ConfigurationActionSource.NonConstraint, numberOfReads, numberOfWrites));
            }

            return result;
        }

        private static List<ConfigurationAction> GetActionsForEventualConsistency(string ContainerName, ReplicaConfiguration config, SubSLA subSLA, ClientUsageData usage, string slaId)
        {
            List<ConfigurationAction> result = new List<ConfigurationAction>();

            int desiredLatency = subSLA.Latency;

            // we first compute the maximum probability of meeting the latency with current replicas. 
            float currentMaxProbability = usage.ServerRTTs
                .Where(sr => config.PrimaryServers.Contains(sr.Key) || config.SecondaryServers.Contains(sr.Key))
                .Max(sr => sr.Value.ProbabilityOfFindingValueLessThanGiven(desiredLatency));


            // for any non-replica with higher probabiliy than both the currentMaxProbability and MIN_ACCEPTABLE_PROB_FOR_EVENTUAL_CONS, we create a new action.
            IEnumerable<string> chosenServers = usage.ServerRTTs
                 .Where(sr => config.NonReplicaServers.Contains(sr.Key)
                     && sr.Value.ProbabilityOfFindingValueLessThanGiven(desiredLatency) > currentMaxProbability
                     && sr.Value.ProbabilityOfFindingValueLessThanGiven(desiredLatency) > ConstPool.MIN_ACCEPTABLE_PROB_FOR_CONS)
                 .Select(sr => sr.Key);
            
            foreach (string server in chosenServers)
            {
                float utilityGain = (usage.ServerRTTs[server].ProbabilityOfFindingValueLessThanGiven(desiredLatency) - currentMaxProbability) * subSLA.Utility; /* * subSLA.NumberOfMisses*/
                result.Add(new AddSecondaryServer(ContainerName, server, utilityGain, slaId, ConfigurationActionSource.NonConstraint, numberOfReads, numberOfWrites));
            }

            return result;
        }

        private static List<ConfigurationAction> GetActionsFor_RMW_MON_CAUSAL_BOUNDED_Consistency(string ContainerName, ReplicaConfiguration config, SubSLA subSLA, ClientUsageData usage, string slaId)
        {
            List<ConfigurationAction> result = new List<ConfigurationAction>();

            int desiredLatency = subSLA.Latency;

            float currentMaxProbability = 0;
            string nearestServer = null;

            // Find the closest replica, i.e. the one that has the maximum probability of meeting the desired latency
            foreach (string server in usage.ServerRTTs.Keys)
            {
                if (config.PrimaryServers.Contains(server) || config.SecondaryServers.Contains(server))
                {
                    float prob = usage.ServerRTTs[server].ProbabilityOfFindingValueLessThanGiven(desiredLatency);
                    if (prob > currentMaxProbability)
                    {
                        currentMaxProbability = prob;
                        nearestServer = server;
                    }
                }
            }

            // One action is to reduce the sync period for the nearest replica. 
            if (nearestServer != null && !config.PrimaryServers.Contains(nearestServer))
            {
                int oldInterval = config.GetSyncPeriod(nearestServer);

                if ((oldInterval * ConstPool.ADJUSTING_SYNC_INTERVAL_MULTIPLIER) > ConstPool.MINIMUM_ALLOWED_SYNC_INTERVAL)
                {
                    // TODO: This can't be right.  The utility gain for adjusting the sync period is hard to predict.
                    float utilityGain = currentMaxProbability * subSLA.Utility; /* * subSLA.NumberOfMisses*/
                    result.Add(new AdjustSyncPeriod(ContainerName, nearestServer, utilityGain, slaId, ConfigurationActionSource.NonConstraint, numberOfReads, numberOfWrites));
                }
            }

            // For any non-replica with higher probabiliy than both the currentMaxProbability and MIN_ACCEPTABLE_PROB_FOR_EVENTUAL_CONS, we create a new action.
            IEnumerable<string> chosenServers = usage.ServerRTTs
                 .Where(sr => config.NonReplicaServers.Contains(sr.Key)
                     && sr.Value.ProbabilityOfFindingValueLessThanGiven(desiredLatency) > currentMaxProbability
                     && sr.Value.ProbabilityOfFindingValueLessThanGiven(desiredLatency) > ConstPool.MIN_ACCEPTABLE_PROB_FOR_CONS)
                 .Select(sr => sr.Key);

            foreach (string server in chosenServers)
            {
                float utilityGain = (usage.ServerRTTs[server].ProbabilityOfFindingValueLessThanGiven(desiredLatency) - currentMaxProbability) * subSLA.Utility; /* * subSLA.NumberOfMisses*/
                result.Add(new AddSecondaryServer(ContainerName, server, utilityGain, slaId, ConfigurationActionSource.NonConstraint, numberOfReads, numberOfWrites));
            }

            return result;
        }
      
    }
}
