using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Orleans.GrainDirectory;
using Orleans.SystemTargetInterfaces;

namespace Orleans.Runtime.GrainDirectory.MyTemp
{
    internal class GlobalSingleInstanceActivationMaintainer : AsynchAgent
    {

        private static readonly TimeSpan SLEEP_TIME_BETWEEN_REFRESHES = Debugger.IsAttached ? TimeSpan.FromMinutes(5) : TimeSpan.FromMinutes(1); // this should be something like minTTL/4
        
        private LocalGrainDirectory router;


        internal GlobalSingleInstanceActivationMaintainer(LocalGrainDirectory router)
        {
            this.router = router;
        }

        protected async override void Run()
        {
            while (router.Running)
            {
                //Run through all the doubtful entries and do the following:
                //1. Process activations with status "CACHED" and "REQUESTED_OWNERSHIP"
                //2. FOR REQUESTED_OWNERSHIP STATUS:
                    //2a. If the activation exists as "Owned" in other cluster, remove it from the current one and cache the owned activation.
                    //2b. If the activation exists as "REQUESTED_OWNERSHIP" in other cluster, use the precedence function to decide the actual owner and cache entry if required.
                //3. For "CACHED" status:
                    //3a. If the entry is removed from the cached cluster, remove the cached entry. On next grain call, the protocol should handle the scenario.

                var allEntries = router.DirectoryPartition.GetItems().Where(kp =>
                {
                    if (!kp.Value.SingleInstance) return false;
                    var act = kp.Value.Instances.Keys.FirstOrDefault();
                    if (act == null) return false;
                    return true;
                }).Select(kp => Tuple.Create(kp.Key, kp.Value.Instances.FirstOrDefault().Value));

                //Step 2.
                var reqOwnershipEntries =
                    allEntries.Where(t => t.Item2.ActivationStatus == ActivationStatus.REQUESTED_OWNERSHIP);

                var results = await RunAntiEntropy(reqOwnershipEntries.ToList());
                
                if (results != null)
                {
                    foreach (var kvp in results)
                    {
                        var ownedbyOther = kvp.Value.FirstOrDefault(r => r.ResponseStatus == ActivationResponseStatus.FAILED && r.ExistingActivationAddress != null);

                        if (ownedbyOther != null)
                        {
                            var currentActivation =
                                router.DirectoryPartition.LookUpGrain(kvp.Key).Item1.FirstOrDefault();
                            //this will be non null.

                            router.DirectoryPartition.CacheOrUpdateRemoteClusterRegistration(
                                kvp.Key, currentActivation != null ? currentActivation.Activation : null,
                                ownedbyOther.ExistingActivationAddress);
                        }
                        else
                        {
                            var ownerOrDoubtfulClusters =
                                kvp.Value.Where(
                                    r =>
                                        r.ResponseStatus == ActivationResponseStatus.FAILED &&
                                        r.ExistingActivationAddress != null).ToList();

                            if (ownerOrDoubtfulClusters.Any())
                            {
                                //Atleast one owner or doubtful cluster. 
                                //Use the Precedence function to decide who the owner should be.

                                RemoteClusterActivationResponse best = null;
                                foreach (var res in ownerOrDoubtfulClusters)
                                {
                                    if (best == null ||
                                        MultiClusterUtils.ActivationPrecedenceFunc(kvp.Key,
                                            res.ExistingActivationAddress.Silo.ClusterId,
                                            best.ExistingActivationAddress.Silo.ClusterId))
                                    {
                                        best = res;
                                    }
                                }

                                if (best != null)
                                {
                                    //"cache" the entry if some other cluster is a owner. Here cache means, add the entery to the GrainDirectory of this cluster with activation address of the other cluster and status as "CACHE".
                                    var currentActivation =
                                router.DirectoryPartition.LookUpGrain(kvp.Key).Item1.FirstOrDefault();

                                    if (best.ExistingActivationAddress.Silo.ClusterId.Equals(router.MyAddress.ClusterId))
                                    {
                                        //if i am the best, change the entry to OWNED.
                                        router.DirectoryPartition.UpdateActivationStatus(kvp.Key,
                                            currentActivation.Activation, ActivationStatus.OWNED);
                                    }
                                    else
                                    {
                                        //update to the CACHE state.                                        
                                        router.DirectoryPartition.CacheOrUpdateRemoteClusterRegistration(kvp.Key, currentActivation != null ? currentActivation.Activation : null, best.ExistingActivationAddress);
                                    }                                        
                                }
                            }
                        }
                        //find best and change the entry as appropriate.
                    }                            
                }

                //STEP 3.. (TODO: Merge step 2 and 3)?
                var cahcedEntries =
                    allEntries.Where(t => t.Item2.ActivationStatus == ActivationStatus.CACHED);

                results = await RunAntiEntropy(cahcedEntries.ToList());

                if (results != null)
                {
                    foreach (var kvp in results)
                    {
                        var ownedbyOther = kvp.Value.FirstOrDefault(r => r.ResponseStatus == ActivationResponseStatus.FAILED && r.ExistingActivationAddress != null);

                        var currentActivation =
                            router.DirectoryPartition.LookUpGrain(kvp.Key).Item1.FirstOrDefault(); //this will be non null.

                        if (ownedbyOther == null)
                        {
                            //remove the cahced entry.
                            router.DirectoryPartition.RemoveActivation(kvp.Key, currentActivation.Activation, true);
                        }
                        else
                        {
                            //update the cached entry to the new OWNED cluster.
                            router.DirectoryPartition.CacheOrUpdateRemoteClusterRegistration(
                                kvp.Key, currentActivation != null ? currentActivation.Activation : null,
                                ownedbyOther.ExistingActivationAddress);
                        }
                    }
                }


                // recheck every X seconds (Consider making it a configurable parameter)
                Thread.Sleep(SLEEP_TIME_BETWEEN_REFRESHES);
            }
        }

        private async Task<Dictionary<GrainId, List<RemoteClusterActivationResponse>>> RunAntiEntropy(
            List<Tuple<GrainId, IActivationInfo>> reqOwnershipEntries)
        {
            SiloAddress myAddress = router.MyAddress;
            string myClusterId = myAddress.ClusterId;

            var clusterOracle = Silo.CurrentSilo.LocalClusterMembershipOracle;
            if (clusterOracle == null) return null;

            // Forward the list of doubtful (requested ownership) activations to all other clusters.
            var responseTasks = new List<Task<List<Tuple<GrainId, RemoteClusterActivationResponse>>>>();

            foreach (var cluster in clusterOracle.GetActiveClusters())
            {
                if (cluster.Equals(myClusterId)) continue;
                
                var cgwAddr = clusterOracle.GetRandomClusterGateway(cluster);
                var clusterGrainDir = InsideRuntimeClient.Current.InternalGrainFactory.GetSystemTarget<IClusterGrainDirectory>(Constants.ClusterDirectoryServiceId, cgwAddr);

                responseTasks.Add(clusterGrainDir.ProcessRemoteDoubtfulActivations(reqOwnershipEntries.Select(t => t.Item1).ToList(), myClusterId));
            }
            
            try
            {
                await Task.WhenAll(responseTasks);
            }
            catch (Exception)
            {
                //ignore handled below.
            }

            //List<Tuple<GrainId, List<>>> results = new List<Tuple<GrainId, List<RemoteClusterActivationResponse>>>();
            var results = new Dictionary<GrainId, List<RemoteClusterActivationResponse>>();
            foreach (var response in responseTasks)
            {
                List<Tuple<GrainId, RemoteClusterActivationResponse>> retVal;
                try
                {
                    // This call may throw an exception.
                    retVal = response.Result;                    
                }
                catch (Exception)
                {
                    retVal = new List<Tuple<GrainId, RemoteClusterActivationResponse>>();
                }

                retVal.ForEach(t =>
                {
                    if (!results.ContainsKey(t.Item1))
                        results.Add(t.Item1, new List<RemoteClusterActivationResponse>());
                    results[t.Item1].Add(t.Item2);
                });
            }
            return results;
        }
    }
}