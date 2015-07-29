using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans.GrainDirectory;
using Orleans.SystemTargetInterfaces;

namespace Orleans.Runtime.GrainDirectory.MyTemp
{
    internal class GlobalSingleInstanceRegistrar : SingleInstanceRegistrar
    {
        private static int NUM_RETRIES = 3;
        private static TimeSpan RetryDelay = TimeSpan.FromSeconds(5);

        public GlobalSingleInstanceRegistrar(GrainDirectoryPartition partition) : base(partition)
        {
        }

        public override async Task<Tuple<ActivationAddress, int>> RegisterAsync(ActivationAddress address)
        {
            //Algorithm:
            // 1. Check if optimistic of pessimistic version is to be used.
            // (NOTE: FOR NOW ONLY IMPLEMENTING OPTIMISTIC VERSION)
            
            // If Optimistic: (assume it is, will see later how to deal with it)
            
            // 2. Register the activation locally. (i.e. call base.RegisterSingleActivationAsync function).

            //assume you are the owner and perform local operation.
            var myClusterRegisterdAddress = DirectoryPartition.AddSingleActivation(address.Grain, address.Activation, address.Silo, ActivationStatus.REQUESTED_OWNERSHIP);

            if (!myClusterRegisterdAddress.Item1.Equals(address)) //This implies that the registration already existed in some state? return the existing activation.
            {
                return myClusterRegisterdAddress;
            }

            int retries = NUM_RETRIES;
            while (retries-- > 0)
            {
                // 3. Send "RequestOwnership" message to all other clusters and wait for their reply.
                var responses = await SendActivationRequests(address.Grain);

                // 3b. before processing responses, check if there was a RACE which led to the cluster being the RACE_LOOSER.
                var currentActivations = DirectoryPartition.LookUpGrain(address.Grain).Item1;

                var localAct = currentActivations.FirstOrDefault();

                Debug.Assert(localAct != null);

                //this might return R_OWNERSHIP and later transition to RACE_LOSER? todo:
                if (localAct.Status == ActivationStatus.RACE_LOSER)
                {
                    await UpdateSingleInstanceActivationStatus(address.Grain, localAct.Activation, ActivationStatus.REQUESTED_OWNERSHIP);
                    await Task.Delay(RetryDelay);
                    continue;
                }

                if (localAct.Status != ActivationStatus.REQUESTED_OWNERSHIP)
                {
                    //THIS IS A BUG IF IT HAPPENS.
                    throw new OrleansException(String.Format("Unexpected error occured. Found status {0} expected {1}. Possible Race Condition.", localAct.Status, ActivationStatus.REQUESTED_OWNERSHIP));
                }

                //4. for each response check the status.
                if (responses.All(res => res.ResponseStatus == ActivationResponseStatus.PASS))
                {
                    //4a. If all passed, change the ownership to Owned.

                    //IN A RACE CONDITION, WE MIGHT HAVE TRANSITIONED TO RACE_LOOSER. TODO: COMPARE AND SWAP?
              
                    await UpdateSingleInstanceActivationStatus(myClusterRegisterdAddress.Item1.Grain, myClusterRegisterdAddress.Item1.Activation, ActivationStatus.OWNED);

                    return myClusterRegisterdAddress;
                }
                else if (responses.Any(r => r.ResponseStatus == ActivationResponseStatus.FAILED))
                {


                    //We received some failed responses. 
                    //NOTE: FAULTED/TIMEOUTS are not considered as FAILED. These are handled separately.

                    //Now there may be two conditions.
                    //1. One or more of the failed responses are owners/doubtful.
                    //2. We do not receive any responses from owner/doubtful clusters. All return A = null in their response.

                    var ownerOrDoubtfulClusters =
                        responses.Where(
                            res =>
                                (res.ResponseStatus == ActivationResponseStatus.FAILED &&
                                    res.ExistingActivationAddress != null)).ToList();

                    if (ownerOrDoubtfulClusters.Any())
                    {
                        //Atleast one owner or doubtful cluster. 
                        //Use the Precedence function to decide who the owner should be.

                        RemoteClusterActivationResponse best = null;
                        foreach (var res in ownerOrDoubtfulClusters)
                        {
                            if (best == null ||
                                MultiClusterUtils.ActivationPrecedenceFunc(address.Grain,
                                    res.ExistingActivationAddress.Silo.ClusterId, best.ExistingActivationAddress.Silo.ClusterId))
                            {
                                best = res;
                            }
                        }

                        if (best != null)
                        {
                            //4b. "cache" the entry if some other cluster is a owner. Here cache means, add the entery to the GrainDirectory of this cluster with activation address of the other cluster and status as "CACHE".
                            await
                                CacheOrUpdateSingleRegistration(address.Grain, address.Activation, best.ExistingActivationAddress);
                            return Tuple.Create(best.ExistingActivationAddress, best.eTag);
                        }                        
                    }

                    //If we reach here, implies that we recived few <failed,null> responses but non <failed, A> response. Hence wait for some time and retry the protocol.

                    //todo: SHOULD WE TRANSITION TO REQUESTED_OWNERSHIP? WHAT IF WE TRANSITIONED TO RACE_LOOSER IN THE MEANWHILE?
                    await UpdateSingleInstanceActivationStatus(address.Grain, localAct.Activation, ActivationStatus.REQUESTED_OWNERSHIP);
                    await Task.Delay(RetryDelay);
                    continue;
                }
                else if (responses.Any(r => r.ResponseStatus == ActivationResponseStatus.FAULTED))
                {
                    //if control reaches here, it implies that we have a combination of Pass and Faulted responses. (i.e. there might be a partition, or come cluster is unreachable/down).
                    //So set the status to doubtful.
                    await UpdateSingleInstanceActivationStatus(myClusterRegisterdAddress.Item1.Grain, myClusterRegisterdAddress.Item1.Activation, ActivationStatus.DOUBTFUL);
                    return myClusterRegisterdAddress;
                }
            }

            //directory is not stable. Optimisically, set the status to doubtful and continue operations.
            await UpdateSingleInstanceActivationStatus(myClusterRegisterdAddress.Item1.Grain, myClusterRegisterdAddress.Item1.Activation, ActivationStatus.DOUBTFUL);
            return myClusterRegisterdAddress;
        }

        /// <summary>
        /// Sends activation requests to all the other clusters (through the cluster gateways) in parallel and waits for all the responses.
        /// </summary>
        /// <param name="grain"></param>
        /// <param name="activationStrategy"></param>
        /// <returns></returns>
        private async Task<List<RemoteClusterActivationResponse>>  SendActivationRequests(GrainId grain)       
        {
            //Get the list of cluster and cluster gateways from the "ClusterMembershipOracle".

            var clusterMembershipOracle = Silo.CurrentSilo.LocalClusterMembershipOracle;

            var activeClusters = clusterMembershipOracle.GetActiveClusters();

            List<Task<RemoteClusterActivationResponse>> activationResonseTasks = new List<Task<RemoteClusterActivationResponse>>();

            //send the request to each of the cluster's gateways and wait for response. 
            foreach (var clusterId in activeClusters)
            {
                //Do not send request to the self cluster.
                if (clusterId.Equals(Silo.CurrentSilo.SiloAddress.ClusterId))
                    continue;

                var addr = clusterMembershipOracle.GetRandomClusterGateway(clusterId);

                //BUG: Wrong generation of silo address is returned.
                //Temp fix: make generation 0
                var clusterGatewayAddress = SiloAddress.New(addr.Endpoint, 0, clusterId);

                //get the reference for the system target on the gateway. The gateway will be responsible for forwarding the request to appropriate silo if it is not the owner.
                //var clusterGrainDir = GetClusterDirectoryReference(clusterGatewayAddress);
                var clusterGrainDir = InsideRuntimeClient.Current.InternalGrainFactory.GetSystemTarget<IClusterGrainDirectory>(Constants.ClusterDirectoryServiceId, clusterGatewayAddress);

                activationResonseTasks.Add(clusterGrainDir.ProcessActivationRequest(grain, Silo.CurrentSilo.SiloAddress.ClusterId, NUM_RETRIES));
            }
            try
            {
                await Task.WhenAll(activationResonseTasks);
            }
            catch (Exception ex)
            {
                //nothing to do. will be handled below. if one of the tasks fail.
            }


            List<RemoteClusterActivationResponse> activationResonses = new List<RemoteClusterActivationResponse>();

            foreach (var t in activationResonseTasks)
            {
                RemoteClusterActivationResponse response;
                try
                {
                    response = t.Result;
                }
                catch (Exception ex)
                {
                    response = new RemoteClusterActivationResponse
                    {
                        ResponseStatus = ActivationResponseStatus.FAULTED,
                        ResponseException = ex
                    };
                }
                activationResonses.Add(response);
            }

            return activationResonses;
        }


        private async Task CacheOrUpdateSingleRegistration(GrainId grain, ActivationId oldActivation, ActivationAddress otherClusterAddress)
        {
            // Here, I am the owner, store the new activation locally
            //var success = DirectoryPartition.UpdateActivationStatus(grain, activation, otherClusterAddress);
            DirectoryPartition.CacheOrUpdateRemoteClusterRegistration(grain, oldActivation, otherClusterAddress);            
        }

        private async Task UpdateSingleInstanceActivationStatus(GrainId grain, ActivationId activationId, ActivationStatus activationStatus)
        {            
            // if I am the owner, store the new activation locally
            var success = DirectoryPartition.UpdateActivationStatus(grain, activationId, activationStatus);
            if (!success)
            {
                //log.
            }            
        }
    }
}
