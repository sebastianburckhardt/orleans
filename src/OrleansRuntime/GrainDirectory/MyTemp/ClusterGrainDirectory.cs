using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans.GrainDirectory;
using Orleans.SystemTargetInterfaces;

namespace Orleans.Runtime.GrainDirectory.MyTemp
{
    internal class ClusterGrainDirectory : SystemTarget, IClusterGrainDirectory
    {

        private readonly LocalGrainDirectory router;

        public ClusterGrainDirectory(LocalGrainDirectory r, GrainId grainId) : base(grainId, r.MyAddress)
        {
            router = r;
        }

        public ClusterGrainDirectory(LocalGrainDirectory r, GrainId grainId, bool lowPriority)
            : base(grainId, r.MyAddress, lowPriority)
        {
            router = r;
        }

        public async Task<RemoteClusterActivationResponse> ProcessActivationRequest(GrainId grain, string requestClusterId, bool withRetry = true)
        {

            RemoteClusterActivationResponse response;

            //check if the requesting cluster id is in the current configuration view of this cluster, if not, reject the message.
            var gossipOracle = Orleans.Runtime.Silo.CurrentSilo.LocalClusterMembershipOracle;
            if (gossipOracle == null || !gossipOracle.GetActiveClusters().Any(t => t.Equals(requestClusterId)))            
            {
                response = new RemoteClusterActivationResponse();
                response.ResponseStatus = ActivationResponseStatus.FAILED;
                response.ExistingActivationAddress = null;
                return response;
            }


            response = await router.PerformLocalOrRemoteWithRetry(grain, grain,
                async (gid) => await ProcessRequestLocal(gid, requestClusterId),
                async (gid, owner) =>
                {
                    var clusterGrainDir = InsideRuntimeClient.Current.InternalGrainFactory.GetSystemTarget<IClusterGrainDirectory>(Constants.ClusterDirectoryServiceId, owner);
                    return await clusterGrainDir.ProcessActivationRequest(gid, requestClusterId, false);
                }, withRetry);

            return response;
        }

        private async Task<RemoteClusterActivationResponse> ProcessRequestLocal(GrainId grain, string requestClusterId)
        {
            RemoteClusterActivationResponse response = new RemoteClusterActivationResponse();
            
            //This function will be called only on the Owner silo.
    
            //Optimize? Look in the cache first?
            //NOTE: THIS COMMENT IS FROM LOOKUP. HAS IMPLICATIONS ON "OWNED" INVARIANCE.
            //// It can happen that we cannot find the grain in our partition if there were 
            // some recent changes in the membership. Return empty list in such case (and not null) to avoid
            // NullReference exceptions in the code of invokers
            try
            {
                //var activations = await LookUp(grain, LocalGrainDirectory.NUM_RETRIES);
                List<ActivationAddress> addresses;
                bool foundlocally = router.LocalLookup(grain, out addresses) && addresses != null && addresses.Count > 0;

                if (!foundlocally)
                {
                    //If no activation found in the cluster, return response as PASS.
                    response.ResponseStatus = ActivationResponseStatus.PASS;
                }
                else
                {
                    //Find the Activation Status for the entry and return appropriate value.
                    //For now returning the address as is.

                    var act = addresses.FirstOrDefault();
                        //addresses should contain only one item since there should be only one valid instance per cluster. Hence FirstOrDefault() should work fine.

                    if (act == null)
                    {
                        response.ResponseStatus = ActivationResponseStatus.PASS;
                    }
                    else
                    {
                        var existingActivationStatus = act.Status;

                        switch (existingActivationStatus)
                        {
                            case ActivationStatus.OWNED:
                            case ActivationStatus.DOUBTFUL:
                                response.ResponseStatus = ActivationResponseStatus.FAILED;
                                response.ExistingActivationAddress = act;
                                break;
                            case ActivationStatus.CACHED:
                            case ActivationStatus.RACE_LOSER:
                                response.ResponseStatus = ActivationResponseStatus.PASS;
                                response.ExistingActivationAddress = null;
                                break;
                            case ActivationStatus.REQUESTED_OWNERSHIP:
                                var iWin = MultiClusterUtils.ActivationPrecedenceFunc(grain, router.MyAddress.ClusterId,
                                    requestClusterId);
                                if (iWin)
                                {
                                    response.ResponseStatus = ActivationResponseStatus.FAILED;
                                    response.ExistingActivationAddress = act;
                                }
                                else
                                {
                                    response.ResponseStatus = ActivationResponseStatus.PASS;
                                    response.ExistingActivationAddress = null;
                                    //update own activation status to race looser.
                                    router.DirectoryPartition.UpdateActivationStatus(grain, act.Activation, act.Status);                                    
                                }
                                break;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                //LOG exception
                response.ResponseStatus = ActivationResponseStatus.FAULTED;
                response.ResponseException = ex;
            }
            return response;
        }

        public Task<Dictionary<ActivationId, GrainId>> GetDoubtfulActivations()
        {
            throw new NotImplementedException();
        }

        public Task<Tuple<Dictionary<ActivationId, GrainId>, bool>> ProcessRemoteDoubtfulActivations(Dictionary<ActivationId, GrainId> addrList, int sendingClusterId)
        {
            throw new NotImplementedException();
        }

        public Task<Dictionary<ActivationId, GrainId>> FindLoserDoubtfulActivations(Dictionary<ActivationId, GrainId> remoteDoubtful, int remoteClusterId)
        {
            throw new NotImplementedException();
        }

        public Task ProcessAntiEntropyResults(Dictionary<ActivationId, GrainId> losers, Dictionary<ActivationId, GrainId> winners)
        {
            throw new NotImplementedException();
        }
    }
}
