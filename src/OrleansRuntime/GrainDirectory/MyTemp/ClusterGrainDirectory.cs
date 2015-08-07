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
                //since we are the owner, we can look directly into the partition. No need to lookinto the cache.
                var localResult = router.DirectoryPartition.LookUpGrain(grain);

                if (localResult == null)
                {
                    //If no activation found in the cluster, return response as PASS.
                    response.ResponseStatus = ActivationResponseStatus.PASS;
                }
                else
                {
                    //Find the Activation Status for the entry and return appropriate value.

                    
                    var addresses = localResult.Item1;
                    
                    //addresses should contain only one item since there should be only one valid instance per cluster. Hence FirstOrDefault() should work fine.
                    var act = addresses.FirstOrDefault();
                    

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

        public async Task<List<Tuple<GrainId, RemoteClusterActivationResponse>>> ProcessRemoteDoubtfulActivations(List<GrainId> grains, string sendingClusterId)
        {
            List<Tuple<GrainId, RemoteClusterActivationResponse>> responses = new List<Tuple<GrainId, RemoteClusterActivationResponse>>();

            var collectDoubtfuls = grains
                .Select(async g =>
                {
                    var r = await ProcessActivationRequest(g, sendingClusterId, false);

                    responses.Add(Tuple.Create(g, r));
                });

            await Task.WhenAll(collectDoubtfuls);
            return responses.ToList();
        }

        public async Task InvalidateCache(GrainId gid)
        {
            
        }
    }
}
