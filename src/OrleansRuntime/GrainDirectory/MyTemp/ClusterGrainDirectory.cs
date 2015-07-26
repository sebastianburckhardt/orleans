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

        public ClusterGrainDirectory(GrainId grainId, LocalGrainDirectory r) : base(grainId, r.MyAddress)
        {
            router = r;
        }

        public ClusterGrainDirectory(GrainId grainId, LocalGrainDirectory r, bool lowPriority)
            : base(grainId, r.MyAddress, lowPriority)
        {
            router = r;
        }

        public async Task<RemoteClusterActivationResponse> ProcessActivationRequest(GrainId grain, string requestClusterId, int retries)
        {
            var response = new RemoteClusterActivationResponse();
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
                    var result = await router.FullLookUp(grain, withRetry: true);
                    addresses = result.Item1;
                }

                if (addresses == null || !addresses.Any())
                {
                    //If no activation found in the cluster, return response as PASS.
                    response.ResponseStatus = ActivationResponseStatus.PASS;
                }
                else
                {
                    //Find the Activation Status for the entry and return appropriate value.
                    //For now returning the address as is.

                    var act = addresses.FirstOrDefault(); //addresses should contain only one item since there should be only one valid instance per cluster. Hence FirstOrDefault() should work fine.

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
                                var iWin = MultiClusterUtils.ActivationPrecedenceFunc(grain, router.MyAddress.ClusterId, requestClusterId);
                                if (iWin)
                                {
                                    response.ResponseStatus = ActivationResponseStatus.FAILED;
                                    response.ExistingActivationAddress = act;
                                }
                                else
                                {
                                    response.ResponseStatus = ActivationResponseStatus.PASS;
                                    response.ExistingActivationAddress = null;
                                    //await UpdateActivationStatus(grain, act.Item2, ActivationStatus.RACE_LOSER, LocalGrainDirectory.NUM_RETRIES);
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
