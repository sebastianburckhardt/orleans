using Orleans.SystemTargetInterfaces;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime.GrainDirectory 
{
    
    /// <summary>
    /// A class that encapsulates response processing logic.
    /// It is a promise that fires once it has enough responses to make a determination.
    /// </summary>
    internal class GlobalSingleInstanceResponseTracker : TaskCompletionSource<GlobalSingleInstanceResponseTracker.Outcome> {

        public enum Outcome {
            SUCCEED,
            REMOTE_OWNER,
            REMOTE_OWNER_LIKELY,
            INCONCLUSIVE
        }

        private RemoteClusterActivationResponse[] responses;
        private GrainId grain;

        public ActivationAddress RemoteOwner;
        public int RemoteOwnerETag;
        public string RemoteOwnerCluster;

        public GlobalSingleInstanceResponseTracker(RemoteClusterActivationResponse[] responses, GrainId grain)
        {

            this.responses = responses;
            this.grain = grain;

            Notify();
        }

        /// <summary>
        /// Check responses and transition if warranted
        /// </summary>
        public void Notify()
        {
            if (!this.Task.IsCompleted)
            {
                if (responses.All(res => res != null && res.ResponseStatus == ActivationResponseStatus.PASS))
                {
                   // All passed, or no other clusters exist
                    TrySetResult(Outcome.SUCCEED);
                   return;
                }

                var ownerresponses = responses.Where(
                        res => (res != null && res.ResponseStatus == ActivationResponseStatus.FAILED && res.Owned == true)).ToList();

                if (ownerresponses.Count > 0)
                {
                    Debug.Assert(ownerresponses.Count == 1);

                    //TODO find a way to actually report errors
                    // if (ownerresponses.Count > 1)
                    //     logger.Warn((int) ErrorCode.GlobalSingleInstance_MultipleOwners, "Unexpected error occured. Multiple Owner Replies.");
                    
                    RemoteOwner = ownerresponses[0].ExistingActivationAddress;
                    RemoteOwnerETag = ownerresponses[0].eTag;
                    RemoteOwnerCluster = ownerresponses[0].ClusterId;
                    TrySetResult(Outcome.REMOTE_OWNER);
                }

                // are all responses here or have failed?
                if (responses.All(res => res != null))
                {
                    // determine best candidate
                    var candidates = responses
                        .Where(res => (res.ResponseStatus == ActivationResponseStatus.FAILED && res.ExistingActivationAddress != null))
                        .ToList();

                    foreach (var res in candidates)
                    {
                        if (RemoteOwner == null ||
                            MultiClusterUtils.ActivationPrecedenceFunc(grain,
                                res.ClusterId, RemoteOwnerCluster))
                        {
                            RemoteOwner = res.ExistingActivationAddress;
                            RemoteOwnerETag = res.eTag;
                            RemoteOwnerCluster = res.ClusterId;
                        }
                    }

                    if (RemoteOwner != null)
                        TrySetResult(Outcome.REMOTE_OWNER_LIKELY);
                    else
                        TrySetResult(Outcome.INCONCLUSIVE);
                }
            }
        }

    

    }
}
