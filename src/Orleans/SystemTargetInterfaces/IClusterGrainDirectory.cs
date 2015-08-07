using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans.GrainDirectory;
using Orleans.Runtime;

namespace Orleans.SystemTargetInterfaces
{

    internal enum ActivationResponseStatus
    {
        PASS,
        FAILED,
        FAULTED
    }


    internal class RemoteClusterActivationResponse
    {
        public ActivationResponseStatus ResponseStatus { get; set; }
        public ActivationAddress ExistingActivationAddress { get; set; }
        public int eTag { get; set; }
        public Exception ResponseException { get; set; }
    }

    interface IClusterGrainDirectory : ISystemTarget
    {
        Task<RemoteClusterActivationResponse> ProcessActivationRequest(
            GrainId grain,
            string requestClusterId,
            bool withRetry = true);

        Task<List<Tuple<GrainId, RemoteClusterActivationResponse>>> ProcessRemoteDoubtfulActivations(
            List<GrainId> grains,
            string sendingClusterId);

        Task InvalidateCache(GrainId gid);
    }
}
