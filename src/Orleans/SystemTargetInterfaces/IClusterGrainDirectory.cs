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
            int retries);

        Task<Dictionary<ActivationId, GrainId>> GetDoubtfulActivations();

        Task<Tuple<Dictionary<ActivationId, GrainId>, bool>> ProcessRemoteDoubtfulActivations(
            Dictionary<ActivationId, GrainId> addrList,
            int sendingClusterId);

        Task<Dictionary<ActivationId, GrainId>> FindLoserDoubtfulActivations(
            Dictionary<ActivationId, GrainId> remoteDoubtful,
            int remoteClusterId);

        Task ProcessAntiEntropyResults(
            Dictionary<ActivationId, GrainId> losers,
            Dictionary<ActivationId, GrainId> winners);
    }
}
