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

    /// <summary>
    /// Reponse message used by Global Single Instance Protocol
    /// </summary>
    internal class RemoteClusterActivationResponse
    {
        public ActivationResponseStatus ResponseStatus { get; set; }
        public ActivationAddress ExistingActivationAddress { get; set; }
        public string ClusterId { get; set; }
        public bool Owned { get; set; }
        public int eTag { get; set; }
        public Exception ResponseException { get; set; }

        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append("[");
            sb.Append(ResponseStatus.ToString());
            if (ExistingActivationAddress != null) {
                sb.Append(" ");
                sb.Append(ExistingActivationAddress);
                sb.Append(" ");
                sb.Append(ClusterId);
            }
            if (Owned)
            {
                sb.Append(" owned");
            }
            if (ResponseException != null)
            {
                sb.Append(" ");
                sb.Append(ResponseException.GetType().Name);
            }
            sb.Append("]");
            return sb.ToString();
        }
    }

    interface IClusterGrainDirectory : ISystemTarget
    {
        Task<RemoteClusterActivationResponse> ProcessActivationRequest(
            GrainId grain,
            string requestClusterId,
            bool withForward = true);

        Task<RemoteClusterActivationResponse[]> ProcessActivationRequestBatch(
            GrainId[] grains,
            string sendingClusterId);

       // Task DeactivateLosers(ActivationId[] activations);
    }
}
