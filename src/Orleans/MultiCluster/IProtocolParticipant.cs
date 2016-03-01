using System.Threading.Tasks;
using Orleans.Concurrency;

namespace Orleans.MultiCluster
{
    /// <summary>
    /// Grain interface for grains that participate in multi-cluster-protocols.
    /// </summary>
    public interface IProtocolParticipant  : IGrain  
    {
        /// <summary>
        /// Called when a message is received from another replica.
        /// This MUST interleave with other calls to avoid deadlocks.
        /// </summary>
        /// <param name="payload"></param>
        /// <returns></returns>
        [AlwaysInterleave]
        Task<IProtocolMessage> OnProtocolMessageReceived(IProtocolMessage payload);

        /// <summary>
        /// Called when a configuration change notification is received.
        /// </summary>
        /// <param name="prev"></param>
        /// <param name="current"></param>
        /// <returns></returns>
        [AlwaysInterleave]
        Task OnMultiClusterConfigurationChange(MultiClusterConfiguration next);
    }

    /// <summary>
    /// interface to mark classes that represent protocol messages
    /// </summary>
    public interface IProtocolMessage
    {
    }
}
