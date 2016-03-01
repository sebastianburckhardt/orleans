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
        /// Called when a message is received from another cluster.
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


        /// <summary>
        /// Called immediately before the user-level OnActivateAsync, on same scheduler
        /// </summary>
        /// <returns></returns>
        Task ActivateProtocolParticipant();

        /// <summary>
        /// Called immediately after the user-level OnDeactivateAsync, on same scheduler
        /// </summary>
        /// <returns></returns>
        Task DeactivateProtocolParticipant();
    }

    /// <summary>
    /// interface to mark classes that represent protocol messages
    /// </summary>
    public interface IProtocolMessage
    {
    }
}
