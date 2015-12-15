using Orleans.MultiCluster;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Orleans.Runtime
{
    internal interface IMultiClusterGossipService : ISystemTarget
    {
        /// <summary>
        /// One-way small-scale gossip: send partial data to recipient
        /// </summary>
        /// <param name="data">The gossip data</param>
        /// <param name="forwardlocally">Whether to forward the changes to local silos</param>
        /// <returns></returns>
        Task Push(IMultiClusterGossipData GossipData, bool forwardlocally);

        /// <summary>
        /// Two-way bulk gossip: send all known data to recipient, and receive all unknown data
        /// </summary>
        /// <param name="data">The pushed gossip data</param>
        /// <returns>The returned gossip data</returns>
        Task<IMultiClusterGossipData> PushAndPull(IMultiClusterGossipData data);

        /// <summary>
        /// Find silos whose configuration does not match the expected configuration.
        /// </summary>
        /// <param name="expected">the configuration to compare with</param>
        /// <param name="forwardlocally">whether to recursively include all silos in the same cluster</param>
        /// <returns></returns>
        Task<Dictionary<SiloAddress,MultiClusterConfiguration>> FindUnstableSilos(MultiClusterConfiguration expected, bool forwardlocally);
    }


    // placeholder interface for gossip data. Actual implementation is in Orleans.Runtime.
    internal interface IMultiClusterGossipData { }
}
