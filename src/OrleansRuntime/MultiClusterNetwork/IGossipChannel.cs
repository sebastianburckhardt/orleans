using System.Threading.Tasks;
using Orleans.Runtime.Configuration;


namespace Orleans.Runtime.MultiClusterNetwork
{
    /// <summary>
    /// Interface for a multi cluster channel, providing gossip-style communication
    /// </summary>
    public interface IGossipChannel
    {
        /// <summary>
        /// Initialize the channel with given configuration.
        /// </summary>
        /// <param name="config"></param>
        /// <returns></returns>
        Task Initialize(GlobalConfiguration globalconfig, string connectionstring);

        /// <summary>
        /// A name for the channel.
        /// </summary>
        string Name { get; }

        /// <summary>
        /// One-way small-scale gossip: send partial data to recipient
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        Task Push(MultiClusterData data);

         /// <summary>
        /// Two-way bulk gossip: send all known data to recipient, and receive all unknown data
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        Task<MultiClusterData> PushAndPull(MultiClusterData data);

    }
 

}
