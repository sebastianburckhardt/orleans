
using System.Collections.Generic;
using System.Threading.Tasks;


namespace Orleans.Runtime.GossipNetwork
{
    // Interface for local, per-silo authorative source of information about status of other silos.
    // A local interface for local communication between in-silo runtime components and this ISiloStatusOracle.
    internal interface IGossipOracle
    {
      
        Task Start(ISiloStatusOracle silostatusoracle);

        /// <summary>
        /// Get the latest multicluster configuration.
        /// </summary>
        /// <returns>The current multicluster configuration, or null if there is none</returns>
        MultiClusterConfiguration GetMultiClusterConfiguration();

        /// <summary>
        /// Whether a gateway is functional (to the best knowledge of this node) 
        /// </summary>
        /// <param name="siloAddress">A gateway whose status we are interested in.</param>
        bool IsFunctionalClusterGateway(SiloAddress siloAddress);


        /// <summary>
        /// Returns a list of cluster ids for active clusters based on what gateways we have stored in the table.
        /// </summary>
        /// <returns></returns>
        IEnumerable<string> GetActiveClusters();

        /// <summary>
        /// Returns one of the active cluster gateways for a given cluster.
        /// </summary>
        /// <param name="cluster">the cluster for which we want a gateway</param>
        /// <returns>a gateway address, or null if none is found for the given cluster</returns>
        SiloAddress GetRandomClusterGateway(string cluster);

        /// <summary>
        /// Subscribe to gossip data change events.
        /// </summary>
        /// <param name="observer">An observer async interface to receive configuration change notifications.</param>
        /// <returns>bool value indicating that subscription succeeded or not.</returns>
        bool SubscribeToGossipEvents(IGossipListener observer);

        /// <summary>
        /// UnSubscribe from gossip data change events.
        /// </summary>
        /// <returns>bool value indicating that subscription succeeded or not.</returns>
        bool UnSubscribeFromGossipEvents(IGossipListener observer);


    }
}
