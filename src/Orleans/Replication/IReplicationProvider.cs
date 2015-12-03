
using System;
using System.Net;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using System.Collections.Generic;
using Orleans.Providers;
using Orleans.Runtime;

namespace Orleans.Replication
{
    /// <summary>
    /// Interface to be implemented for a replication provider.
    /// </summary>
    public interface IReplicationProvider : IProvider
    {
        /// <summary>TraceLogger used by this replication provider.</summary>
        /// <returns>Reference to the TraceLogger object used by this provider.</returns>
        /// <seealso cref="Logger"/>
        Logger Log { get; }

        /// <summary>
        /// Construct a replication adaptor to be installed in the given host grain.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="hostgrain">the grain that should host the replication adapter</param>
        /// <param name="initialstate">an initial state object</param>
        /// <param name="graintypename">type name of the grain</param>
        /// <param name="services">replication services</param>
        /// <returns></returns>
        IQueuedGrainAdaptor<T> MakeReplicationAdaptor<T>(
            QueuedGrain<T> hostgrain, 
            T initialstate, 
            string graintypename, 
            IReplicationProtocolServices services) where T : GrainState, new();

        /// <summary>
        /// Give this provider a chance to set up any storage providers it depends on.
        /// </summary>
        void SetupDependedOnStorageProviders(Func<string, Orleans.Storage.IStorageProvider> providermanagerlookup);


    }

   
}
