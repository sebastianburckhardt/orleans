
using System;
using System.Net;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using System.Collections.Generic;
using Orleans.Providers;
using Orleans.Runtime;
using Orleans.Core;

namespace Orleans.Replication
{
    /// <summary>
    /// Interface to be implemented for a replication provider.
    /// </summary>
    public interface IReplicationProvider : IProvider
    {
        /// <summary>TraceLogger used by this replication provider.</summary>
        Logger Log { get; }

        /// <summary>
        /// Construct a replication adaptor to be installed in the given host grain.
        /// </summary>
        IQueuedGrainAdaptor<T> MakeReplicationAdaptor<T>( 
            IReplicationAdaptorHost hostGrain, 
            T initialState, 
            string grainTypeName, 
            IReplicationProtocolServices services) where T : GrainState, new();
    }
}
