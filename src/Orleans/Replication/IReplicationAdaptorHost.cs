using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans.Concurrency;
using Orleans.Runtime;

namespace Orleans.Replication
{
    /// <summary>
    ///  This interface is for grain classes that use a replication provider.
    /// </summary>
    public interface IReplicationAdaptorHost 
    {
        // called on a parameterized instance which can then pass the 
        // type into the replication provider as  simple type parameter
       void InstallAdaptor(IReplicationProvider provider, 
            object state, string graintypename, IReplicationProtocolServices services);


        // identity of the host, for logging purposes
        string IdentityString { get; }

    }
}
