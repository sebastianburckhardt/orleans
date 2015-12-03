using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans.Concurrency;
using Orleans.Runtime;

namespace Orleans.Replication
{
    // we use this non-parameterized interface as a trick to to get the type parameter to the provider
    public interface IQueuedGrainAdaptorHost 
    {
        void InstallAdaptor(IReplicationProvider provider, 
            object state, string graintypename, IReplicationProtocolServices services);
    }
}
