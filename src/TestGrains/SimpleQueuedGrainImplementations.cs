using Orleans.Providers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UnitTests.Grains
{

    // use the explictly specified "SharedStorage" replication provider
    [ReplicationProvider(ProviderName = "SharedStorage")]
    public class SimpleQueuedGrainSharedStorage : SimpleQueuedGrain
    {
    }

    // use the default storage provider as the shared storage
    public class SimpleQueuedGrainDefaultStorage : SimpleQueuedGrain
    {
    }

    // use an explicitly specified storage provider
    [StorageProvider(ProviderName = "MemoryStore")]
    public class SimpleQueuedGrainMemoryStorage : SimpleQueuedGrain
    {
    }

    // use an explicitly specified replication provider
    [ReplicationProvider(ProviderName = "Dummy")]
    public class SimpleQueuedGrainDummyStorage : SimpleQueuedGrain
    {
    }
 
}
