using Orleans.Providers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UnitTests.Grains
{

    // use the explictly specified "SharedStorage" replication provider
    [LogViewProvider(ProviderName = "SharedStorage")]
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
    [LogViewProvider(ProviderName = "LocalMemory")]
    public class SimpleQueuedGrainLocalMemoryStorage : SimpleQueuedGrain
    {
    }

}
