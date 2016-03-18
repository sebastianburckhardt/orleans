using Orleans;
using Orleans.Providers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using UnitTests.GrainInterfaces;

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

    // use the explictly specified "CustomStorage" replication provider
    [LogViewProvider(ProviderName = "CustomStorage")]
    public class SimpleQueuedGrainCustomStorage : SimpleQueuedGrain,
        Orleans.LogViews.ICustomStorageInterface<MyGrainState, object>
    {

      // we use another impl of this grain as the primary.
        ISimpleQueuedGrain storagegrain;

        public override Task OnActivateAsync()
        {
            storagegrain = GrainFactory.GetGrain<ISimpleQueuedGrain>(this.GetPrimaryKeyLong(), "UnitTests.Grains.SimpleQueuedGrainSharedStorage");
            return TaskDone.Done;
        }

        public Task<bool> ApplyUpdatesToStorageAsync(IReadOnlyList<object> updates, int expectedversion)
        {
            return storagegrain.Update(updates, expectedversion);
        }

        public async Task<KeyValuePair<int, MyGrainState>> ReadStateFromStorageAsync()
        {
            var kvp = await storagegrain.Read();
            return new KeyValuePair<int, MyGrainState>(kvp.Key, (MyGrainState)kvp.Value);
        }
    }


}
