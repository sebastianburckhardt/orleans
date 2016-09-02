using Orleans;
using Orleans.MultiCluster;
using Orleans.Providers;
using Orleans.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using UnitTests.GrainInterfaces;

namespace UnitTests.Grains
{

    // use the explictly specified "SharedStorage" log view provider
    [LogViewProvider(ProviderName = "SharedStorage")]
    public class SimpleLogViewGrainSharedStorage : SimpleLogViewGrain
    {
    }

    // use the explictly specified "SharedMemory" replication provider
    [LogViewProvider(ProviderName = "SharedMemory")]
    public class SimpleLogViewGrainSharedMemory : SimpleLogViewGrain
    {
    }

    // use the default storage provider as the shared storage
    public class SimpleLogViewGrainDefaultStorage : SimpleLogViewGrain
    {
    }


    // use  singleinstance and the explictly specified "SharedStorage" provider
    [GlobalSingleInstance]
    [LogViewProvider(ProviderName = "SharedStorage")]
    public class SimpleLogViewGrainSingleInstance : SimpleLogViewGrain
    {
    }

    // use an explicitly specified storage provider
    // use MemoryStore as the log view provider (uses GSI grain for memory store)
    [StorageProvider(ProviderName = "MemoryStore")]
    public class SimpleLogViewGrainMemoryStorage : SimpleLogViewGrain
    {
    }

    // use the explictly specified "CustomStorage" log view provider with symmetric access from all clusters
    [LogViewProvider(ProviderName = "CustomStorage")]
    public class SimpleLogViewGrainCustomStorage : SimpleLogViewGrain,
        Orleans.Providers.LogViews.ICustomStorageInterface<MyGrainState, object>
    {

      // we use another impl of this grain as the primary.
        ISimpleLogViewGrain storagegrain;

        public override Task OnActivateAsync()
        {
            storagegrain = GrainFactory.GetGrain<ISimpleLogViewGrain>(this.GetPrimaryKeyLong(), "UnitTests.Grains.SimpleLogViewGrainSharedStorage");
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

    // use the explictly specified "CustomStorage" log view provider with access from primary cluster only
    [LogViewProvider(ProviderName = "CustomStoragePrimaryCluster")]
    public class SimpleLogViewGrainCustomStoragePrimaryCluster : SimpleLogViewGrain,
        Orleans.Providers.LogViews.ICustomStorageInterface<MyGrainState, object>
    {

        // we use fake in-memory state as the storage
        MyGrainState state;
        int version;

        public Task<bool> ApplyUpdatesToStorageAsync(IReadOnlyList<object> updates, int expectedversion)
        {
            if (state == null)
            {
                state = new MyGrainState();
                version = 0;
            }

            if (expectedversion != version)
                return Task.FromResult(false);

            foreach (var u in updates)
            {
                state.Apply(u);
                version++;
            }

            return Task.FromResult(true);
        }

        public Task<KeyValuePair<int, MyGrainState>> ReadStateFromStorageAsync()
        {
            if (state == null)
            {
                state = new MyGrainState();
                version = 0;
            }
            return Task.FromResult(new KeyValuePair<int, MyGrainState>(version, (MyGrainState)SerializationManager.DeepCopy(state)));
        }
    }


}
