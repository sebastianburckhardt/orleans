using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Orleans;
using Orleans.Providers;

namespace Orleans.Storage
{
    /// <summary>
    /// This is a simple in-memory grain implementation of a storage provider.
    /// </summary>
    /// <remarks>
    /// This storage provider is ONLY intended for simple in-memory Development / Unit Test scenarios.
    /// This class should NOT be used in Production environment, 
    ///  because [by-design] it does not provide any resilience 
    ///  or long-term persistence capabilities.
    /// </remarks>
    /// <example>
    /// Example configuration for this storage provider in OrleansConfiguration.xml file:
    /// <code>
    /// &lt;OrleansConfiguration xmlns="urn:orleans">
    ///   &lt;Globals>
    ///     &lt;StorageProviders>
    ///       &lt;Provider Type="Orleans.Storage.MemoryStorage" Name="MemoryStore" />
    ///   &lt;/StorageProviders>
    /// </code>
    /// </example>
    public class MemoryStorage : IStorageProvider
    {
        private const int DEFAULT_NUM_STORAGE_GRAINS = 10;
        private const string NUM_STORAGE_GRAINS = "NumStorageGrains";
        private int numStorageGrains;
        private static int _counter;
        private readonly int _id;

        private Lazy<IMemoryStorageGrain>[] _storageGrains;

        /// <summary> Name of this storage provider instance. </summary>
        /// <see cref="IOrleansProvider#Name"/>
        public string Name { get; private set; }

        /// <summary> Logger used by this storage provider instance. </summary>
        /// <see cref="IStorageProvider#Log"/>
        public OrleansLogger Log { get; private set; }

        public MemoryStorage()
        {
            _id = Interlocked.Increment(ref _counter);
        }

        /// <summary> Initialization function for this storage provider. </summary>
        /// <see cref="IOrleansProvider#Init"/>
        public Task Init(string name, IProviderRuntime providerRuntime, IProviderConfiguration config)
        {
            this.Name = name;
            this.Log = providerRuntime.GetLogger("Storage.MemoryStorage." + _id, Logger.LoggerType.Application);

            numStorageGrains = DEFAULT_NUM_STORAGE_GRAINS;
            string numStorageGrainsStr;
            if (config.Properties.TryGetValue(NUM_STORAGE_GRAINS, out numStorageGrainsStr))
            {
                numStorageGrains = Int32.Parse(numStorageGrainsStr);
            }

            Log.Info("Init: Name={0} NumStorageGrains={1}", Name, numStorageGrains);

            _storageGrains = new Lazy<IMemoryStorageGrain>[numStorageGrains];
            for (int i = 0; i < numStorageGrains; i++)
            {
                int idx = i; // Capture variable to avoid modified closure error
                _storageGrains[idx] = new Lazy<IMemoryStorageGrain>(() => MemoryStorageGrainFactory.GetGrain(idx));
            }
            return TaskDone.Done;
        }

        /// <summary> Shutdown function for this storage provider. </summary>
        /// <see cref="IStorageProvider#Close"/>
        public Task Close()
        {
            for (int i = 0; i < numStorageGrains; i++)
            {
                _storageGrains[i] = null;
            }
            return TaskDone.Done;
        }

        /// <summary> Read state data function for this storage provider. </summary>
        /// <see cref="IStorageProvider#ReadStateAsync"/>
        public async Task ReadStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            IMemoryStorageGrain storageGrain = GetStorageGrain(grainReference);
            IGrainState state = await storageGrain.ReadStateAsync(grainType, grainReference);
            if (state != null)
            {
                grainState.SetAll(state.AsDictionary());
            }
        }

        /// <summary> Write state data function for this storage provider. </summary>
        /// <see cref="IStorageProvider#WriteStateAsync"/>
        public Task WriteStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            IMemoryStorageGrain storageGrain = GetStorageGrain(grainReference);
            return storageGrain.WriteStateAsync(grainType, grainReference, grainState);
        }

        /// <summary> Delete / Clear state data function for this storage provider. </summary>
        /// <see cref="IStorageProvider#ClearStateAsync"/>
        public Task ClearStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            IMemoryStorageGrain storageGrain = GetStorageGrain(grainReference);
            return storageGrain.DeleteStateAsync(grainType, grainReference);
        }

        private IMemoryStorageGrain GetStorageGrain(GrainReference grainReference)
        {
            int idx = StorageProviderUtility.PositiveHash(grainReference, numStorageGrains);
            IMemoryStorageGrain storageGrain = _storageGrains[idx].Value;
            return storageGrain;
        }
    }

    /// <summary>
    /// Implementaiton class for the Storage Grain used by In-memory Storage Provider
    /// </summary>
    /// <seealso cref="MemoryStorage"/>
    /// <seealso cref="IMemoryStorageGrain"/>
    internal class MemoryStorageGrain : GrainBase, IMemoryStorageGrain
    {
        private Dictionary<string, GrainStateStore> _grainStore;

        public override Task ActivateAsync()
        {
            _grainStore = new Dictionary<string, GrainStateStore>();
            return TaskDone.Done;
        }

        public override Task DeactivateAsync()
        {
            _grainStore = null;
            return TaskDone.Done;
        }

        public Task<IGrainState> ReadStateAsync(string grainType, GrainReference grainReference)
        {
            GrainStateStore storage = GetStoreForGrain(grainType);
            IGrainState state = storage.GetGrainState(grainReference);
            return Task.FromResult(state);
        }

        public Task WriteStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            GrainStateStore storage = GetStoreForGrain(grainType);
            storage.UpdateGrainState(grainReference, grainState);
            return TaskDone.Done;
        }

        public Task DeleteStateAsync(string grainType, GrainReference grainId)
        {
            GrainStateStore storage = GetStoreForGrain(grainType);
            storage.DeleteGrainState(grainId);
            return TaskDone.Done;
        }

        private GrainStateStore GetStoreForGrain(string grainType)
        {
            GrainStateStore storage;
            if (!_grainStore.TryGetValue(grainType, out storage))
            {
                storage = new GrainStateStore();
                _grainStore.Add(grainType, storage);
            }
            return storage;
        }

        private class GrainStateStore
        {
            private readonly Dictionary<GrainReference, IGrainState> _grainStateStorage = new Dictionary<GrainReference, IGrainState>();

            public IGrainState GetGrainState(GrainReference grainReference)
            {
                IGrainState state;
                _grainStateStorage.TryGetValue(grainReference, out state);
                return state;
            }

            public void UpdateGrainState(GrainReference grainReference, IGrainState state)
            {
                IGrainState grainState;
                if (_grainStateStorage.TryGetValue(grainReference, out grainState))
                {
                    grainState.SetAll(state.AsDictionary());
                }
                else
                {
                    _grainStateStorage.Add(grainReference, state);
                }
            }

            public void DeleteGrainState(GrainReference grainReference)
            {
                _grainStateStorage.Remove(grainReference);
            }
        }
    }

    internal class StorageProviderUtility
    {
        public static int PositiveHash(GrainReference grainReference, int hashRange)
        {
            int hash = grainReference.GetUniformHashCode();
            int positiveHash = ((hash % hashRange) + hashRange) % hashRange;
            return positiveHash;
        }
    }
}
