using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans.Providers;
using Orleans.Runtime.Scheduler;

using Orleans.Runtime.Providers;
using Orleans.Storage;

namespace Orleans.Runtime.Storage
{
    internal class StorageProviderManager : IStorageProviderManager, IStorageProviderRuntime
    {
        private ProviderLoader<IStorageProvider> _storageProviderLoader;
        
        internal Task LoadStorageProviders(Dictionary<string, ProviderCategoryConfiguration> configs)
        {
            _storageProviderLoader = new ProviderLoader<IStorageProvider>();
            
            if(!configs.ContainsKey("Storage"))
                return TaskDone.Done;

            _storageProviderLoader.LoadProviders(configs["Storage"].Providers, this);
            return _storageProviderLoader.InitProviders(SiloProviderRuntime.Instance);
        }

        public int GetNumLoadedProviders()
        {
            return _storageProviderLoader.GetNumLoadedProviders();
        }

        public OrleansLogger GetLogger(string loggerName, Logger.LoggerType logType)
        {
            return Logger.GetLogger(loggerName, logType);
        }

        /// <summary>
        /// Get list of providers loaded in this silo.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<string> GetProviderNames()
        {
            var providers = _storageProviderLoader.GetProviders();
            return providers.Select(p => p.GetType().FullName).ToList();
        }

        public IStorageProvider GetDefaultProvider()
        {
            return _storageProviderLoader.GetDefaultProvider(Constants.DEFAULT_STORAGE_PROVIDER_NAME);
        }

        public bool TryGetProvider(string name, out IStorageProvider provider, bool caseInsensitive = false)
        {
            return _storageProviderLoader.TryGetProvider(name, out provider, caseInsensitive);
        }

        public IOrleansProvider GetProvider(string name)
        {
            return _storageProviderLoader.GetProvider(name, true);
        }

        // used only for testing
        internal Task LoadEmptyStorageProviders()
        {
            _storageProviderLoader = new ProviderLoader<IStorageProvider>();
            _storageProviderLoader.LoadProviders(new Dictionary<string, IProviderConfiguration>(), this);
            return _storageProviderLoader.InitProviders(ClientProviderRuntime.Instance);
        }

        // used only for testing
        internal async Task AddAndInitProvider(string name, IStorageProvider provider, IProviderConfiguration config=null)
        {
            await provider.Init(name, this, config);
            _storageProviderLoader.AddProvider(name, provider, config);
        }
    }
}
