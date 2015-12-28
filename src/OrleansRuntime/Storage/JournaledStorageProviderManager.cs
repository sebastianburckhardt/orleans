using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans.EventSourcing;
using Orleans.Providers;
using Orleans.Runtime.Configuration;
using Orleans.Runtime.Providers;
using Orleans.Storage;

namespace Orleans.Runtime.Storage
{
    internal class JournaledStorageProviderManager : IJournaledStorageProviderManager, IStorageProviderRuntime
    {
        private ProviderLoader<IJournaledStorageProvider> storageProviderLoader;
        private IProviderRuntime providerRuntime;

        public JournaledStorageProviderManager(IGrainFactory grainFactory, IServiceProvider serviceProvider)
        {
            GrainFactory = grainFactory;
            ServiceProvider = serviceProvider;
        }

        internal Task LoadStorageProviders(IDictionary<string, ProviderCategoryConfiguration> configs)
        {
            storageProviderLoader = new ProviderLoader<IJournaledStorageProvider>();
            providerRuntime = SiloProviderRuntime.Instance;

            if (!configs.ContainsKey(ProviderCategoryConfiguration.JOURNALED_STORAGE_PROVIDER_CATEGORY_NAME))
                return TaskDone.Done;

            storageProviderLoader.LoadProviders(configs[ProviderCategoryConfiguration.JOURNALED_STORAGE_PROVIDER_CATEGORY_NAME].Providers, this);
            return storageProviderLoader.InitProviders(providerRuntime);
        }

        public Task CloseProviders()
        {
            List<Task> tasks = new List<Task>();
            foreach (var provider in GetProviders())
            {
                tasks.Add(provider.Close());
            }
            return Task.WhenAll(tasks);
        }

        public int GetNumLoadedProviders()
        {
            return storageProviderLoader.GetNumLoadedProviders();
        }

        public IList<IJournaledStorageProvider> GetProviders()
        {
            return storageProviderLoader.GetProviders();
        }

        public Logger GetLogger(string loggerName)
        {
            return TraceLogger.GetLogger(loggerName, TraceLogger.LoggerType.Provider);
        }

        public Guid ServiceId
        {
            get { return providerRuntime.ServiceId; }
        }

        public string SiloIdentity
        {
            get { return providerRuntime.SiloIdentity; }
        }

        public IGrainFactory GrainFactory { get; private set; }
        public IServiceProvider ServiceProvider { get; private set; }

        /// <summary>
        /// Get list of providers loaded in this silo.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<string> GetProviderNames()
        {
            var providers = storageProviderLoader.GetProviders();
            return providers.Select(p => p.GetType().FullName).ToList();
        }

        public IJournaledStorageProvider GetDefaultProvider()
        {
            return storageProviderLoader.GetDefaultProvider(Constants.DEFAULT_STORAGE_PROVIDER_NAME);
        }

        public bool TryGetProvider(string name, out IJournaledStorageProvider provider, bool caseInsensitive = false)
        {
            return storageProviderLoader.TryGetProvider(name, out provider, caseInsensitive);
        }

        public IProvider GetProvider(string name)
        {
            return storageProviderLoader.GetProvider(name, true);
        }

        // used only for testing
        internal Task LoadEmptyStorageProviders(IProviderRuntime providerRtm)
        {
            storageProviderLoader = new ProviderLoader<IJournaledStorageProvider>();
            providerRuntime = providerRtm;

            storageProviderLoader.LoadProviders(new Dictionary<string, IProviderConfiguration>(), this);
            return storageProviderLoader.InitProviders(providerRuntime);
        }

        // used only for testing
        internal async Task AddAndInitProvider(string name, IJournaledStorageProvider provider, IProviderConfiguration config=null)
        {
            await provider.Init(name, this, config);
            storageProviderLoader.AddProvider(name, provider, config);
        }
    }
}
