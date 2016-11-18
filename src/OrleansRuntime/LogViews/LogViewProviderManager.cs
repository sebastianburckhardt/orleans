using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans.Core;
using Orleans.Providers;
using Orleans.Runtime.Configuration;
using Orleans.Runtime.Providers;
using Orleans.LogViews;
using Orleans.Storage;

namespace Orleans.Runtime.LogViews
{
    internal class LogViewProviderManager : ILogViewProviderManager, ILogViewProviderRuntime
    {
        private ProviderLoader<ILogViewProvider> logViewProviderLoader;
        private IProviderRuntime providerRuntime;
        private IStorageProviderManager storageProviderManager;
        public IGrainFactory GrainFactory { get; private set; }
        public IServiceProvider ServiceProvider { get; private set; }

        public LogViewProviderManager(IGrainFactory grainFactory, IServiceProvider serviceProvider, IStorageProviderManager storageProviderManager)
        {
            GrainFactory = grainFactory;
            ServiceProvider = serviceProvider;
            this.storageProviderManager = storageProviderManager;
        }

        internal Task LoadLogViewProviders(IDictionary<string, ProviderCategoryConfiguration> configs)
        {
            logViewProviderLoader = new ProviderLoader<ILogViewProvider>();
            providerRuntime = SiloProviderRuntime.Instance;

            if (!configs.ContainsKey(ProviderCategoryConfiguration.LOG_VIEW_PROVIDER_CATEGORY_NAME))
                return TaskDone.Done;

            logViewProviderLoader.LoadProviders(configs[ProviderCategoryConfiguration.LOG_VIEW_PROVIDER_CATEGORY_NAME].Providers, this);
            return logViewProviderLoader.InitProviders(this);
        }

        internal void UnloadLogViewProviders()
        {
            foreach (var provider in logViewProviderLoader.GetProviders())
            {
                var disp = provider as IDisposable;
                if (disp != null)
                    disp.Dispose();
            }
        }

        public int GetLoadedProvidersNum()
        {
            return logViewProviderLoader.GetLoadedProvidersNum();
        }

        public IList<ILogViewProvider> GetProviders()
        {
            return logViewProviderLoader.GetProviders();
        }

        public void SetInvokeInterceptor(InvokeInterceptor interceptor)
        {
            providerRuntime.SetInvokeInterceptor(interceptor);
        }

        public InvokeInterceptor GetInvokeInterceptor()
        {
            return providerRuntime.GetInvokeInterceptor();
        }

        public Logger GetLogger(string loggerName)
        {
            return LogManager.GetLogger(loggerName, LoggerType.Provider);
        }

        public Guid ServiceId
        {
            get { return providerRuntime.ServiceId; }
        }

        public string SiloIdentity
        {
            get { return providerRuntime.SiloIdentity; }
        }

        /// <summary>
        /// Get list of providers loaded in this silo.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<string> GetProviderNames()
        {
            var providers = logViewProviderLoader.GetProviders();
            return providers.Select(p => p.GetType().FullName).ToList();
        }

        public bool TryGetProvider(string name, out ILogViewProvider provider, bool caseInsensitive = false)
        {
            return logViewProviderLoader.TryGetProvider(name, out provider, caseInsensitive);
        }

        public IProvider GetProvider(string name)
        {
            return logViewProviderLoader.GetProvider(name, true);
        }

        public bool TryGetStorageProvider(string name, out IStorageProvider provider, bool caseInsensitive = false)
        {
            return storageProviderManager.TryGetProvider(name, out provider, caseInsensitive);
        }

        // A log view provider that is really just a wrapper around the storage provider
        internal class WrappedStorageProvider : ILogViewProvider
        {
            public WrappedStorageProvider(IStorageProvider storageProvider)
            {
                globalstorageprovider = storageProvider;
            }
            internal IStorageProvider globalstorageprovider;

            public ILogViewAdaptor<T, E> MakeLogViewAdaptor<T, E>(ILogViewHost<T, E> hostGrain, T initialState, string grainTypeName, IProtocolServices services) 
                where T : class,new() where E: class
            {
                return new StorageProviderLogViewAdaptor<T,E>(hostGrain, initialState, this, globalstorageprovider, grainTypeName, services);
            }

            public string Name
            {
                get { return globalstorageprovider.Name; }
            }

            public Logger Log { get { return globalstorageprovider.Log;  } }

            public Task Init(string name, IProviderRuntime providerRuntime, IProviderConfiguration config)
            {
                // not called
                return TaskDone.Done;
            }

            public Task Close()
            {
                return TaskDone.Done;
            }
        }

    
    }
}
