/*
Project Orleans Cloud Service SDK ver. 1.0
 
Copyright (c) Microsoft Corporation
 
All rights reserved.
 
MIT License

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and 
associated documentation files (the ""Software""), to deal in the Software without restriction,
including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense,
and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

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

        public int GetNumLoadedProviders()
        {
            return logViewProviderLoader.GetNumLoadedProviders();
        }

        public IList<ILogViewProvider> GetProviders()
        {
            return logViewProviderLoader.GetProviders();
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

        public ILogViewProvider WrapStorageProvider(IStorageProvider storageprovider)
        {
            // create a wrapping provider.
            return new WrappedStorageProvider() { globalstorageprovider = storageprovider };
        }

        public bool TryGetStorageProvider(string name, out IStorageProvider provider, bool caseInsensitive = false)
        {
            return storageProviderManager.TryGetProvider(name, out provider, caseInsensitive);
        }


        // A log view provider that is really just a wrapper around the storage provider
        private class WrappedStorageProvider : ILogViewProvider
        {
            internal IStorageProvider globalstorageprovider;

            public ILogViewAdaptor<T,E> MakeLogViewAdaptor<T,E>(ILogViewAdaptorHost hostgrain, T initialstate, string graintypename, IProtocolServices services) where T : LogViewType<E>, new() where E: class
            {
                return new StorageBasedLogViewAdaptor<T,E>(hostgrain, initialstate, this, globalstorageprovider, graintypename, services);
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
