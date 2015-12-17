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
using Orleans.Replication;
using Orleans.Storage;

namespace Orleans.Runtime.Replication
{
    internal class ReplicationProviderManager : IReplicationProviderManager, IReplicationProviderRuntime
    {
        private ProviderLoader<IReplicationProvider> replicationProviderLoader;
        private IProviderRuntime providerRuntime;
        private IStorageProviderManager storageProviderManager;

        public ReplicationProviderManager(IGrainFactory grainFactory, IServiceProvider serviceProvider, IStorageProviderManager storageProviderManager)
        {
            GrainFactory = grainFactory;
            ServiceProvider = serviceProvider;
            this.storageProviderManager = storageProviderManager;
        }

        internal Task LoadReplicationProviders(IDictionary<string, ProviderCategoryConfiguration> configs)
        {
            replicationProviderLoader = new ProviderLoader<IReplicationProvider>();
            providerRuntime = SiloProviderRuntime.Instance;

            if (!configs.ContainsKey(ProviderCategoryConfiguration.REPLICATION_PROVIDER_CATEGORY_NAME))
                return TaskDone.Done;

            replicationProviderLoader.LoadProviders(configs[ProviderCategoryConfiguration.REPLICATION_PROVIDER_CATEGORY_NAME].Providers, this);
            return replicationProviderLoader.InitProviders(this);
        }

        internal void UnloadReplicationProviders()
        {
            foreach (var provider in replicationProviderLoader.GetProviders())
            {
                var disp = provider as IDisposable;
                if (disp != null)
                    disp.Dispose();
            }
        }

        public int GetNumLoadedProviders()
        {
            return replicationProviderLoader.GetNumLoadedProviders();
        }

        public IList<IReplicationProvider> GetProviders()
        {
            return replicationProviderLoader.GetProviders();
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
            var providers = replicationProviderLoader.GetProviders();
            return providers.Select(p => p.GetType().FullName).ToList();
        }

        public bool TryGetProvider(string name, out IReplicationProvider provider, bool caseInsensitive = false)
        {
            return replicationProviderLoader.TryGetProvider(name, out provider, caseInsensitive);
        }

        public IProvider GetProvider(string name)
        {
            return replicationProviderLoader.GetProvider(name, true);
        }

        public IReplicationProvider WrapStorageProvider(IStorageProvider storageprovider)
        {
            // create a wrapping provider.
            return new WrappedStorageProvider() { globalstorageprovider = storageprovider };
        }

        public bool TryGetStorageProvider(string name, out IStorageProvider provider, bool caseInsensitive = false)
        {
            return storageProviderManager.TryGetProvider(name, out provider, caseInsensitive);
        }


        // A pseudo-replication provider that is really just a wrapper around the storage provider
        private class WrappedStorageProvider : IReplicationProvider
        {
            internal IStorageProvider globalstorageprovider;

            public IQueuedGrainAdaptor<T> MakeReplicationAdaptor<T>(IReplicationAdaptorHost hostgrain, T initialstate, string graintypename, IReplicationProtocolServices services) where T : GrainState, new()
            {
                return new SharedStorageAdaptor<T>(hostgrain, initialstate, this, globalstorageprovider, graintypename, services);
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


        // used only for testing
        internal Task LoadEmptyReplicationProviders(IProviderRuntime providerRtm)
        {
            replicationProviderLoader = new ProviderLoader<IReplicationProvider>();
            providerRuntime = providerRtm;

            replicationProviderLoader.LoadProviders(new Dictionary<string, IProviderConfiguration>(), this);
            return replicationProviderLoader.InitProviders(providerRuntime);
        }

        // used only for testing
        internal async Task AddAndInitProvider(string name, IReplicationProvider provider, IProviderConfiguration config = null)
        {
            await provider.Init(name, this, config);
            replicationProviderLoader.AddProvider(name, provider, config);
        }

  
    }
}
