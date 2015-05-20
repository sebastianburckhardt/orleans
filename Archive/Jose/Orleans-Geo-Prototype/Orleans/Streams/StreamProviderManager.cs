#if !DISABLE_STREAMS
using System;
using System.Collections.Generic;
using Orleans.Providers;
using System.Threading.Tasks;


namespace Orleans.Streams
{
    internal class StreamProviderManager : IStreamProviderManager
    {
        private ProviderLoader<IStreamProvider> appStreamProviders;

        internal async Task LoadStreamProviders(
            Dictionary<string, ProviderCategoryConfiguration> configs,
            IStreamProviderRuntime providerRuntime)
        {
            appStreamProviders = new ProviderLoader<IStreamProvider>();

            if (!configs.ContainsKey("Stream"))
                return;

            appStreamProviders.LoadProviders(configs["Stream"].Providers, this);
            await appStreamProviders.InitProviders(providerRuntime);
        }

        public IEnumerable<IStreamProvider> GetStreamProviders()
        {
            return appStreamProviders.GetProviders();
        }

        public IOrleansProvider GetProvider(string name)
        {
            return appStreamProviders.GetProvider(name);
        }
    }
}

#endif