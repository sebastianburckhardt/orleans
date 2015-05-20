using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans.Providers;
using Orleans.Runtime.Providers;


namespace Orleans.Runtime
{
    internal class AppBootstrapManager : IProviderManager
    {
        private ProviderLoader<IBootstrapProvider> _appBootstrapProviders;
        private readonly Logger _logger;

        internal AppBootstrapManager()
        {
            _logger = Logger.GetLogger(this.GetType().Name, Logger.LoggerType.Runtime);
        }

        public IOrleansProvider GetProvider(string name)
        {
            return _appBootstrapProviders != null ? _appBootstrapProviders.GetProvider(name) : null;
        }

        public List<IBootstrapProvider> GetProviders()
        {
            return _appBootstrapProviders != null ? _appBootstrapProviders.GetProviders() : new List<IBootstrapProvider>();
        }

        internal async Task LoadAppBootstrapProviders(Dictionary<string, ProviderCategoryConfiguration> configs)
        {
            ProviderCategoryConfiguration categoryConfig;
            if (!configs.TryGetValue(BootstrapProviderConstants.ConfigCategoryName, out categoryConfig))
            {
                return;
            }
            Dictionary<string, IProviderConfiguration> providers = categoryConfig.Providers;

            _appBootstrapProviders = new ProviderLoader<IBootstrapProvider>();

            _appBootstrapProviders.LoadProviders(providers, this);

            _logger.Info(ErrorCode.SiloCallingAppBootstrapClasses, "Calling app bootstrap classes");

            // Await here to force any errors to show this method name in stack trace, for better diagnostics
            await _appBootstrapProviders.InitProviders(SiloProviderRuntime.Instance);
        }
    }
}
