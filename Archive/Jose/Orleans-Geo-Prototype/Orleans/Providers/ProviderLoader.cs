using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Xml;

using System.Threading.Tasks;

namespace Orleans.Providers
{
    /// <summary>
    /// Providers configuration and loading error semantics:
    /// 1) We will only load the providers that were specified in the config. 
    /// If a provider is not specified in the config, we will not attempt to load it.
    /// Specificaly, it means both storage and streaming providers are loaded only if configured.
    /// 2) If a provider is specified in the config, but was not loaded (no type found, or constructor failed, or Init failed), the silo will fail to start.
    /// 
    /// Loading providers workflow and error handling implementation:
    /// 1) Load ProviderCategoryConfiguration.
    ///     a) If CategoryConfiguration not found - it is not an error, continue.
    /// 2) Go over all assemblies and load all found providers and instantiate them via ProviderTypeManager.
    ///     a) If a certain found provider type failed to get instantiated, it is not an error, continue.
    /// 3) Validate all providers were loaded: go over all provider config and check that we could indeed load and instantiate all of them.
    ///     a) If failed to load or instantiate at least one configured provider, fail the silo start.
    /// 4) InitProviders: call Init on all loaded providers. 
    ///     a) Failure to init a provider wil result in silo failing to start.
    /// </summary>
    /// <typeparam name="TProvider"></typeparam>

    internal class ProviderLoader<TProvider>
        where TProvider : IOrleansProvider
    {
        private readonly Dictionary<string, TProvider> _providers;
        private Dictionary<string, IProviderConfiguration> _providerConfigs;
        private readonly Logger _logger;

        public ProviderLoader()
        {
            _logger = Logger.GetLogger("ProviderLoader/" + typeof(TProvider).Name, Logger.LoggerType.Runtime);
            _providers = new Dictionary<string, TProvider>();
        }

        public void LoadProviders(Dictionary<string, IProviderConfiguration> configs, IProviderManager providerManager)
        {

            _providerConfigs = configs ?? new Dictionary<string, IProviderConfiguration>();

            foreach (var provider in _providerConfigs.Values)
                ((ProviderConfiguration)provider).SetProviderManager(providerManager);

            // Load _providers
            ProviderTypeLoader.AddProviderTypeManager(t => typeof(TProvider).IsAssignableFrom(t), RegisterProviderType);
            ValidateProviders();
        }


        private void ValidateProviders()
        {
            foreach (var providerConfig in _providerConfigs.Values)
            {
                TProvider provider;
                var fullConfig = (ProviderConfiguration) providerConfig;
                if (!_providers.TryGetValue(fullConfig.Name, out provider))
                {
                    string msg = String.Format("Provider of type {0} name {1} was not loaded.", fullConfig.Type, fullConfig.Name);
                    _logger.Error(ErrorCode.Provider_ConfiguredProviderNotLoaded, msg);
                    throw new OrleansException(msg);
                }
            }
        }


        public async Task InitProviders(IProviderRuntime providerRuntime)
        {
            Dictionary<string, TProvider> providers; 
            lock (_providers)
            {
                providers = _providers.ToDictionary(p => p.Key, p => p.Value);
            }

            foreach (var provider in providers)
            {
                string name = provider.Key;
                try
                {
                    await provider.Value.Init(provider.Key, providerRuntime, _providerConfigs[name]);
                }
                catch (Exception exc)
                {
                    _logger.Error(ErrorCode.Provider_ErrorFromInit, string.Format("Exception initializing provider Name={0} Type={1}", name, provider), exc);
                    throw;
                }
            }
        }


        // used only for testing
        internal void AddProvider(string name, TProvider provider, IProviderConfiguration config)
        {
            lock (_providers)
            {
                _providers.Add(name, provider);
            }
        }

        internal int GetNumLoadedProviders()
        {
            lock (_providers)
            {
                return _providers.Count;
            }
        }

        public TProvider GetProvider(string name, bool caseInsensitive = false)
        {
            TProvider provider;
            if (!TryGetProvider(name, out provider, caseInsensitive))
            {
                throw new KeyNotFoundException(string.Format(
                                       "Cannot find provider of type {0} with Name={1}", typeof(TProvider).FullName, name));
            }
            return provider;
        }

        public bool TryGetProvider(string name, out TProvider provider, bool caseInsensitive = false)
        {
            lock (_providers)
            {
                if (!_providers.TryGetValue(name, out provider))
                {
                    if (caseInsensitive)
                    {
                        // Try all lower case
                        if (!_providers.TryGetValue(name.ToLowerInvariant(), out provider))
                        {
                            // Try all upper case
                            _providers.TryGetValue(name.ToUpperInvariant(), out provider);
                        }
                    }
                }
            }
            if (provider == null)
                return false;
            
            return true;
        }

        public List<TProvider> GetProviders()
        {
            lock (_providers)
            {
                return _providers.Values.AsList();
            }
        }

        public TProvider GetDefaultProvider(string defaultProviderName)
        {
            lock (_providers)
            {
                TProvider provider;
                // Use provider named "Default" if present
                if (!_providers.TryGetValue(defaultProviderName, out provider))
                {
                    // Otherwise, if there is only a single provider listed, use that
                    if (_providers.Count == 1) provider = _providers.First().Value;
                }
                if (provider == null)
                {
                    string errMsg = "Cannot find default provider for " + typeof(TProvider);
                    _logger.Error(ErrorCode.Provider_NoDefaultProvider, errMsg);
                    throw new InvalidOperationException(errMsg);
                }
                return provider;
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private void RegisterProviderType(Type t)
        {
            // First, figure out the provider type name
            var typeName = TypeUtils.GetFullName(t);

            // Now see if we have any config entries for that type 
            // If there's no config entry, then we don't load the type
            Type[] constructorBindingTypes = new[] { typeof(string), typeof(XmlElement) };
            foreach (var entry in _providerConfigs.Values)
            {
                var fullConfig = (ProviderConfiguration) entry;
                if (fullConfig.Type == typeName)
                {
                    // Found one! Now look for an appropriate constructor; try TProvider(string, Dictionary<string,string>) first
                    var constructor = t.GetConstructor(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
                        null, constructorBindingTypes, null);
                    var parms = new object[] { typeName, entry.Properties };
                    if (constructor == null)
                    {
                        // See if there's a default constructor to use, if there's no two-parameter constructor
                        constructor = t.GetConstructor(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
                            null, Type.EmptyTypes, null);
                        parms = new object[0];
                    }
                    if (constructor != null)
                    {
                        TProvider instance;
                        try
                        {
                            instance = (TProvider)constructor.Invoke(parms);
                        }
                        catch (Exception ex)
                        {
                            _logger.Warn(ErrorCode.Provider_InstanceConstructionError1, "Error constructing an instance of a " + typeName +
                                " provider using type " + t.Name + " for provider with name " + fullConfig.Name, ex);
                            return;
                        }

                        lock (_providers)
                        {
                            _providers[fullConfig.Name] = instance;
                            _logger.Info(ErrorCode.Provider_Loaded, "Loaded provider of type {0} Name={1}", typeName, fullConfig.Name);
                        }
                    }
                }
            }
        }
    }
}