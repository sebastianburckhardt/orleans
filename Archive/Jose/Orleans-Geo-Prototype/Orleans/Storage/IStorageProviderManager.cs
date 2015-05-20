using Orleans.Providers;

using System.Collections.Generic;

namespace Orleans.Storage
{
    internal interface IStorageProviderManager : IProviderManager
    {
        OrleansLogger GetLogger(string loggerName, Logger.LoggerType logType);

        IEnumerable<string> GetProviderNames();

        int GetNumLoadedProviders();

        IStorageProvider GetDefaultProvider();

        bool TryGetProvider(string name, out IStorageProvider provider, bool caseInsensitive = false);
    }
}
