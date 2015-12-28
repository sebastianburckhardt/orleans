using System.Collections.Generic;

using Orleans.Runtime;
using Orleans.Providers;
using Orleans.EventSourcing;

namespace Orleans.Storage
{
    internal interface IJournaledStorageProviderManager : IProviderManager
    {
        Logger GetLogger(string loggerName);

        IEnumerable<string> GetProviderNames();

        int GetNumLoadedProviders();

        IJournaledStorageProvider GetDefaultProvider();

        bool TryGetProvider(string name, out IJournaledStorageProvider provider, bool caseInsensitive = false);
    }
}
