using System.Collections.Generic;

using Orleans.Runtime;
using Orleans.Providers;


namespace Orleans.LogViews
{
    internal interface ILogViewProviderManager : IProviderManager
    {
        Logger GetLogger(string loggerName);

        IEnumerable<string> GetProviderNames();

        int GetNumLoadedProviders();

        bool TryGetProvider(string name, out ILogViewProvider provider, bool caseInsensitive = false);    
    }


}
