#if !DISABLE_STREAMS
using System;
using System.Threading.Tasks;
using Orleans.Streams;
using Orleans.Providers.Streams.Persistent.AzureQueueAdapter;

namespace Orleans.Providers.Streams.Persistent
{
    /// <summary>
    /// Persistent stream provider that uses azure queue for persistence
    /// </summary>
    public class AzureQueueStreamProvider : PersistentStreamProvider<AzureQueueAdapterFactory> { }
}

#endif