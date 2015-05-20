#if !DISABLE_STREAMS
using System.Threading.Tasks;
using Orleans.Providers;

namespace Orleans.Streams
{
    /// <summary>
    /// Adapter factory.  This should create an adapter from the stream provider configuration
    /// </summary>
    public interface IQueueAdapterFactory
    {
        Task<IQueueAdapter> Create(IProviderConfiguration config);
    }
}
#endif