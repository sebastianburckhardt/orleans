#if !DISABLE_STREAMS

using System.Threading.Tasks;
using Orleans;
using Orleans.Streams;

namespace UnitTestGrainInterfaces.Halo.Streaming
{
    /// <summary>
    /// Stream consumer grain that just counts the events it consumes
    /// </summary>
    interface IConsumerEventCountingGrain : IGrain
    {
        Task BecomeConsumer(StreamId streamId, string providerToUse);

        Task StopConsuming();

        Task<int> NumberConsumed { get; }
    }
}

#endif