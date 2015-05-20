#if !DISABLE_STREAMS

using System.Threading.Tasks;
using Orleans;
using Orleans.Streams;

namespace UnitTestGrainInterfaces.Halo.Streaming
{
    /// <summary>
    /// Stream producer grain that sends a single event at a time (when told, see SendEvent) and tracks the number of events sent
    /// </summary>
    interface IProducerEventCountingGrain : IGrain
    {
        Task BecomeProducer(StreamId streamId, string providerToUse);

        /// <summary>
        /// Sends a single event and, upon successful completion, updates the number of events produced.
        /// </summary>
        /// <returns></returns>
        Task SendEvent();

        Task<int> NumberProduced { get; }
    }
}
#endif