#if !DISABLE_STREAMS
using System.Threading.Tasks;
using Orleans.Providers;

namespace Orleans.Streams
{
    internal interface IPersistentStreamPullingAgent : ISystemTarget, IStreamProducerExtension
    {
        // the pub sub interface as well as ring provider have to be Immutable<>, since we want deliberatily to pass them by reference.
        Task Init(Immutable<IQueueAdapter> queueAdapter);
    }
}

#endif