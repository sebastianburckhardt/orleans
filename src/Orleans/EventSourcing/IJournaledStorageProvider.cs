using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.Providers;
using Orleans.Runtime;

namespace Orleans.EventSourcing
{
    public interface IJournaledStorageProvider : IProvider
    {
        /// <summary>TraceLogger used by this storage provider instance.</summary>
        /// <returns>Reference to the TraceLogger object used by this provider.</returns>
        /// <seealso cref="Logger"/>
        Logger Log { get; }

        Task ClearState(string streamName);

        Task ReadState(string streamName, GrainState grainState);

        Task WriteState(string streamName, IEnumerable<object> newEvents);
    }
}
