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

        /// <summary>
        /// Deletes the stream. If <paramref name="expectedVersion"/> is provided an optimistic concurrency check will be made.
        /// </summary>
        Task ClearState(string streamName, int? expectedVersion);

        /// <summary>
        /// Reads the state from the stream.
        /// </summary>
        Task ReadState(string streamName, GrainState grainState);

        /// <summary>
        /// Writes <paramref name="newEvents"/> to the stream. If <paramref name="expectedVersion"/> is provided an optimistic concurrency check will be made.
        /// </summary>
        Task WriteState(string streamName, int? expectedVersion, IEnumerable<object> newEvents);
    }
}
