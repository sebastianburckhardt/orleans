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

        Task ClearStateAsync(string grainType, GrainReference grainReference);

        Task ReadStateAsync(string grainType, GrainReference grainReference, GrainState grainState);

        Task WriteStateAsync(string grainType, GrainReference grainReference, IEnumerable<object> newEvents);
    }
}
