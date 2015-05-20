#if !DISABLE_STREAMS 

using System;
using System.Collections.Generic;
using Orleans.Providers;

namespace Orleans.Streams
{
    public interface IStreamProvider : IOrleansProvider
    {
        IAsyncStream<T> GetStream<T>(StreamId id);

        /// <summary>
        /// Determines whether this is a rewindable provider - supports creating rewindable streams 
        /// (streams that allow subscribing from previous point in time).
        /// </summary>
        /// <returns>True if this is a rewindable provider, false otherwise.</returns>
        bool IsRewindable { get; }
    }

    internal interface IStreamProviderManager : IProviderManager
    {
        IEnumerable<IStreamProvider> GetStreamProviders();
    }
}

#endif