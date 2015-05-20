using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Storage
{
    /// <summary>
    /// Grain interface for internal memory storage grain used by Orleans in-memory storage provider.
    /// </summary>
    /// <seealso cref="Orleans.Storage.MemoryStorage"/>
    /// <seealso cref="Orleans.Storage.MemoryStorageGrain"/>
    public interface IMemoryStorageGrain : IGrain
    {
        /// <summary>
        /// Async method to cause retrieval of the specified grain state data from memory store.
        /// </summary>
        /// <param name="grainType">Type of this grain [fully qualified class name]</param>
        /// <param name="grainReference">Grain reference object for this grain.</param>
        /// <returns>Value promise for the currently stored grain state for the specified grain.</returns>
        Task<IGrainState> ReadStateAsync(string grainType, GrainReference grainReference);
        
        /// <summary>
        /// Async method to cause update of the specified grain state data into memory store.
        /// </summary>
        /// <param name="grainType">Type of this grain [fully qualified class name]</param>
        /// <param name="grainReference">Grain reference object for this grain.</param>
        /// <param name="grainState">New state data to be stored for this grain.</param>
        /// <returns>Completion promise for the update operation for stored grain state for the specified grain.</returns>
        Task WriteStateAsync(string grainType, GrainReference grainReference, IGrainState grainState);

        /// <summary>
        /// Async method to cause deletion of the specified grain state data from memory store.
        /// </summary>
        /// <param name="grainType">Type of this grain [fully qualified class name]</param>
        /// <param name="grainReference">Grain reference object for this grain.</param>
        /// <returns>Completion promise for the update operation for stored grain state for the specified grain.</returns>
        Task DeleteStateAsync(string grainType, GrainReference grainReference);
    }
}
