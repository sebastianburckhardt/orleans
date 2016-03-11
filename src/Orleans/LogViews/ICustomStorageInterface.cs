using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.LogViews
{
    /// <summary>
    /// An interface for grains using the CustomStorageLogViewProvider
    /// </summary>
    public interface ICustomStorageInterface<State, Update>
    {
        /// <summary>
        /// Reads the current state and version from storage.
        /// </summary>
        /// <returns>the version number and the state</returns>
        Task<KeyValuePair<int, State>> ReadStateFromStorageAsync();

        /// <summary>
        /// Applies the given array of updates to storage, if the version in storage matches the expected version. 
        /// Otherwise, does nothing. If successful, the version of storage increases by the number of updates.
        /// </summary>
        /// <param name="u"></param>
        /// <returns>true if the updates were applied, false otherwise</returns>
        Task<bool> ApplyUpdatesToStorageAsync(IReadOnlyList<Update> updates, int expectedversion);
    }

}
