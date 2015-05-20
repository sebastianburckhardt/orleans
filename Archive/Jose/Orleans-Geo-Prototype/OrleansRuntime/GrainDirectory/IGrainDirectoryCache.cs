using System;
using System.Collections.Generic;


namespace Orleans.Runtime.GrainDirectory
{
    interface IGrainDirectoryCache<TValue>
    {
        /// <summary>
        /// Adds a new entry with the given version into the cache: key (grain) --> value
        /// The new entry will override any existing entry under the given key, 
        /// regardless of the stored version
        /// </summary>
        /// <param name="key">key to add</param>
        /// <param name="value">value to add</param>
        /// <param name="version">version for the value</param>
        void AddOrUpdate(GrainId key, TValue value, int version);

        /// <summary>
        /// Removes an entry from the cache given its key
        /// </summary>
        /// <param name="key">key to remove</param>
        /// <returns>True iff the enrty was in the cache and the removal was succeful</returns>
        bool Remove(GrainId key);
        
        /// <summary>
        /// Clear the cache, deleting all entries.
        /// </summary>
        void Clear();

        /// <summary>
        /// Lookups the cached value by the given key.
        /// </summary>
        /// <param name="key">key for the lookup</param>
        /// <param name="result">value if the key is found, undefined otherwise</param>
        /// <returns>true iff the the given key is in the cache</returns>
        bool LookUp(GrainId key, out TValue result);

        /// <summary>
        /// Returns list of key-value-version tuples stored currently in the cache.
        /// </summary>
        List<Tuple<GrainId, TValue, int>> KeyValues { get; }
    }
}
