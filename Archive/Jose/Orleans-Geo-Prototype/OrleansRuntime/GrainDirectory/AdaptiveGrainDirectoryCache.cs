﻿using System;
using System.Collections.Generic;
using System.Text;

using Orleans.Counters;

namespace Orleans.Runtime.GrainDirectory
{
    internal class AdaptiveGrainDirectoryCache<TValue> : IGrainDirectoryCache<TValue>
    {
        internal class GrainDirectoryCacheEntry
        {
            internal TValue Value { get; private set; }

            internal DateTime Created { get; set; }

            private DateTime LastRefreshed { get; set; }

            internal TimeSpan ExpirationTimer { get; private set; }

            internal int ETag { get; private set; }

            /// <summary>
            /// flag notifying whether this cache entry was accessed lately 
            /// (more precisely, since the last refresh)
            /// </summary>
            internal int NumAccesses { get; set; }

            internal GrainDirectoryCacheEntry(TValue value, int etag, DateTime created, TimeSpan expirationTimer)
            {
                Value = value;
                ETag = etag;
                ExpirationTimer = expirationTimer;
                Created = created;
                LastRefreshed = DateTime.UtcNow;
                NumAccesses = 0;
            }

            internal bool IsExpired()
            {
                return DateTime.UtcNow >= LastRefreshed.Add(ExpirationTimer);
            }

            internal void Refresh(TimeSpan newExpirationTimer)
            {
                LastRefreshed = DateTime.UtcNow;
                ExpirationTimer = newExpirationTimer;
            }
        }

        private LRU<GrainId, GrainDirectoryCacheEntry> cache;
        /// controls the time the new entry is considered "fresh" (unit: ms)
        private readonly TimeSpan initialExpirationTimer;
        /// controls the exponential growth factor (i.e., x2, x4) for the freshness timer (unit: none)
        private readonly double exponentialTimerGrowth;
        // controls the boundary on the expiration timer
        private readonly TimeSpan maxExpirationTimer;

        internal long numAccesses;   // number of cache item accesses (for stats)
        internal long numHits;       // number of cache access hits (for stats)

        internal long lastNumAccesses;
        internal long lastNumHits;

        public AdaptiveGrainDirectoryCache(TimeSpan initialExpirationTimer, TimeSpan maxExpirationTimer, double exponentialTimerGrowth, int maxCacheSize)
        {
            cache = new LRU<GrainId, GrainDirectoryCacheEntry>(maxCacheSize, TimeSpan.MaxValue, null);

            this.initialExpirationTimer = initialExpirationTimer;
            this.maxExpirationTimer = maxExpirationTimer;
            this.exponentialTimerGrowth = exponentialTimerGrowth;

            IntValueStatistic.FindOrCreate(StatNames.STAT_DIRECTORY_CACHE_SIZE, () => cache.Count);
        }

        public void AddOrUpdate(GrainId key, TValue value, int version)
        {            
            GrainDirectoryCacheEntry entry = new GrainDirectoryCacheEntry(value, version, DateTime.UtcNow, initialExpirationTimer);

            // Notice that LRU should know how to throw the oldest entry if the cache is full
            cache.Add(key, entry);
        }

        public bool Remove(GrainId key)
        {
            GrainDirectoryCacheEntry tmp;
            return cache.RemoveKey(key, out tmp);
        }

        public void Clear()
        {
            cache.Clear();
        }

        public bool LookUp(GrainId key, out TValue result)
        {
            numAccesses++;      // for stats

            // Here we do not check whether the found entry is expired. 
            // It will be done by the thread managing the cache.
            // This is to avoid situation where the entry was just expired, but the manager still have not run and have not refereshed it.
            GrainDirectoryCacheEntry tmp;
            if (cache.TryGetValue(key, out tmp))
            {
                numHits++;      // for stats
                tmp.NumAccesses++;
                result = tmp.Value;
                return true;
            }
            else
            {
                result = default(TValue);
                return false;
            }
        }

        public List<Tuple<GrainId, TValue, int>> KeyValues
        {
            get
            {
                List<Tuple<GrainId, TValue, int>> result = new List<Tuple<GrainId, TValue, int>>();
                IEnumerator<KeyValuePair<GrainId, GrainDirectoryCacheEntry>> enumerator = GetStoredEntries();
                while (enumerator.MoveNext())
                {
                    KeyValuePair<GrainId, GrainDirectoryCacheEntry> current = enumerator.Current;
                    result.Add(new Tuple<GrainId, TValue, int>(current.Key, current.Value.Value, current.Value.ETag));
                }
                return result;
            }
        }

        public bool MarkAsFresh(GrainId key)
        {
            GrainDirectoryCacheEntry result;
            if (cache.TryGetValue(key, out result))
            {
                TimeSpan newExpirationTimer = StandardExtensions.Min(maxExpirationTimer, result.ExpirationTimer.Multiply(exponentialTimerGrowth));
                result.Refresh(newExpirationTimer);

                return true;
            }

            return false;
        }

        internal GrainDirectoryCacheEntry Get(GrainId key)
        {
            return cache.Get(key);
        }


        internal IEnumerator<KeyValuePair<GrainId, GrainDirectoryCacheEntry>> GetStoredEntries()
        {
            return cache.GetEnumerator();
        }

        public override string ToString()
        {
            var sb = new StringBuilder();

            long curNumAccesses = numAccesses - lastNumAccesses;
            lastNumAccesses = numAccesses;
            long curNumHits = numHits - lastNumHits;
            lastNumHits = numHits;

            sb.Append("Adaptive cache statistics:").AppendLine();
            sb.AppendFormat("   Cache size: {0} entries ({1} maximum)", cache.Count, cache.MaximumSize).AppendLine();
            sb.AppendFormat("   Since last call:").AppendLine();
            sb.AppendFormat("      Accesses: {0}", curNumAccesses);
            sb.AppendFormat("      Hits: {0}", curNumHits);
            if (curNumAccesses > 0)
            {
                sb.AppendFormat("      Hit Rate: {0:F1}%", (100.0 * curNumHits) / curNumAccesses).AppendLine();
            }
            sb.AppendFormat("   Since start:").AppendLine();
            sb.AppendFormat("      Accesses: {0}", lastNumAccesses);
            sb.AppendFormat("      Hits: {0}", lastNumHits);
            if (lastNumAccesses > 0)
            {
                sb.AppendFormat("      Hit Rate: {0:F1}%", (100.0 * lastNumHits) / lastNumAccesses).AppendLine();
            }

            return sb.ToString();
        }
    }
}
