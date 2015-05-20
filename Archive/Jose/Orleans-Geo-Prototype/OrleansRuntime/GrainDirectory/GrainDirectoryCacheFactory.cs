using System;
using System.Collections.Generic;


namespace Orleans.Runtime.GrainDirectory
{
    internal static class GrainDirectoryCacheFactory<TValue>
    {
        //private const int MAX_CACHE_ENTRY_AGE = 1 * 60 * 1000;      // 1 min

        internal static IGrainDirectoryCache<TValue> CreateGrainDirectoryCache(GlobalConfiguration cfg)
        {
            if (cfg.CacheSize <= 0)
            {
                return new NullGrainDirectoryCache<TValue>();
            }
            switch (cfg.DirectoryCachingStrategy)
            {
                case GlobalConfiguration.DirectoryCachingStrategyType.None:
                    return new NullGrainDirectoryCache<TValue>();
                case GlobalConfiguration.DirectoryCachingStrategyType.LRU:
                    return new LRUBasedGrainDirectoryCache<TValue>(cfg.CacheSize, cfg.MaximumCacheTTL);
                default:
                    return new AdaptiveGrainDirectoryCache<TValue>(cfg.InitialCacheTTL, cfg.MaximumCacheTTL, cfg.CacheTTLExtensionFactor, cfg.CacheSize);
            }
        }

        internal static AsynchAgent CreateGrainDirectoryCacheMaintainer(LocalGrainDirectory router, IGrainDirectoryCache<TValue> cache)
        {
            if (cache is AdaptiveGrainDirectoryCache<TValue>)
            {
                return new AdaptiveDirectoryCacheMaintainer<TValue>(router, cache);
            }
            return null;
        }
    }

    internal class NullGrainDirectoryCache<TValue> : IGrainDirectoryCache<TValue>
    {
        private static readonly List<Tuple<GrainId, TValue, int>> EmptyList = new List<Tuple<GrainId, TValue, int>>();

        public void AddOrUpdate(GrainId key, TValue value, int version)
        {
            return;
        }

        public bool Remove(GrainId key)
        {
            return false;
        }

        public void Clear()
        {
            return;
        }

        public bool LookUp(GrainId key, out TValue result)
        {
            result = default(TValue);
            return false;
        }

        public List<Tuple<GrainId, TValue, int>> KeyValues
        {
            get { return EmptyList; }
        }
    }
}

