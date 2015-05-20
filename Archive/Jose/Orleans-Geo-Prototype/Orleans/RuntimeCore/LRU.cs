using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;

namespace Orleans
{
    // This class implements an LRU cache of values. It keeps a bounded set of values and will
    // flush "old" values 
    internal class LRU<TKey, TValue> : IEnumerable<KeyValuePair<TKey,TValue>> where TKey : class
    {
        // Delegate type for fetching the value associated with a given key.
        public delegate TValue FetchValueDelegate(TKey key);

        // The following machinery is used to notify client objects when a key and its value 
        // is being flushed from the cache.
        // The client's event handler is called after the key has been removed from the cache,
        // but when the cache is in a consistent state so that other methods on the cache may freely
        // be invoked.
        public class FlushEventArgs : EventArgs
        {
            private readonly TKey key;
            private readonly TValue value;

            public FlushEventArgs(TKey k, TValue v)
            {
                key = k;
                value = v;
            }

            public TKey Key
            {
                get { return key; }
            }

            public TValue Value
            {
                get { return value; }
            }
        }

        public event EventHandler<FlushEventArgs> RaiseFlushEvent;

        private long nextGeneration = 0;
        private long generationToFree = 0;
        private readonly TimeSpan requiredFreshness;
        // We want this to be a reference type so that we can update the values in the cache
        // without having to call AddOrUpdate, which is a nuisance
        private class TimestampedValue
        {
            public DateTime whenLoaded;
            public TValue value;
            public long generation;

            public TimestampedValue(LRU<TKey,TValue> l, TValue v)
            {
                generation = Interlocked.Increment(ref l.nextGeneration);
                value = v;
                whenLoaded = DateTime.UtcNow;
            }
        }
        private readonly ConcurrentDictionary<TKey, TimestampedValue> cache;
        readonly FetchValueDelegate fetcher;

        public int Count { get { return cache.Count; } }
        public int MaximumSize { get; private set; }

        /// <summary>
        /// Creates a new LRU cache.
        /// </summary>
        /// <param name="maxSize">Maximum number of entries to allow.</param>
        /// <param name="maxAge">Maximum age of an entry.</param>
        /// <param name="f"></param>
        public LRU(int maxSize, TimeSpan maxAge, FetchValueDelegate f) 
        {
            if (maxSize <= 0)
            {
                throw new ArgumentOutOfRangeException("maxSize", "LRU maxSize must be greater than 0");
            }
            MaximumSize = maxSize;
            requiredFreshness = maxAge;
            fetcher = f;
            cache = new ConcurrentDictionary<TKey, TimestampedValue>();
        }

        public void Add(TKey key, TValue value)
        {
            AdjustSize();
            var result = new TimestampedValue(this, value);
            cache.AddOrUpdate(key, result, (k, o) => result);
        }

        public bool ContainsKey(TKey key)
        {
            TimestampedValue ignore;

            return cache.TryGetValue(key, out ignore);
        }

        public bool RemoveKey(TKey key, out TValue value)
        {
            value = default(TValue);
            TimestampedValue tv;
            if (cache.TryRemove(key, out tv))
            {
                value = tv.value;
                return true;
            }
            return false;
        }

        public void Clear()
        {
            foreach (var pair in cache)
            {
                var args = new FlushEventArgs(pair.Key, pair.Value.value);
                EventHandler<FlushEventArgs> handler = RaiseFlushEvent;
                if (handler != null)
                {
                    handler(this, args);
                }
            }
            cache.Clear();
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            TimestampedValue result;

            value = default(TValue);

            if (cache.TryGetValue(key, out result))
            {
                result.generation = Interlocked.Increment(ref nextGeneration);
                TimeSpan age = DateTime.UtcNow.Subtract(result.whenLoaded);
                if (age > requiredFreshness)
                {
                    if (cache.TryRemove(key, out result))
                    {
                        if (RaiseFlushEvent != null)
                        {
                            var args = new FlushEventArgs(key, result.value);
                            RaiseFlushEvent(this, args);
                        }
                    }
                    return false;
                }
                value = result.value;
            }
            else
            {
                return false;
            }

            return true;
        }

        public TValue Get(TKey key)
        {
            TValue value;

            if (!TryGetValue(key, out value))
            {
                if (fetcher != null)
                {
                    value = fetcher(key);
                    Add(key, value);
                }
            }

            return value;
        }

        private void AdjustSize()
        {
            while (cache.Count >= MaximumSize)
            {
                long generationToDelete = Interlocked.Increment(ref generationToFree);
                KeyValuePair<TKey, TimestampedValue> entryToFree =
                    cache.FirstOrDefault(kvp => kvp.Value.generation == generationToDelete);
                if (entryToFree.Key != null)
                {
                    TKey keyToFree = entryToFree.Key;
                    TimestampedValue old;
                    if (cache.TryRemove(keyToFree, out old))
                    {
                        if (RaiseFlushEvent != null)
                        {
                            var args = new FlushEventArgs(keyToFree, old.value);
                            RaiseFlushEvent(this, args);
                        }
                    }
                }
            }
        }

        #region Implementation of IEnumerable

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            return cache.Select(p => new KeyValuePair<TKey, TValue>(p.Key, p.Value.value)).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion
    }
}
