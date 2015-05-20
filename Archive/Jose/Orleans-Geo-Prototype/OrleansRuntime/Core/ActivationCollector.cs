using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;

namespace Orleans.Runtime.Core
{
    /// <summary>
    /// Identifies activations that have been idle long enough to be deactivated.
    /// <see cref="onenote:///\\research\root\orleans\Orleans%20Discussions\Design.one#Algorithm%203&section-id={3F544F66-AAE7-4EA7-84AD-90AB7498ADF0}&page-id={2443B5EE-A93E-4083-A8F6-2977F6022141}&end"/> 
    /// </summary>
    internal class ActivationCollector : IActivationCollector
    {
        private readonly TimeSpan _quantum;
        private readonly ConcurrentDictionary<DateTime, Bucket> _buckets;
        // the next ticket cannot be stored as a DateTime due to incompatibilies between DateTime and Intelocked.Add.
        private readonly object _nextTicketLock;
        private DateTime _nextTicket;
        private static readonly List<ActivationData> NOTHING = new List<ActivationData> { Capacity = 0 };
        private int _count;

        public ActivationCollector(TimeSpan quantum)
        {
            if (TimeSpan.Zero == quantum)
            {
                throw new ArgumentException("The quantum cannot be zero.", "quantum");
            }

            _quantum = quantum;
            _buckets = new ConcurrentDictionary<DateTime, Bucket>();
            _nextTicket = MakeTicketFromDateTime(DateTime.UtcNow);
            _nextTicketLock = new object();
            _count = 0;
        }

        public TimeSpan Quantum { get { return _quantum; } }

        public int Count { get { return _count; } }

        public void ScheduleCollection(ActivationData item, TimeSpan timeout)
        {
            ThrowIfExemptFromCollection(item, "item");
            if (TimeSpan.Zero == timeout)
            {
                // either the CollectionAgeLimit hasn't been initialized (will be rectified later) or it's been disabled.
                return;
            }

            DateTime ticket = MakeTicketFromTimeSpan(timeout);
            lock (item)
            {
                if (default(DateTime) != item.CollectionTicket)
                {
                    throw new InvalidOperationException("call CancelCollection before calling ScheduleCollection.");
                }

                Add(item, ticket);
            }
        }
        
        public bool TryCancelCollection(ActivationData item)
        {
            if (IsExemptFromCollection(item))
            {
                return false;
            }

            lock (item)
            {
                DateTime ticket = item.CollectionTicket;
                if (default(DateTime) == ticket)
                {
                    return false;
                }

                if (IsExpired(ticket))
                {
                    return false;
                }

                // first, we attempt to remove the ticket. 
                Bucket bucket;
                if (!_buckets.TryGetValue(ticket, out bucket) || !bucket.TryRemove(item))
                {
                    return false;
                }
            }

            Interlocked.Decrement(ref _count);
            return true;
        }

        public bool TryRescheduleCollection(ActivationData item, TimeSpan timeout)
        {
            if (item.ActivationCollector == null)
            {
                return false;
            }

            lock (item)
            {
                if (TryRescheduleCollection_Impl(item, timeout))
                {
                    return true;
                }
                item.ResetCollectionTicket();
                return false;
            }
        }

        private bool TryRescheduleCollection_Impl(ActivationData item, TimeSpan timeout)
        {
            // note: we expect the activation lock to be held.

            if (default(DateTime) == item.CollectionTicket)
            {
                return false;
            }

            ThrowIfTicketIsInvalid(item.CollectionTicket);
            if (IsExpired(item.CollectionTicket))
            {
                return false;
            }

            DateTime oldTicket = item.CollectionTicket;
            DateTime newTicket = MakeTicketFromTimeSpan(timeout);
            // if the ticket value doesn't change, then the source and destination bucket are the same and there's nothing to do.
            if (newTicket.Equals(oldTicket))
            {
                return true;
            }

            Bucket bucket;
            if (!_buckets.TryGetValue(oldTicket, out bucket) || !bucket.TryRemove(item))
            {
                // fail: item is not associated with currentKey.
                return false;
            }

            Interlocked.Decrement(ref _count);
            // it shouldn't be possible for Add to throw an exception here, as only one concurrent competitor should be able to reach to this point in the method.
            item.ResetCollectionTicket();
            Add(item, newTicket);
            return true;
        }

        private bool DequeueQuantum(out IEnumerable<ActivationData> items, DateTime now)
        {
            DateTime key;
            lock (_nextTicketLock)
            {
                if (_nextTicket > now)
                {
                    items = null;
                    return false;
                }

                key = _nextTicket;
                _nextTicket += _quantum;
            }

            Bucket bucket;
            if (!_buckets.TryRemove(key, out bucket))
            {
                items = NOTHING;
                return true;
            }

            items = bucket.CancelAll();
            return true;
        }

        public override string ToString()
        {
            DateTime now = DateTime.UtcNow;
            return string.Format("<#Activations={0}, #Buckets={1}, buckets={2}>", 
                    Count, 
                    _buckets.Count,       
                    Utils.IEnumerableToString(
                        _buckets.Values.OrderBy(bucket => bucket.Key), bucket => (Utils.TimeSpanToString(bucket.Key - now) + "->" + bucket.Count + " items").ToString(CultureInfo.InvariantCulture)));
        }

        /// <summary>
        /// Scans for activations that are due for collection.
        /// </summary>
        /// <returns>A list of activations that are due for collection.</returns>
        public List<ActivationData> ScanStale()
        {
            DateTime now = DateTime.UtcNow;
            List<ActivationData> result = null;
            IEnumerable<ActivationData> activations;
            while (DequeueQuantum(out activations, now))
            {
                // at this point, all tickets associated with activations are cancelled and any attempts to reschedule will fail silently. if the activation is to be reactivated, it's our job to clear the activation's copy of the ticket.
                foreach (var activation in activations)
                {
                    // it's possible for the activation to no longer be a candidate for collection. this could occur if the sctivation has been used in between the time it was removed from the collector's internal structures and now. in the future, we'll defer this logic to a queue consumer on a dedicated system target but for now, we'll throw activations back into the collector that aren't ready.
                    lock (activation)
                    {
                        activation.ResetCollectionTicket();
                        if (activation.IsCollectionCandidate && activation.IsStale(now))
                        {
                            if (null == result)
                            {
                                result = new List<ActivationData> { activation };
                            }
                            else
                            {
                                result.Add(activation);
                            }
                        }
                        else
                        {
                            ScheduleCollection(activation, activation.CollectionAgeLimit);
                        }
                    }
                }
            }
            return result ?? NOTHING;
        }

        /// <summary>
        /// Scans for activations that have been idle for the specified age limit.
        /// </summary>
        /// <param name="ageLimit">The age limit.</param>
        /// <returns></returns>
        public List<ActivationData> ScanAll(TimeSpan ageLimit)
        {
            List<ActivationData> result = null;
            DateTime now = DateTime.UtcNow;
            int bucketCount = _buckets.Count;
            int i = 0;
            foreach (var bucket in _buckets.Values)
            {
                // theoretically, we could iterate forever on the ConcurrentDictionary. we limit ourselves to a snapshot of the dictionary's Count property to limit the number of iterations we perform.
                if (i >= bucketCount)
                {
                    break;
                }

                int activationCount = bucket.Count; 
                int j = 0;
                foreach (var activation in bucket)
                {
                // theoretically, we could iterate forever on the ConcurrentDictionary. we limit ourselves to a snapshot of the bucket's Count property to limit the number of iterations we perform.
                    if (j >= activationCount)
                    {
                        break;
                    }

                    lock (activation)
                    {
                        if (activation.IsCollectionCandidate && activation.GetIdleness(now) >= ageLimit && bucket.TryCancel(activation))
                        {
                            if (null == result)
                            {
                                result = new List<ActivationData> { activation };
                            }
                            else
                            {
                                result.Add(activation);
                            }
                        }
                    }
                    ++j;
                }
                ++i;
            }
            return result ?? NOTHING;
        }

        private static void ThrowIfTicketIsInvalid(DateTime ticket, TimeSpan quantum)
        {
            ThrowIfDefault(ticket, "ticket");
            if (0 != ticket.Ticks % quantum.Ticks)
            {
                throw new ArgumentException(string.Format("invalid ticket ({0})", ticket));
            }
        }

        private void ThrowIfTicketIsInvalid(DateTime ticket)
        {
            ThrowIfTicketIsInvalid(ticket, _quantum);
        }

        internal static bool IsExemptFromCollection(ActivationData activation)
        {
            return activation.Grain.IsSystemTarget || Constants.IsSystemGrain(activation.Grain);
        }

        private void ThrowIfExemptFromCollection(ActivationData activation, string name)
        {
            if (IsExemptFromCollection(activation))
            {
                throw new ArgumentException(string.Format("{0} should not refer to a system target or system grain.", name), name);
            }
        }

        private bool IsExpired(DateTime ticket)
        {
            return ticket < _nextTicket;
        }

        private DateTime MakeTicketFromDateTime(DateTime timestamp)
        {
            // round the timestamp to the next quantum. e.g. if the quantum is 1 minute and the timestamp is 3:45:22, then the ticket will be 3:46. note that TimeStamp.Ticks and DateTime.Ticks both return a long.
            DateTime ticket = new DateTime(((timestamp.Ticks - 1) / _quantum.Ticks + 1) * _quantum.Ticks);
            if (ticket < _nextTicket)
            {
                throw new ArgumentException(string.Format("The earliest collection that can be scheduled from now is for {0}", new DateTime(_nextTicket.Ticks - _quantum.Ticks + 1)));
            }
            return ticket;
        }

        private DateTime MakeTicketFromTimeSpan(TimeSpan timeout)
        {
            if (timeout < _quantum)
            {
                throw new ArgumentException(string.Format("timeout must be at least {0}", _quantum));
            }

            return MakeTicketFromDateTime(DateTime.UtcNow + timeout);
        }

        private void Add(ActivationData item, DateTime ticket)
        {
            // note: we expect the activation lock to be held.

            item.ResetCollectionCancelledFlag();
            Bucket bucket = 
                _buckets.GetOrAdd(
                    ticket, 
                    key => 
                        new Bucket(key, _quantum));
            bucket.Add(item);
            Interlocked.Increment(ref _count);
            item.SetCollectionTicket(ticket);
        }

        static private void ThrowIfDefault<T>(T value, string name) where T : IEquatable<T>
        {
            if (value.Equals(default(T)))
            {
                throw new ArgumentException(string.Format("default({0}) is not allowed in this context.", typeof(T).Name), name);
            }
        }
        
        private class Bucket : IEnumerable<ActivationData>
        {
            private readonly DateTime _key;
            private readonly ConcurrentDictionary<ActivationId, ActivationData> _items;

            public DateTime Key { get { return _key; } }
            public int Count { get {  return _items.Count; } }

            public Bucket(DateTime key, TimeSpan quantum)
            {
                ThrowIfTicketIsInvalid(key, quantum);
                _key = key;
                _items = new ConcurrentDictionary<ActivationId, ActivationData>();
            }

            public void Add(ActivationData item)
            {
                if (!_items.TryAdd(item.ActivationId, item))
                {
                    throw new InvalidOperationException("item is already associated with this bucket");
                }
            }

            public bool TryRemove(ActivationData item)
            {
                if (!TryCancel(item))
                {
                    return false;
                }

                // actual removal is a memory optimization and isn't technically necessary to cancel the timeout.
                ActivationData unused;
                return _items.TryRemove(item.ActivationId, out unused);
            }

            public bool TryCancel(ActivationData item)
            {
                if (item.TrySetCollectionCancelledFlag())
                {
                    // we need to null out the ActivationData reference in the bucket in order to ensure that the memory gets collected. if we've succeeded in setting the cancellation flag, then we should have won the right to do this, so we throw an exception if we fail.
                    if (_items.TryUpdate(item.ActivationId, null, item))
                    {
                        return true;
                    }
                    else
                    {
                        throw new InvalidOperationException("unexpected failure to cancel deactivation");
                    }
                }
                else
                {
                    return false;
                }
            }

            public IEnumerable<ActivationData> CancelAll()
            {
                List<ActivationData> result = null;
                int i = 0;
                foreach (var pair in _items)
                {
                    // attempt to cancel the item. if we succeed, it wasn't already cancelled and we can return it. otherwise, we silently ignore it.
                    if (pair.Value.TrySetCollectionCancelledFlag())
                    {
                        if (result == null)
                        {
                            // we only need to ensure there's enough space left for this element and any potential entries.
                            result = new List<ActivationData>();
                        }
                        result.Add(pair.Value);
                    }
                    ++i;
                }

                return result ?? NOTHING;
            }

            public IEnumerator<ActivationData> GetEnumerator()
            {
                return _items.Values.GetEnumerator();
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }
        }
    }
}
