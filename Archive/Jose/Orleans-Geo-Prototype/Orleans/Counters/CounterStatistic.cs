using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Orleans.Counters
{
    internal enum CounterStorage
    {
        DontStore,
        LogOnly,
        LogAndTable
    }

    internal interface IOrleansCounter
    {
        string Name { get; }
        bool IsValueDelta { get; }
        string GetValueString();
        string GetDisplayString();
        CounterStorage Storage { get; }
    }

    internal interface IOrleansCounter<out T> : IOrleansCounter
    {
        T GetCurrentValue();
    }

    internal class CounterStatistic : IOrleansCounter<long>
    {
        [ThreadStatic]
        private static List<long> perOrleansThreadCounters;
        [ThreadStatic]
        private static bool isOrleansManagedThread;

        private static readonly Dictionary<string, CounterStatistic> registeredStatistics;
        private static readonly object lockable;
        private static int nextId;
        private static readonly HashSet<List<long>> allThreadCounters;
        
        private readonly int id;
        private long last;
        private bool firstStatDisplay;
        private Func<long, long> ValueConverter;
        private long nonOrleansThreadsCounter; // one for all non-Orleans threads

        public string Name { get; private set; }
        public bool UseDelta { get; private set; }
        public CounterStorage Storage { get; private set; }

        static CounterStatistic()
        {
            registeredStatistics = new Dictionary<string, CounterStatistic>();
            allThreadCounters = new HashSet<List<long>>();
            nextId = 0;
            lockable = new object();
        }

        // Must be called while Lockable is locked
        private CounterStatistic(string name, bool useDelta, CounterStorage storage)
        {
            Name = name;
            UseDelta = useDelta;
            Storage = storage;
            id = Interlocked.Increment(ref nextId);
            last = 0;
            firstStatDisplay = true;
            ValueConverter = null;
            nonOrleansThreadsCounter = 0;
        }

        internal static void SetOrleansManagedThread()
        {
            if (!isOrleansManagedThread)
            {
                lock (lockable)
                {
                    isOrleansManagedThread = true;
                    perOrleansThreadCounters = new List<long>();
                    allThreadCounters.Add(perOrleansThreadCounters);
                }
            }
        }

        public static CounterStatistic FindOrCreate(StatName name)
        {
            return FindOrCreate_Impl(name, true, CounterStorage.LogAndTable);
        }

        public static CounterStatistic FindOrCreate(StatName name, bool useDelta)
        {
            return FindOrCreate_Impl(name, useDelta, CounterStorage.LogAndTable);
        }

        public static CounterStatistic FindOrCreate(StatName name, CounterStorage storage)
        {
            return FindOrCreate_Impl(name, true, storage);
        }

        public static CounterStatistic FindOrCreate(StatName name, bool useDelta, CounterStorage storage)
        {
            return FindOrCreate_Impl(name, useDelta, storage);
        }

        private static CounterStatistic FindOrCreate_Impl(StatName name, bool useDelta, CounterStorage storage)
        {
            CounterStatistic stat;
            lock (lockable)
            {
                if (registeredStatistics.TryGetValue(name.Name, out stat))
                {
                    return stat;
                }
                var ctr = new CounterStatistic(name.Name, useDelta, storage);
                registeredStatistics[name.Name] = ctr;
                return ctr;
            }
        }

        static public bool Delete(string name)
        {
            lock (lockable)
            {
                return registeredStatistics.Remove(name);
            }
        }

        public CounterStatistic AddValueConverter(Func<long, long> valueConverter)
        {
            ValueConverter = valueConverter;
            return this;
        }

        public void Increment()
        {
            IncrementBy(1);
        }

        public void DecrementBy(long n)
        {
            IncrementBy(-n);
        }

        // Orleans-managed threads aggregate stats in per thread local storage list.
        // For non Orleans-managed threads (.NET IO completion port threads, thread pool timer threads) we don't want to allocate a thread local storage,
        // since we don't control how many of those threads are created (could lead to too much thread local storage allocated).
        // Thus, for non Orleans-managed threads, we use a counter shared between all those threads and Interlocked.Add it (creating small contention).
        public void IncrementBy(long n)
        {
            if (isOrleansManagedThread)
            {
                while (perOrleansThreadCounters.Count <= id)
                {
                    perOrleansThreadCounters.Add(0);
                }
                perOrleansThreadCounters[id] = perOrleansThreadCounters[id] + n;
            }
            else
            {
                if (n == 1)
                {
                    Interlocked.Increment(ref nonOrleansThreadsCounter);
                }
                else
                {
                    Interlocked.Add(ref nonOrleansThreadsCounter, n);
                }
            }
        }

        /// <summary>
        /// Returns the current value
        /// </summary>
        /// <returns></returns>
        public long GetCurrentValue()
        {
            List<List<long>> lists;
            lock (lockable)
            {
                lists = allThreadCounters.ToList();
            }
            // Where(list => list.Count > id) takes only list from threads that actualy have value for this counter. 
            // The whole way we store counters is very ineffecient and better be re-written.
            long val = Interlocked.Read(ref nonOrleansThreadsCounter);
            foreach(var list in lists.Where(list => list.Count > id))
            {
                val += list[id];
            }
            return val;
            // return lists.Where(list => list.Count > id).Aggregate<List<long>, long>(0, (current, list) => current + list[id]) + nonOrleansThreadsCounter;
        }

        /// <summary>
        /// Returns the current value and the delta since the last call to this method.
        /// Note: This call also resets the delta by remembering the 'current' value as the new 'last' value.
        /// </summary>
        /// <returns></returns>
        public long GetCurrentValueAndDeltaAndResetDelta(out long delta)
        {
            var currentValue = GetCurrentValue();
            delta = UseDelta ? (currentValue - last) : 0;
            last = currentValue;
            return currentValue;
        }

        // does not reset delta
        public long GetCurrentValueAndDelta(out long delta)
        {
            var currentValue = GetCurrentValue();
            delta = UseDelta ? (currentValue - last) : 0;
            return currentValue;
        }

        public bool IsValueDelta { get { return UseDelta; } }

        public string GetValueString()
        {
            long current = GetCurrentValue();
            long delta = UseDelta ? (current - last) : 0;

            if (ValueConverter != null)
            {
                try
                {
                    current = ValueConverter(current);
                }
                catch (Exception) { }
                try
                {
                    delta = ValueConverter(delta);
                }
                catch (Exception) { }
            }

            return UseDelta ? delta.ToString() : current.ToString();
        }

        private string GetDisplayString(bool resetLastValue)
        {
            long current;
            long delta;
            
            if (resetLastValue)
            {
                current = GetCurrentValueAndDeltaAndResetDelta(out delta);
            }
            else
            {
                current = GetCurrentValue();
                delta = UseDelta ? (current - last) : 0;
            }

            if (firstStatDisplay)
            {
                delta = 0; // Special case: don't output first delta
                firstStatDisplay = false;
            }

            if (ValueConverter != null)
            {
                try
                {
                    current = ValueConverter(current);
                }
                catch (Exception) { }
                try
                {
                    delta = ValueConverter(delta);
                }
                catch (Exception) { }
            }

            if (delta == 0)
            {
                return String.Format("{0}.Current={1}", Name, current);
            }
            else
            {
                return String.Format("{0}.Current={1},      Delta={2}", Name, current, delta);
            }
        }

        public override string ToString()
        {
            return GetDisplayString(false);
        }

        public string GetDisplayString()
        {
            return GetDisplayString(true);
        }

        //public static List<IOrleansCounter> GetValues()
        //{
        //    List<IOrleansCounter> list;
        //    lock (lockable)
        //    {
        //        list = registeredStatistics.Values.Cast<IOrleansCounter>().ToList();
        //    }
        //    return list;
        //}

        public static void AddCounters(List<IOrleansCounter> list, Func<IOrleansCounter, bool> predicate)
        {
            lock (lockable)
            {
                list.AddRange(registeredStatistics.Values.Where(predicate));
            }
        }
    }
}
