using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Orleans.Counters
{
    class IntValueStatistic : IOrleansCounter<long>
    {
        private static readonly Dictionary<string, IntValueStatistic> registeredStatistics;
        private static readonly object lockable;

        public string Name { get; private set; }
        public CounterStorage Storage { get; private set; }

        private readonly Func<long> fetcher;

        static IntValueStatistic()
        {
            registeredStatistics = new Dictionary<string, IntValueStatistic>();
            lockable = new object();
        }

        // Must be called while Lockable is locked
        private IntValueStatistic(string n, Func<long> f)
        {
            Name = n;
            fetcher = f;
        }

        static public IntValueStatistic Find(StatName name)
        {
            lock (lockable)
            {
                if (registeredStatistics.ContainsKey(name.Name))
                {
                    return registeredStatistics[name.Name];
                }
                else
                {
                    return null;
                }
            }
        }

        static public IntValueStatistic FindOrCreate(StatName name, Func<long> f, CounterStorage storage = CounterStorage.LogOnly)
        {
            lock (lockable)
            {
                IntValueStatistic stat;
                if (registeredStatistics.TryGetValue(name.Name, out stat))
                {
                    return stat;
                }
                var ctr = new IntValueStatistic(name.Name, f) { Storage = storage };
                registeredStatistics[name.Name] = ctr;
                return ctr;
            }
        }

        /// <summary>
        /// Returns the current value
        /// </summary>
        /// <returns></returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public long GetCurrentValue()
        {
            try
            {
                return fetcher();
            }
            catch (Exception)
            {
                return default(long);
            }
        }

        public static void AddCounters(List<IOrleansCounter> list, Func<IOrleansCounter, bool> predicate)
        {
            lock (lockable)
            {
                list.AddRange(registeredStatistics.Values.Where(predicate));
            }
        }

        public bool IsValueDelta { get { return false; } }

        public string GetValueString()
        {
            long current = GetCurrentValue();
            return current.ToString();
        }

        public string GetDisplayString()
        {
            return ToString();
        }

        public override string ToString()
        {
            return Name + "=" + GetCurrentValue();
        }
    }
}
