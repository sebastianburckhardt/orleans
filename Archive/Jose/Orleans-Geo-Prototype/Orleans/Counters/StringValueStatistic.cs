using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Orleans.Counters
{
    class StringValueStatistic : IOrleansCounter<string>
    {
        private static readonly Dictionary<string, StringValueStatistic> registeredStatistics;
        private static readonly object lockable;

        public string Name { get; private set; }
        public CounterStorage Storage { get; private set; }

        private readonly Func<string> fetcher;

        static StringValueStatistic()
        {
            registeredStatistics = new Dictionary<string, StringValueStatistic>();
            lockable = new object();
        }

        // Must be called while Lockable is locked
        private StringValueStatistic(string n, Func<string> f)
        {
            Name = n;
            fetcher = f;
        }

        static public StringValueStatistic Find(StatName name)
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

        static public StringValueStatistic FindOrCreate(StatName name, Func<string> f, CounterStorage storage = CounterStorage.LogOnly)
        {
            lock (lockable)
            {
                StringValueStatistic stat;
                if (registeredStatistics.TryGetValue(name.Name, out stat))
                {
                    return stat;
                }
                var ctr = new StringValueStatistic(name.Name, f) { Storage = storage };
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

        /// <summary>
        /// Returns the current value
        /// </summary>
        /// <returns></returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public string GetCurrentValue()
        {
            try
            {
                return fetcher();
            }
            catch (Exception)
            {
                return "";
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
            return GetCurrentValue();
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
