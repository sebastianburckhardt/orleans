using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Orleans.Counters
{
    internal class FloatValueStatistic : IOrleansCounter<float>
    {
        private static readonly Dictionary<string, FloatValueStatistic> registeredStatistics;
        private static readonly object lockable;

        public string Name { get; private set; }
        public CounterStorage Storage { get; private set; }

        private readonly Func<float> fetcher;

        static FloatValueStatistic()
        {
            registeredStatistics = new Dictionary<string, FloatValueStatistic>();
            lockable = new object();
        }

        // Must be called while Lockable is locked
        private FloatValueStatistic(string n, Func<float> f)
        {
            Name = n;
            fetcher = f;
        }

        public static FloatValueStatistic Find(StatName name)
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

        static public FloatValueStatistic FindOrCreate(StatName name, Func<float> f, CounterStorage storage = CounterStorage.LogOnly)
        {
            lock (lockable)
            {
                FloatValueStatistic stat;
                if (registeredStatistics.TryGetValue(name.Name, out stat))
                {
                    return stat;
                }
                var ctr = new FloatValueStatistic(name.Name, f) { Storage = storage };
                registeredStatistics[name.Name] = ctr;
                return ctr;
            }
        }

        static public FloatValueStatistic CreateDoNotRegister(string name, Func<float> f)
        {
            return new FloatValueStatistic(name, f) { Storage = CounterStorage.DontStore };
        }

        /// <summary>
        /// Returns the current value
        /// </summary>
        /// <returns></returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public float GetCurrentValue()
        {
            try
            {
                return fetcher();
            }
            catch (Exception)
            {
                return default(float);
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
            float current = GetCurrentValue();
            return current.ToString();
        }

        public string GetDisplayString()
        {
            return ToString();
        }

        public override string ToString()
        {
            return String.Format("{0}={1:0.000}", Name, GetCurrentValue());
        }
    }
}
