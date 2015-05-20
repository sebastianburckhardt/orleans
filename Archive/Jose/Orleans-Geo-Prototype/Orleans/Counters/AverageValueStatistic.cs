#define COLLECT_AVERAGE
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Orleans.Counters
{
    class AverageValueStatistic
    {
#if COLLECT_AVERAGE
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1823:AvoidUnusedPrivateFields")]
        private FloatValueStatistic Average;
#endif
        public string Name { get; private set; }

        //static public AverageValueStatistic FindOrCreate(StatName name, bool multiThreaded, CounterStorage storage = CounterStorage.LogOnly)
        //{
        //    return FindOrCreate_Impl(name, multiThreaded, storage);
        //}

        static public AverageValueStatistic FindOrCreate(StatName name, CounterStorage storage = CounterStorage.LogOnly)
        {
            return FindOrCreate_Impl(name, true, storage);
        }

        static private AverageValueStatistic FindOrCreate_Impl(StatName name, bool multiThreaded, CounterStorage storage)
        {
            AverageValueStatistic stat;
#if COLLECT_AVERAGE
            if (multiThreaded)
            {
                stat = new MultiThreadedAverageValueStatistic(name);
            }
            else
            {
                stat = new SingleThreadedAverageValueStatistic(name);
            }
            stat.Average = FloatValueStatistic.FindOrCreate(name,
                      () =>
                      {
                          return stat.GetAverageValue();
                      }, storage);
#else
            stat = new AverageValueStatistic(name);
#endif
            
            return stat;
        }

        protected AverageValueStatistic(StatName name)
        {
            Name = name.Name;
        }

        public virtual void AddValue(long value) { }

        public virtual float GetAverageValue() { return 0; }

        public override string ToString()
        {
            return Name;
        }
    }

    internal class MultiThreadedAverageValueStatistic : AverageValueStatistic
    {
        private CounterStatistic TotalSum;
        private CounterStatistic NumItems;

        internal MultiThreadedAverageValueStatistic(StatName name)
            : base(name)
        {
            TotalSum = CounterStatistic.FindOrCreate(new StatName(String.Format("{0}.{1}", name.Name, "TotalSum.Hidden")), CounterStorage.DontStore);
            NumItems = CounterStatistic.FindOrCreate(new StatName(String.Format("{0}.{1}", name.Name, "NumItems.Hidden")), CounterStorage.DontStore);
        }

        public override void AddValue(long value)
        {
            TotalSum.IncrementBy(value);
            NumItems.Increment();
        }

        public override float GetAverageValue()
        {
            long numItems = NumItems.GetCurrentValue();
            if (numItems == 0) return 0;
            long totalSum = TotalSum.GetCurrentValue();
            return (float)totalSum / (float)numItems;
        }
    }

    // An optimized implementation to be used in a single threaded mode (not thread safe).
    internal class SingleThreadedAverageValueStatistic : AverageValueStatistic
    {
        private long TotalSum;
        private long NumItems;

        internal SingleThreadedAverageValueStatistic(StatName name)
            : base(name)
        {
            TotalSum = 0;
            NumItems = 0;
        }

        public override void AddValue(long value)
        {
            long oldTotal = TotalSum;
            TotalSum = (oldTotal + value);
            NumItems = NumItems + 1;
        }

        public override float GetAverageValue()
        {
            long numItems = NumItems;
            if (numItems == 0) return 0;
            long totalSum = TotalSum;
            return (float)totalSum / (float)numItems;
        }
    }
}
