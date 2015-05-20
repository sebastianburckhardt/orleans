using System.Collections.Generic;
using System.Diagnostics;
using System;
using System.Text;

namespace Orleans.Counters
{
    /// <summary>
    /// Abstract class for histgram value statistics, instantiate either HistogramValueStatistic or LinearHistogramValueStatistic
    /// </summary>
    internal abstract class HistogramValueStatistic
    {
        protected Object lockable;
        protected long[] buckets;

        public abstract void AddData(long data);
        public abstract void AddData(TimeSpan data);
        
        protected HistogramValueStatistic(int numBuckets)
        {
            lockable = new object();
            buckets = new long[numBuckets];
        }

        protected string PrintHistogram()
        {
            return PrintHistogram_Impl(false);
        }

        protected string PrintHistogramInMillis()
        {
            return PrintHistogram_Impl(true);
        }

        protected void addToCategory(uint histogramCategory)
        {
            histogramCategory = Math.Min(histogramCategory, (uint)(buckets.Length - 1));
            lock (lockable)
            {
                buckets[histogramCategory]++;
            }
        }

        protected abstract double bucketStart(int i);

        protected abstract double bucketEnd(int i);

        protected string PrintHistogram_Impl(bool InMillis)
        {
            StringBuilder str = new StringBuilder();
            for (int i = 0; i < buckets.Length; i++)
            {
                long bucket = buckets[i];
                if (bucket > 0)
                {
                    double start = bucketStart(i);
                    double end = bucketEnd(i);
                    if (InMillis)
                    {
                        string one =
                            Double.MaxValue == end ?
                                "EOT" :
                                TimeSpan.FromTicks((long) end).TotalMilliseconds.ToString();
                        str.Append(String.Format("[{0}:{1}]={2}, ", TimeSpan.FromTicks((long)start).TotalMilliseconds, one, bucket));
                    }
                    else
                    {
                        str.Append(String.Format("[{0}:{1}]={2}, ", start, end, bucket));
                    }
                }
            }
            return str.ToString();
        }
    }

    /// <summary>
    /// Histogram created where buckets grow exponentially
    /// </summary>
    internal class ExponentialHistogramValueStatistic : HistogramValueStatistic
    {
        private ExponentialHistogramValueStatistic(int numBuckets)
            : base(numBuckets) 
        {
        }

        public static ExponentialHistogramValueStatistic Create_ExponentialHistogram(StatName name, int numBuckets)
        {
            ExponentialHistogramValueStatistic hist = new ExponentialHistogramValueStatistic(numBuckets);
            StringValueStatistic.FindOrCreate(name, hist.PrintHistogram);
            return hist;
        }

        public static ExponentialHistogramValueStatistic Create_ExponentialHistogram_ForTiming(StatName name, int numBuckets)
        {
            ExponentialHistogramValueStatistic hist = new ExponentialHistogramValueStatistic(numBuckets);
            StringValueStatistic.FindOrCreate(name, hist.PrintHistogramInMillis);
            return hist;
        }

        public override void AddData(TimeSpan data)
        {
            uint histogramCategory = (uint)Log2((ulong)data.Ticks);
            addToCategory(histogramCategory);
        }

        public override void AddData(long data)
        {
            uint histogramCategory = (uint)Log2((ulong)data);
            addToCategory(histogramCategory);
        }

        protected override double bucketStart(int i)
        {
            if (i == 0)
            {
                return 0.0;
            }
            return Math.Pow(2, i);
        }

        protected override double bucketEnd(int i)
        {
            if (i == buckets.Length - 1)
            {
                return Double.MaxValue;
            }
            return Math.Pow(2, i + 1) - 1;
        }

        public static string PrintHistogram(IEnumerable<long> inTicks, int numBuckets, bool inMsec = false)
        {
            var hg = new ExponentialHistogramValueStatistic(numBuckets);
            foreach (var i in inTicks)
                hg.AddData(i);
            return hg.PrintHistogram_Impl(inMsec);
        }

        // The log base 2 of an integer is the same as the position of the highest bit set (or most significant bit set, MSB). The following log base 2 methods are faster than this one. 
        // More impl. methods here: http://graphics.stanford.edu/~seander/bithacks.html
        private static uint Log2(ulong number)
        {
            uint r = 0; // r will be log2(number)

            while ((number >>= 1) != 0) // unroll for more speed...
            {
                r++;
            }
            return r;
        }
    }

    /// <summary>
    /// Histogram created where buckets are uniform size
    /// </summary>
    internal class LinearHistogramValueStatistic : HistogramValueStatistic
    {
        private double bucketWidth;

        private LinearHistogramValueStatistic(int numBuckets, double maximumValue)
            : base(numBuckets)
        {
            bucketWidth = maximumValue / numBuckets;
        }

        public static LinearHistogramValueStatistic Create_LinearHistogram(StatName name, int numBuckets, double maximumValue)
        {
            LinearHistogramValueStatistic hist = new LinearHistogramValueStatistic(numBuckets, maximumValue);
            StringValueStatistic.FindOrCreate(name, hist.PrintHistogram);
            return hist;
        }

        public static LinearHistogramValueStatistic Create_LinearHistogram_ForTiming(StatName name, int numBuckets, TimeSpan maximumValue)
        {
            LinearHistogramValueStatistic hist = new LinearHistogramValueStatistic(numBuckets, maximumValue.Ticks);
            StringValueStatistic.FindOrCreate(name, hist.PrintHistogramInMillis);
            return hist;
        }

        public override void AddData(TimeSpan data)
        {
            uint histogramCategory = (uint)(data.Ticks / bucketWidth);
            addToCategory(histogramCategory);
        }

        public override void AddData(long data)
        {
            uint histogramCategory = (uint)(data / bucketWidth);
            addToCategory(histogramCategory);
        }

        protected override double bucketStart(int i)
        {
            return i * bucketWidth;
        }

        protected override double bucketEnd(int i)
        {
            if (i == buckets.Length - 1)
            {
                return Double.MaxValue;
            }
            return (i + 1) * bucketWidth;
        }
    }
}
 
