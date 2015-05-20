using System;
using System.Collections.Generic;


namespace Orleans
{
    /// <summary>
    /// This class is for internal use by the Orleans run-time.
    /// </summary>
    internal class CoarseGrainTimerWheel : IDisposable
    {
        private readonly List<Timeoutable>[] buckets;
        private TimeSpan period;
        private int nextBucketToTick;
        private SafeTimer timer;

        /// <summary>
        /// For internal use by the Orleans run-time.
        /// </summary>
        /// <param name="numPeriods"></param>
        /// <param name="maxTimeout"></param>
        public CoarseGrainTimerWheel(int numPeriods, TimeSpan maxTimeout)
        {
            this.period = maxTimeout.Divide(numPeriods);
            this.nextBucketToTick = 0;
            this.buckets = new List<Timeoutable>[numPeriods];
            for (int i = 0; i < buckets.Length; i++)
            {
                buckets[i] = new List<Timeoutable>();
            }
            this.timer = new SafeTimer(TimeoutCallback, null, period, period);
        }

        /// <summary>
        /// For internal use by the Orleans run-time.
        /// </summary>
        /// <param name="target"></param>
        /// <param name="timeout"></param>
        public void ScheduleForTimeout(Timeoutable target, TimeSpan timeout)
        {
            if (timeout < TimeSpan.Zero)
                throw new ArgumentOutOfRangeException("timeout", "The timeout parameter is negative.");
            TimeSpan maxTimeout = period.Multiply(buckets.Length);
            if (timeout > maxTimeout)
                throw new ArgumentOutOfRangeException("timeout", "The timeout parameter is too big. CoarseGrainTimerWheel only supports timeouts up to " + maxTimeout);

            int numBucketsToWait = (int)(timeout.Divide(period)) + 1;
            int myBucket = (nextBucketToTick + numBucketsToWait) % buckets.Length;
            buckets[myBucket].Add(target);
        }

        private void TimeoutCallback(object obj)
        {
            List<Timeoutable> timed = buckets[nextBucketToTick];
            for (int i = 0; i < timed.Count; i++)
            {
                timed[i].OnTimeout();
            }
            buckets[nextBucketToTick].Clear();
            nextBucketToTick = (nextBucketToTick + 1)%buckets.Length;
        }

        public void Dispose()
        {
            if (timer != null)
            {
                timer.Dispose();
                timer = null;
            }

            GC.SuppressFinalize(this);
        }
    }
}
