using System;
using System.Threading;

namespace Orleans
{
    /// <summary>
    /// OneTimeSafeTimer is just a convenience class for creating one time safe timers.
    /// No need to manually dispose it. It will self dispose after the first tick.
    /// </summary>
    internal class OneTimeSafeTimer : SafeTimer
    {
        public OneTimeSafeTimer(TimerCallback callback, object state, TimeSpan dueTime) : base(callback, state, dueTime, Constants.INFINITE_TIMESPAN)
        {
        }

        public OneTimeSafeTimer(TimerCallback callback, object state)
            : base(callback, state)
        {
        }

        public void Start(TimeSpan dueTime)
        {
            base.Start(dueTime, Constants.INFINITE_TIMESPAN);
        }

        public new void Start(TimeSpan dueTime, TimeSpan period)
        {
            throw new InvalidOperationException(String.Format("OneTimeSafeTimer {0} does not have ticking period, and thus must be started via public void Start(TimeSpan dueTime)", GetFullName()));
        }
    }
}
