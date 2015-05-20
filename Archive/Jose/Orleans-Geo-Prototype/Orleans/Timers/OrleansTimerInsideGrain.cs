using System;
using System.Threading;
using System.Threading.Tasks;
using Orleans.Counters;


namespace Orleans
{
    internal class OrleansTimerInsideGrain : IOrleansTimer
    {
        [Flags]
        public enum OptionFlags
        {
            None = 0x0,
            CountTicks = 0x1,
        };

        private Func<object, Task> asyncCallback;
        private AsyncSafeTimer timer;
        private readonly TimeSpan dueTime;
        private readonly TimeSpan timerFrequency;
        private DateTime previousTickTime;
        private int totalNumTicks;
        private readonly string Name;
        private readonly CounterStatistic ticksCounter;
        private static readonly Logger logger = Logger.GetLogger("OrleansTimerInsideGrain", Logger.LoggerType.Runtime);
        private OptionFlags options;

        private bool CountTicksOption
        {
            get { return (options & OptionFlags.CountTicks) == OptionFlags.CountTicks; }
        }

        private OrleansTimerInsideGrain(Func<object, Task> asyncCallback, object state, TimeSpan dueTime, TimeSpan period, ISchedulingContext context, string name, OptionFlags options)
        {
            var ctxt = context ?? RuntimeContext.Current.ActivationContext;
            this.Name = name;
            this.options = options;
            if (CountTicksOption)
                this.ticksCounter = CounterStatistic.FindOrCreate(new StatName(StatNames.STAT_ORLEANS_TIMER_NUM_TICKS_PERTIMER, Name));
            this.asyncCallback = asyncCallback;
            this.timer = new AsyncSafeTimer(
                    stateObj => AsyncCompletion.FromTask(
                        GrainClient.InternalCurrent.ExecAsync(
                            () => ForwardToAsyncCallback(stateObj), 
                            ctxt)),
                    state);
            this.dueTime = dueTime;
            this.timerFrequency = period;
            this.previousTickTime = DateTime.UtcNow;
            this.totalNumTicks = 0;
        }

        internal static OrleansTimerInsideGrain
            FromTimerCallback(
                TimerCallback callback,
                object state,
                TimeSpan dueTime,
                TimeSpan period,
                ISchedulingContext context = null,
                string name = null,
                OptionFlags options = OptionFlags.None)
        {
            return
                new OrleansTimerInsideGrain(
                    ob =>
                        {
                            if (callback != null)
                                callback(ob);
                            return TaskDone.Done;
                        },
                    state,
                    dueTime,
                    period,
                    context,
                    name,
                    options);
        }

        internal static OrleansTimerInsideGrain 
            FromTaskCallback(
                Func<object, Task> asyncCallback,
                object state,
                TimeSpan dueTime,
                TimeSpan period,
                ISchedulingContext context = null,
                string name = null,
                OptionFlags options = OptionFlags.None)
        {
            return
                new OrleansTimerInsideGrain(
                    asyncCallback,
                    state,
                    dueTime,
                    period,
                    context,
                    name,
                    options);
        }

        public void Start()
        {
            this.timer.Start(dueTime, timerFrequency);
        }

        private async Task ForwardToAsyncCallback(object state)
        {
            // [mlr] AsyncSafeTimer ensures that calls to this method are serialized.
            if (timer == null || asyncCallback == null)
                return;
            
            totalNumTicks++;
            if (ticksCounter != null)
                ticksCounter.Increment();

            if (logger.IsVerbose3)
                logger.Verbose3(
                    ErrorCode.TimerBeforeCallback,
                    "About to make timer callback for timer {0}",
                    GetFullName());

            try
            { 
                await asyncCallback(state);
                previousTickTime = DateTime.UtcNow;

                if (logger.IsVerbose3)
                    logger.Verbose3(
                        ErrorCode.TimerAfterCallback,
                        "Completed timer callback for timer {0}",
                        GetFullName());
            }
            catch (Exception exc)
            {
                logger.Error(
                    ErrorCode.Timer_GrainTimerCallbackError,
                    string.Format(
                        "Caught and ignored exception {0} thrown from timer callback {1}",
                        exc.Message,
                        GetFullName()),
                    exc);       
            }
            finally
            {
                // [mlr] if this is not a repeating timer, then we can
                // dispose of the timer.
                if (timerFrequency == Constants.INFINITE_TIMESPAN)
                    DisposeTimer();                
            }
        }

        private string GetFullName()
        {
            return String.Format("OrleansTimerInsideGrain.{0}. TimerCallbackHandler:{1}->{2}",
                                                                    Name == null ? "" : Name,
                                                                    (asyncCallback != null && asyncCallback.Target != null) ? asyncCallback.Target.ToString() : "",
                                                                    (asyncCallback != null && asyncCallback.Method != null) ? asyncCallback.Method.ToString() : "");
        }

        internal string GetName()
        {
            return Name;
        }

        internal int GetNumTicks()
        {
            return totalNumTicks;
        }

        // The reason we need to check CheckTimerFreeze on both the SafeTimer and this OrleansTimerInsideGrain
        // is that SafeTimer may tick OK (no starvation by .NET thread pool), but then scheduler.QueueWorkItem
        // may not execute and starve this OrleansTimerInsideGrain callback.
        internal bool CheckTimerFreeze(DateTime lastCheckTime)
        {
            // check underlying SafeTimer (checking that .NET thread pool does not starve this timer)
            bool ok = timer.CheckTimerFreeze(lastCheckTime, GetName);
            if (!ok)
            {
                return ok; // if SafeTimer failed the check, no need to check OrleansTimerInsideGrain too, since it will fail as well.
            }
            // check myself (checking that scheduler.QueueWorkItem does not starve this timer)
            return SafeTimerBase.CheckTimerDelay(previousTickTime, totalNumTicks,
                        dueTime, timerFrequency, logger, GetFullName, ErrorCode.Timer_TimerInsideGrainIsNotTicking, true);
        }

        internal bool CheckTimerDelay()
        {
            return SafeTimerBase.CheckTimerDelay(previousTickTime, totalNumTicks,
                        dueTime, timerFrequency, logger, GetFullName, ErrorCode.Timer_TimerInsideGrainIsNotTicking, false);
        }

        #region IDisposable Members

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        // Maybe called by finalizer thread with disposing=false. As per guidelines, in such a case do not touch other objects.
        // Dispose() may be called multiple times
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                DisposeTimer();
            }
            this.asyncCallback = null;
        }

        private void DisposeTimer()
        {
            if (this.timer != null)
            {
                this.timer.Dispose();
                this.timer = null;
            }
        }

        #endregion
    }
}
