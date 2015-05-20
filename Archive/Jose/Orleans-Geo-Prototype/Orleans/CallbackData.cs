using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Orleans.Counters;

namespace Orleans
{
    /// <summary>
    /// This interface is for use with the Orleans timers.
    /// See <see cref="Orleans.CoarseGrainTimerWheel"/>.
    /// </summary>
    internal interface Timeoutable
    {
        /// <summary>
        /// This method is called by the timer when the time out is reached.
        /// </summary>
        void OnTimeout();
        /// <summary>
        /// This method is not used.
        /// </summary>
        /// <returns>Not used.</returns>
        TimeSpan RequestedTimeout();
    }

    internal class CallbackData : Timeoutable, IDisposable
    {
        private readonly Action<Message, TaskCompletionSource<object>> callback;
        private readonly Func<Message, bool> resend;
        private readonly Action unregister;
        private readonly TaskCompletionSource<object> context;
        public Message Message { get; set; } // might hold metadata used by response pipeline

        private bool alreadyFired;
        private TimeSpan timeout;
        private Action<CallbackData> onTimeout; // temporary for pipeline transition
        
        private SafeTimer timer;
//#if TRACK_DETAILED_STATS
        private ITimeInterval timeSinceIssued;
//#endif
        private static Logger logger = Logger.GetLogger("CallbackData");
        private static bool useCoarseGrainTimerWheel = false;
        private static CoarseGrainTimerWheel coarseGrainTimerWheel;

        internal static IMessagingConfiguration Config;

        public CallbackData(Action<Message, TaskCompletionSource<object>> callback, Func<Message, bool> resend, TaskCompletionSource<object> ctx, Message msg, Action unregisterDelegate, Action<CallbackData> onTimeout = null)
        {
            this.callback = callback;
            this.resend = resend;
            this.context = ctx;
            this.Message = msg;
            this.unregister = unregisterDelegate;
            this.alreadyFired = false;
            this.onTimeout = onTimeout;

            if (useCoarseGrainTimerWheel)
            {
                coarseGrainTimerWheel = new CoarseGrainTimerWheel(10, TimeSpan.FromSeconds(100));
            }
        }

        /// <summary>
        /// Start this callback timer
        /// </summary>
        /// <param name="time">Timeout time</param>
        public void StartTimer(TimeSpan time)
        {
            if (time < TimeSpan.Zero)
                throw new ArgumentOutOfRangeException("The timeout parameter is negative.");
            timeout = time;
//#if TRACK_DETAILED_STATS
            if (StatisticsCollector.CollectApplicationRequestsStats)
            {
                timeSinceIssued = TimeIntervalFactory.CreateTimeInterval(StatisticsCollector.MeasureAppRequestsFineGrainedTime);
                timeSinceIssued.Start();
            }
//#endif
            if (useCoarseGrainTimerWheel)
            {
                coarseGrainTimerWheel.ScheduleForTimeout(this, timeout);
            }
            else
            {
                TimeSpan firstPeriod = timeout;
                TimeSpan repeatPeriod = Constants.INFINITE_TIMESPAN; // Single timeout period --> No repeat
                if (Config.ResendOnTimeout && resend != null && Config.MaxResendCount > 0)
                {
                    firstPeriod = repeatPeriod = timeout.Divide(Config.MaxResendCount + 1);
                }
                // Start time running
                DisposeTimer();
                timer = new SafeTimer(TimeoutCallback, null, firstPeriod, repeatPeriod);
            }
        }

        private void TimeoutCallback(object obj)
        {
            if (onTimeout != null)
            {
                onTimeout(this);
            }
            else
            {
                OnTimeout();
            }
        }

        public void OnTimeout()
        {
            if (alreadyFired)
                return;
            lock (this)
            {
                if (alreadyFired)
                    return;

                if (Config.ResendOnTimeout && resend != null && resend(Message))
                {
                    if(logger.IsVerbose) logger.Verbose("OnTimeout - Resend {0} for {1}", Message.ResendCount, Message);
                    return;
                }

                alreadyFired = true;
                DisposeTimer();
//#if TRACK_DETAILED_STATS
                if (StatisticsCollector.CollectApplicationRequestsStats)
                {
                    timeSinceIssued.Stop();
                }
//#endif
                if (unregister != null)
                {
                    unregister();
                }
            }

            string errorMsg = String.Format("Response did not arrive on time in {0} for message: {1}. Target History is: {2}",
                                timeout, Message, Message.GetTargetHistory());
            logger.Warn(ErrorCode.Runtime_Error_100157, "{0}. About to break its promise.", errorMsg);

            Message error = new Message(Message.Categories.Application, Message.Directions.Response);
            error.Result = Message.ResponseTypes.Error;
            error.BodyObject = OrleansResponse.ExceptionResponse(new TimeoutException(errorMsg));
//#if TRACK_DETAILED_STATS
            if (StatisticsCollector.CollectApplicationRequestsStats)
            {
                ApplicationRequestsStatisticsGroup.OnAppRequestsEnd(timeSinceIssued.Elapsed);
                ApplicationRequestsStatisticsGroup.OnAppRequestsTimedOut();
            }
//#endif
            if (callback != null)
            {
                callback(error, context);
            }
        }

        public void DoCallback(Message response)
        {
            if (alreadyFired)
                return;
            lock (this)
            {
                if (alreadyFired)
                    return;

                if (response.Result == Message.ResponseTypes.Rejection && response.RejectionType == Message.RejectionTypes.Transient)
                {
                    if (resend != null && resend(Message))
                    {
                        return;
                    }
                }

                alreadyFired = true;
                DisposeTimer();
//#if TRACK_DETAILED_STATS
                if (StatisticsCollector.CollectApplicationRequestsStats)
                {
                    timeSinceIssued.Stop();
                }
//#endif
                if (unregister != null)
                {
                    unregister();
                }     
            }
            response.AddTimestamp(Message.LifecycleTag.InvokeIncoming);
            if (logger.IsVerbose2) logger.Verbose2("Message {0} timestamps: {1}", response, response.GetTimestampString());
//#if TRACK_DETAILED_STATS
            if (StatisticsCollector.CollectApplicationRequestsStats)
            {
                ApplicationRequestsStatisticsGroup.OnAppRequestsEnd(timeSinceIssued.Elapsed);
            }
//#endif
            if (callback != null)
            {
                // do callback outside the CallbackData lock. Just not a good practice to hold a lock for this unrelated operation.
                callback(response, context);
            }
            return;
        }

        public void Dispose()
        {
            if (coarseGrainTimerWheel != null)
            {
                coarseGrainTimerWheel.Dispose();
                coarseGrainTimerWheel = null;
            }
            else
            {
                DisposeTimer();
            }

            GC.SuppressFinalize(this);
        }

        private void DisposeTimer()
        {
            try
            {
                if (timer != null)
                {
                    timer.Dispose();
                    timer = null;
                }
            }
            catch (Exception) { }
        }

        public TimeSpan RequestedTimeout()
        {
            return timeout;
        }
    }
}
