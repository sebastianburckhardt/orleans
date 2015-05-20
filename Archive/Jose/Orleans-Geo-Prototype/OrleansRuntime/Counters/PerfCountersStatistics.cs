using System;


namespace Orleans.Runtime.Counters
{
    /// <summary>
    /// Background publisher of counter values.
    /// Updates to counters needs to be very fast, so are all in-memory operations.
    /// This class then follows up to periodically write the counter values to OS
    /// </summary>
    internal class PerfCountersStatistics
    {
        private static readonly Logger logger = Logger.GetLogger("WindowsPerfCountersStatistics", Logger.LoggerType.Runtime);

        private const int ErrorThreshold = 10; // A totally arbitrary value!

        public TimeSpan PerfCountersWriteInterval { get; private set; }

        private SafeTimer timer;
        private bool shouldWritePerfCounters = true;


        /// <summary>
        /// Initialize the counter publisher framework. Start the background stats writer thread.
        /// </summary>
        /// <param name="writeInterval">Frequency of writing to Windows perf counters</param>
        public PerfCountersStatistics(TimeSpan writeInterval)
        {
            if (writeInterval <= TimeSpan.Zero)
                throw new ArgumentException("Creating CounterStatsPublisher with negative or zero writeInterval", "writeInterval");

            this.PerfCountersWriteInterval = writeInterval;
        }

        /// <summary>
        /// Prepare for stats collection
        /// </summary>
        private void Prepare()
        {
            if (!OrleansPerfCounterManager.AreWindowsPerfCountersAvailable())
            {
                logger.Warn(ErrorCode.PerfCounterNotFound, "Windows perf counters not found -- defaulting to in-memory counters. Run CounterControl.exe as Administrator to create perf counters for Orleans.");
                shouldWritePerfCounters = false;
                return;
            }
            
            try
            {
                OrleansPerfCounterManager.PrecreateCounters();
            }
            catch(Exception exc)
            {
                logger.Warn(ErrorCode.PerfCounterFailedToInitialize, "Failed to initialize perf counters -- defaulting to in-memory counters. Run CounterControl.exe as Administrator to create perf counters for Orleans.", exc);
                shouldWritePerfCounters = false;
            }
        }

        /// <summary>
        /// Start stats collection
        /// </summary>
        public void Start()
        {
            logger.Info(ErrorCode.PerfCounterStarting, "Starting Windows perf counter stats collection with frequency={0}", PerfCountersWriteInterval);
            this.Prepare();
            this.timer = new SafeTimer(TimerTick, null, this.PerfCountersWriteInterval, this.PerfCountersWriteInterval); // Start the timer running
        }

        /// <summary>
        /// Stop stats collection
        /// </summary>
        public void Stop()
        {
            logger.Info(ErrorCode.PerfCounterStopping, "Stopping  Windows perf counter stats collection");
            if (this.timer != null)
                this.timer.Dispose(); // Stop timer
            this.timer = null;
        }

        /// <summary>
        /// Handle a timer tick
        /// </summary>
        /// <param name="state"></param>
        private void TimerTick(object state)
        {
            if (shouldWritePerfCounters)
            {
                // Write counters to Windows perf counters
                int numErrors = OrleansPerfCounterManager.WriteCounters();

                if (numErrors > 0)
                {
                    logger.Warn(ErrorCode.PerfCounterWriteErrors,
                                "Completed writing Windows perf counters with {0} errors", numErrors);
                }
                else if (logger.IsVerbose2)
                {
                    logger.Verbose2(ErrorCode.PerfCounterWriteSuccess,
                                    "Completed writing Windows perf counters sucessfully");
                }

                if (numErrors > ErrorThreshold)
                {
                    logger.Error(ErrorCode.PerfCounterWriteTooManyErrors,
                                "Too many errors writing Windows perf counters -- disconnecting counters");
                    shouldWritePerfCounters = false;
                }
            }
            else if (logger.IsVerbose2)
            {
                logger.Verbose2("Skipping - Writing Windows perf counters is disabled");
            }
        }
    }
}
