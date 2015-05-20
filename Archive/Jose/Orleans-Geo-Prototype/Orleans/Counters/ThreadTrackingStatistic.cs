using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using System.Threading;
using System.Runtime.InteropServices;

namespace Orleans.Counters
{
    internal class ThreadTrackingStatistic
    {
        public ITimeInterval executingCPUCycleTime;
        public ITimeInterval executingWallClockTime;
        public ITimeInterval processingCPUCycleTime;
        public ITimeInterval processingWallClockTime;
        public ulong numRequests;
        public string Name;

        private static StageAnalysis globalStageAnalyzer = new StageAnalysis();
        private static readonly string CONTEXT_SWTICH_COUNTER_NAME = "Context Switches/sec";
        private static bool clientConnected = false;
        private bool firstStart;


        /// <summary>
        /// Keep track of thread statistics, mainly timing, can be created outside the thread to be tracked.
        /// </summary>
        /// <param name="threadName">Name used for logging the collected stastistics</param>
        /// <param name="storage"></param>
        public ThreadTrackingStatistic(string threadName, CounterStorage storage = CounterStorage.LogOnly)
        {
            executingCPUCycleTime = new TimeInterval_ThreadCycleCounterBased();
            executingWallClockTime = TimeIntervalFactory.CreateTimeInterval(true);
            processingCPUCycleTime = new TimeInterval_ThreadCycleCounterBased();
            processingWallClockTime = TimeIntervalFactory.CreateTimeInterval(true);

            numRequests = 0;

            // 4 direct counters
            FloatValueStatistic.FindOrCreate(new StatName(StatNames.STAT_THREADS_EXECUTION_TIME_TOTAL_CPU_CYCLES, threadName),
                    () =>
                    {
                        return (float)executingCPUCycleTime.Elapsed.TotalMilliseconds;
                    }, storage);
            FloatValueStatistic.FindOrCreate(new StatName(StatNames.STAT_THREADS_EXECUTION_TIME_TOTAL_WALL_CLOCK, threadName),
                    () =>
                    {
                        return (float)executingWallClockTime.Elapsed.TotalMilliseconds;
                    }, storage);
            FloatValueStatistic.FindOrCreate(new StatName(StatNames.STAT_THREADS_PROCESSING_TIME_TOTAL_CPU_CYCLES, threadName),
                    () =>
                    {
                        return (float)processingCPUCycleTime.Elapsed.TotalMilliseconds;
                    }, storage);
            FloatValueStatistic.FindOrCreate(new StatName(StatNames.STAT_THREADS_PROCESSING_TIME_TOTAL_WALL_CLOCK, threadName),
                    () =>
                    {
                        return (float)processingWallClockTime.Elapsed.TotalMilliseconds;
                    }, storage);

            // numRequests
            FloatValueStatistic.FindOrCreate(new StatName(StatNames.STAT_THREADS_PROCESSED_REQUESTS_PER_THREAD, threadName),
                    () =>
                    {
                        return (float)numRequests;
                    }, storage);

            firstStart = true;
            Name = threadName;

            globalStageAnalyzer.AddTracking(this);
        }

        public static void FirstClientConnectedStartTracking()
        {
            clientConnected = true;
        }

        private void TrackContextSwitches()
        {
            PerformanceCounterCategory allThreadsWithPerformanceCounters = new PerformanceCounterCategory("Thread");
            PerformanceCounter[] performanceCountersForThisThread = null;

            // Iterate over all "Thread" category performance counters on system (includes numerous processes)
            foreach (string threadName in allThreadsWithPerformanceCounters.GetInstanceNames())
            {

                // Obtain those performance counters for the OrleansHost
                if (threadName.Contains("OrleansHost") && threadName.EndsWith("/" + Thread.CurrentThread.ManagedThreadId))
                {
                    performanceCountersForThisThread = allThreadsWithPerformanceCounters.GetCounters(threadName);
                    break;
                }
            }

            // In the case that the performance was not obtained correctly (this condition is null), we simply will not have stats for context switches
            if (performanceCountersForThisThread != null)
            {

                // Look at all performance counters for this thread
                foreach (PerformanceCounter performanceCounter in performanceCountersForThisThread)
                {

                    // Find performance counter for context switches
                    if (performanceCounter.CounterName == CONTEXT_SWTICH_COUNTER_NAME)
                    {
                        // Use raw value for logging, should show total context switches
                        FloatValueStatistic.FindOrCreate(new StatName(StatNames.STAT_THREADS_CONTEXT_SWITCHES, Name), () => { return (float)performanceCounter.RawValue; }, CounterStorage.LogOnly);
                    }
                }
            }
        }

        /// <summary>
        /// Call once when the thread is started, must be called from the thread being tracked
        /// </summary>
        public void OnStartExecution()
        {
            // Only once a client has connected do we start tracking statistics
            if (clientConnected)
            {
                executingCPUCycleTime.Start();
                executingWallClockTime.Start();
            }
        }

        /// <summary>
        /// Call once when the thread is stopped, must be called from the thread being tracked
        /// </summary>
        public void OnStopExecution()
        {
            // Only once a client has connected do we start tracking statistics
            if (clientConnected)
            {
                executingCPUCycleTime.Stop();
                executingWallClockTime.Stop();
            }
        }

        /// <summary>
        /// Call once before processing a request, must be called from the thread being tracked
        /// </summary>
        public void OnStartProcessing()
        {
            // Only once a client has connected do we start tracking statistics
            if (clientConnected)
            {

                // As this function is called constantly, we perform two additional tasks in this function which require calls from the thread being tracked (the constructor is not called from the tracked thread)
                if (firstStart)
                {

                    // If this is the first function call where client has connected, we ensure execution timers are started and context switches are tracked
                    firstStart = false;
                    TrackContextSwitches();
                    OnStartExecution();
                }
                else
                {
                    // Must toggle this counter as its "Elapsed" value contains the value when it was last stopped, this is a limitation of our techniques for CPU tracking of threads
                    executingCPUCycleTime.Stop();
                    executingCPUCycleTime.Start();
                }

                processingCPUCycleTime.Start();
                processingWallClockTime.Start();
            }
        }

        /// <summary>
        /// Call once after processing multiple requests as a batch or a single request, must be called from the thread being tracked
        /// </summary>
        /// <param name="num">Number of processed requests</param>
        public void OnStopProcessing()
        {
            // Only once a client has connected do we start tracking statistics
            if (clientConnected)
            {
                processingCPUCycleTime.Stop();
                processingWallClockTime.Stop();
            }
        }

        /// <summary>
        /// Call once to just increment the stastistic of processed requests
        /// </summary>
        /// <param name="num">Number of processed requests</param>
        public void IncrementNumberOfProcessed(int num = 1)
        {
            // Only once a client has connected do we start tracking statistics
            if (clientConnected)
            {
                if (num > 0)
                {
                    numRequests += (ulong)num;
                }
            }
        }
    }
}
