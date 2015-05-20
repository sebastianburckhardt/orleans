using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Runtime.InteropServices;
using Orleans.Counters;
using System.Diagnostics;

namespace Orleans
{
    /// <summary>
    /// Stopwatch for CPU time of a thread. 
    /// You must only use Start, Stop, and Restart from thread being measured!
    /// CANNOT call this class from a different thread that is not the currently executing thread. 
    /// Otherwise, QueryThreadCycleTime returns undefined (garbage) results. 
    /// </summary>
    internal class TimeInterval_ThreadCycleCounterBased : ITimeInterval
    {
        [DllImport("Kernel32.dll")]
        private static extern bool QueryThreadCycleTime(IntPtr handle, out ulong cycles);
        [DllImport("Kernel32.dll")]
        private static extern bool QueryPerformanceFrequency(out long lpFrequency);
        [DllImport("Kernel32.dll")]
        private static extern IntPtr GetCurrentThread();

        private readonly double cyclesPerSecond;

        private IntPtr handle;
        private ulong startCycles;
        private ulong stopCycles;
        private ulong elapsedCycles;
        private bool running;

        /// <summary>
        /// Obtain current time of stopwatch since last Stop method. You may call this from any thread.
        /// </summary>
        public TimeSpan Elapsed 
        { 
            get 
            { 
                return TimeSpan.FromSeconds(((double)elapsedCycles)/(cyclesPerSecond));
            } 
        }

        /// <summary>
        /// Create thread CPU timing object. You may call this from a thread outside the one you wish to measure.
        /// </summary>
        public TimeInterval_ThreadCycleCounterBased()
        {
            // System call returns what seems to be (from measurements) a value that is in cycles per 1/1024 of a second (close to millisecond)
            long cyclesPerMillisecond;
            QueryPerformanceFrequency(out cyclesPerMillisecond);
            cyclesPerSecond = (double)(1024.0 * (double)cyclesPerMillisecond);

            handle = IntPtr.Zero;
            elapsedCycles = 0;
            startCycles = 0;
            stopCycles = 0;
            running = false;
        }

        /// <summary>
        /// Start measuring time thread is using CPU. Must invoke from thread to be measured!
        /// </summary>
        public void Start()
        {
            if (handle.Equals(IntPtr.Zero))
            {
                handle = GetCurrentThread();
            }
            if (!running)
            {
                running = true;
                QueryThreadCycleTime(handle, out startCycles);
            }
        }

        /// <summary>
        /// Restart measuring time thread is using CPU. Must invoke from thread to be measured!
        /// </summary>
        public void Restart()
        {
            elapsedCycles = 0;
            Start();
        }

        /// <summary>
        /// Stop measuring time thread is using CPU. Must invoke from thread to be measured!
        /// </summary>
        public void Stop()
        {
            if (running)
            {
                running = false;
                QueryThreadCycleTime(handle, out stopCycles);
                if (stopCycles > startCycles)
                {
                    elapsedCycles += (stopCycles - startCycles);
                }
                else
                {
                    Logger log = Logger.GetLogger("Thread Cycle Counter", Logger.LoggerType.Runtime);
                    if(log.IsVerbose2) log.Verbose2(0, String.Format("Invalid cycle counts startCycles = {0}, stopCycles = {1}", startCycles, stopCycles));

                    // Some threadpool threads seem to be reset, this is normal with .NET threadpool threads as its assumed they are restart out of our control
                    elapsedCycles += stopCycles;
                }
            }
        }
    }
}
