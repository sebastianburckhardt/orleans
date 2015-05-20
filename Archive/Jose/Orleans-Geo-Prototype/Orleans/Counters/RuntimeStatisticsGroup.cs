#define LOG_MEMORY_PERF_COUNTERS
using System;
using System.Diagnostics;

using System.Threading;

namespace Orleans.Counters
{
    internal class RuntimeStatisticsGroup
    {
        private static readonly Logger logger = Logger.GetLogger("RuntimeStatisticsGroup", Logger.LoggerType.Runtime);

        private PerformanceCounter cpuCounter;
#if LOG_MEMORY_PERF_COUNTERS
        private PerformanceCounter timeInGC;
        private PerformanceCounter[] genSizes;
        private PerformanceCounter allocatedBytesPerSec;
        private PerformanceCounter promotedMemoryFromGen1;
        private PerformanceCounter numberOfInducedGCs;
        private PerformanceCounter largeObjectHeapSize;
        private PerformanceCounter promotedFinalizationMemoryFromGen0;
#endif
        private SafeTimer cpuUsageTimer;
        private readonly TimeSpan CPU_CHECK_PERIOD = TimeSpan.FromSeconds(5);
        private bool countersAvailable;


        public long MemoryUsage { get { return GC.GetTotalMemory(false); } }

        public float CpuUsage { get; private set; }

        private static string GC_GenCollectionCount
        {
            get
            {
                return String.Format("gen0={0}, gen1={1}, gen2={2}", GC.CollectionCount(0), GC.CollectionCount(1), GC.CollectionCount(2));
            }
        }

#if LOG_MEMORY_PERF_COUNTERS
        private string GC_GenSizes
        {
            get
            {
                return String.Format("gen0={0:0.00}, gen1={1:0.00}, gen2={2:0.00}", genSizes[0].NextValue() / 1024f, genSizes[1].NextValue() / 1024f, genSizes[2].NextValue() / 1024f);
            }
        }
#endif
        internal RuntimeStatisticsGroup()
        {
            InitCpuMemoryCounters();
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private void InitCpuMemoryCounters()
        {
            try
            {
                cpuCounter = new PerformanceCounter("Processor", "% Processor Time", "_Total", true);
#if LOG_MEMORY_PERF_COUNTERS
                string thisProcess = Process.GetCurrentProcess().ProcessName;
                timeInGC = new PerformanceCounter(".NET CLR Memory", "% Time in GC", thisProcess, true);
                genSizes = new PerformanceCounter[] { 
                    new PerformanceCounter(".NET CLR Memory", "Gen 0 heap size", thisProcess, true), 
                    new PerformanceCounter(".NET CLR Memory", "Gen 1 heap size", thisProcess, true), 
                    new PerformanceCounter(".NET CLR Memory", "Gen 2 heap size", thisProcess, true)
                };
                allocatedBytesPerSec = new PerformanceCounter(".NET CLR Memory", "Allocated Bytes/sec", thisProcess, true);
                promotedMemoryFromGen1 = new PerformanceCounter(".NET CLR Memory", "Promoted Memory from Gen 1", thisProcess, true);
                numberOfInducedGCs = new PerformanceCounter(".NET CLR Memory", "# Induced GC", thisProcess, true);
                largeObjectHeapSize = new PerformanceCounter(".NET CLR Memory", "Large Object Heap size", thisProcess, true);
                promotedFinalizationMemoryFromGen0 = new PerformanceCounter(".NET CLR Memory", "Promoted Finalization-Memory from Gen 0", thisProcess, true);
#endif
                countersAvailable = true;
            }
            catch (Exception exc)
            {
                logger.Warn(ErrorCode.PerfCounterConnectError,
                    "Error initializing CPU & Memory perf counters - you need to repair Windows perf counter config on this machine with 'lodctr /r' command", exc);
            }
        }

        internal void Start()
        {
            if (!countersAvailable)
            {
                logger.Warn(ErrorCode.PerfCounterNotRegistered,
                    "CPU & Memory perf counters did not initialize correctly - try repairing Windows perf counter config on this machine with 'lodctr /r' command");
                return;
            }

            cpuUsageTimer = new SafeTimer(CheckCpuUsage, null, CPU_CHECK_PERIOD, CPU_CHECK_PERIOD);
            try
            {
                // Read initial value of CPU Usage counter
                CpuUsage = cpuCounter.NextValue();
            }
            catch (InvalidOperationException)
            {
                // Can sometimes get exception accessing CPU Usage counter for first time in some runtime environments
                CpuUsage = 0;
            }

            FloatValueStatistic.FindOrCreate(StatNames.STAT_RUNTIME_CPUUSAGE, () => CpuUsage);
            IntValueStatistic.FindOrCreate(StatNames.STAT_RUNTIME_GC_TOTALMEMORYKB, () => (MemoryUsage + 1023) / 1024); // Round up
#if LOG_MEMORY_PERF_COUNTERS    // print GC stats in the silo log file.
            StringValueStatistic.FindOrCreate(StatNames.STAT_RUNTIME_GC_GENCOLLECTIONCOUNT, () => GC_GenCollectionCount);
            StringValueStatistic.FindOrCreate(StatNames.STAT_RUNTIME_GC_GENSIZESKB, () => GC_GenSizes);
            FloatValueStatistic.FindOrCreate(StatNames.STAT_RUNTIME_GC_PERCENTOFTIMEINGC, () => timeInGC.NextValue());
            FloatValueStatistic.FindOrCreate(StatNames.STAT_RUNTIME_GC_ALLOCATEDBYTESINKBPERSEC, () => allocatedBytesPerSec.NextValue() / 1024f);
            FloatValueStatistic.FindOrCreate(StatNames.STAT_RUNTIME_GC_PROMOTEDMEMORYFROMGEN1KB, () => promotedMemoryFromGen1.NextValue() / 1024f);
            FloatValueStatistic.FindOrCreate(StatNames.STAT_RUNTIME_GC_LARGEOBJECTHEAPSIZEKB, () => largeObjectHeapSize.NextValue() / 11024f);
            FloatValueStatistic.FindOrCreate(StatNames.STAT_RUNTIME_GC_PROMOTEDMEMORYFROMGEN0KB, () => promotedFinalizationMemoryFromGen0.NextValue() / 1024f);
            FloatValueStatistic.FindOrCreate(StatNames.STAT_RUNTIME_GC_NUMBEROFINDUCEDGCS, () => numberOfInducedGCs.NextValue());
#endif
            IntValueStatistic.FindOrCreate(StatNames.STAT_RUNTIME_DOT_NET_THREADPOOL_INUSE_WORKERTHREADS, () =>
            {
                int maXworkerThreads;
                int maXcompletionPortThreads;
                ThreadPool.GetMaxThreads(out maXworkerThreads, out maXcompletionPortThreads);
                int workerThreads;
                int completionPortThreads;
                // GetAvailableThreads Retrieves the difference between the maximum number of thread pool threads
                // and the number currently active.
                // So max-Available is the actual number in use. If it goes beyond min, it means we are stressing the thread pool.
                ThreadPool.GetAvailableThreads(out workerThreads, out completionPortThreads);
                return maXworkerThreads - workerThreads;
            });
            IntValueStatistic.FindOrCreate(StatNames.STAT_RUNTIME_DOT_NET_THREADPOOL_INUSE_COMPLETIONPORTTHREADS, () =>
            {
                int maXworkerThreads;
                int maXcompletionPortThreads;
                ThreadPool.GetMaxThreads(out maXworkerThreads, out maXcompletionPortThreads);
                int workerThreads;
                int completionPortThreads;
                ThreadPool.GetAvailableThreads(out workerThreads, out completionPortThreads);
                return maXcompletionPortThreads - completionPortThreads;
            });
        }

        private void CheckCpuUsage(object m)
        {
            var currentUsage = cpuCounter.NextValue();
            // We calculate a decaying average for CPU utilization
            CpuUsage = (CpuUsage + 2 * currentUsage) / 3;
        }

        public void Stop()
        {
            if (cpuUsageTimer != null)
                cpuUsageTimer.Dispose();
            cpuUsageTimer = null;
        }
    }
}


