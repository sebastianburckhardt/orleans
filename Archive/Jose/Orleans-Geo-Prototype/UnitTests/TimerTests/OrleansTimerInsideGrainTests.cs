namespace UnitTests.TimerTests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    using Microsoft.VisualStudio.TestTools.UnitTesting;

    using Orleans;
    using Orleans.Counters;
    
    using Orleans.Scheduler;

    using UnitTests.SchedulerTests;

    [TestClass]
    public class OrleansTimerInsideGrainTests : UnitTestBase
    {
        static private List<ISchedulingContext> contexts = null;
        static private OrleansTaskScheduler scheduler = null;
        private Stopwatch stopwatch = null;
        private readonly object stopwatchLock = new object();

        private readonly Random rng = new Random();

        [ClassInitialize]
        static public void InitializeClass(TestContext unused)
        {
            InitializeLogging();
            StartScheduler();
        }

        [ClassCleanup]
        static public void CleanupClass()
        {
            scheduler.Stop();
            Logger.UnInitialize();
        }

        [TestInitialize]
        public void InitializeTest()
        {
            // [mlr] we expect the test stop
            this.stopwatch = null;
        }

        [TestCleanup]
        public void CleanupTest()
        {
            CheckForUnobservedPromises();
        }

        public OrleansTimerInsideGrainTests()
        {}

        static private void InitializeLogging()
        {
            Logger.UnInitialize();
            Logger.LogConsumers.Add(new LogWriterToConsole());
            var traceLevels = new[]
            {
                Tuple.Create("Scheduler", OrleansLogger.Severity.Error),
                Tuple.Create("Scheduler.WorkerPoolThread", OrleansLogger.Severity.Error),
            };
            Logger.SetTraceLevelOverrides(new List<Tuple<string, OrleansLogger.Severity>>(traceLevels));

            var orleansConfig = new OrleansConfiguration();
            orleansConfig.StandardLoad();
            NodeConfiguration config = orleansConfig.GetConfigurationForNode("Primary");
            LimitManager.Initialize(config);
            StatisticsCollector.Initialize(config);
            SchedulerStatisticsGroup.Init();
        }

        static private ISchedulingContext GetContext(int index)
        {
            return contexts[index % contexts.Count];
        }

        static private void StartScheduler()
        {
            if (scheduler != null)
                throw new InvalidOperationException("this.scheduler should be null");

            scheduler = new OrleansTaskScheduler(Environment.ProcessorCount);
            LimitManager.Initialize(new DummyLimitsConfiguration());
            OrleansTask.Initialize(scheduler);
            contexts = new List<ISchedulingContext>();
            for (var i = 0; i < scheduler.MaximumConcurrencyLevel; ++i)
            {
                var ctx = new UnitTestSchedulingContext();
                contexts.Add(ctx);
                scheduler.RegisterWorkContext(ctx);
            }
            scheduler.Start();
        }

        public double Test1()
        {
            return this.TestTimers(10000, TimeSpan.FromSeconds(8), 20);
        }

        [TestMethod, TestCategory("Timers")]
        public void OrleansTimerInsideGrainShouldDeliver97PercentOfExpectedTickThouroughput()
        {
            // [mlr] 10,000 grains at 1 second should be easy to attain.
            var percentDelivered = this.TestTimers(10, TimeSpan.FromSeconds(1), 30);
            Assert.IsTrue(percentDelivered > 0.97, "i delivered fewer ticks per second than anticipated ({0}%).", percentDelivered);
        }

        private double TestTimers(int timerCount, TimeSpan period, int napMultiplier)
        {
            logger.Info(
                "TestTimers(): initializing timerCount={0}, period={1} sec, napMultiplier={2}", 
                timerCount, 
                period, 
                napMultiplier);

            // [mlr] based on observation, it takes approximately a second to create 100000 timers.
            var baseStartDelay = TimeSpan.FromSeconds(((double)timerCount / 100000) + 5);
            var timers = new IDisposable[timerCount];
            var counters = new int[timerCount];
            var locks = new object[timerCount];
            
            var initStopwatch = Stopwatch.StartNew();
            for (var i = 0; i < timers.Length; ++i)
            {
                locks[i] = new object();
                var startDelay = baseStartDelay + TimeSpan.FromSeconds(rng.NextDouble() * period.TotalSeconds);
                var newTimer = 
                    OrleansTimerInsideGrain.FromTaskCallback(
                        ob =>
                            this.TaskCallback((int)ob, counters, locks),
                        i, 
                        startDelay, 
                        period, 
                        GetContext(i));
                timers[i] = newTimer;
                newTimer.Start();
            }
            initStopwatch.Stop();
            logger.Info("TestTimers(): initialized ({0} sec)", initStopwatch.Elapsed.TotalSeconds);
            Assert.IsTrue(
                initStopwatch.Elapsed < baseStartDelay, 
                "initialization took longer than expected (this may affect test results)");
            
            // [mlr] schedule about 3 periods of activity.
            var napLength = 
                TimeSpan.FromSeconds(
                    baseStartDelay.TotalSeconds - initStopwatch.Elapsed.TotalSeconds + period.TotalSeconds * napMultiplier);
            logger.Info("TestTimers(): sleeping");
            Thread.Sleep(napLength);

            this.stopwatch.Stop();

            for (var i = 0; i < timers.Length; ++i)
            {
                var t = timers[i];
                if (t != null)
                {
                    // [mlr] there's a race condition here but it's not a big deal at the moment.
                    timers[i] = null;
                    t.Dispose();
                }
            }

            var runTime = this.stopwatch.Elapsed;
            var totalTickCount = counters.Sum();
            var actualTicksPerSecond = totalTickCount / runTime.TotalSeconds;
            var expectedTicksPerSecond = timerCount / period.TotalSeconds;
            logger.Info("TestTimers: totalTickCount = " + totalTickCount + " for " + timerCount + " timer with timer Period of " + period.TotalSeconds + " s for " + runTime.TotalSeconds + " sec total run time.");
            logger.Info("TestTimers: Min counter = " + counters.Min());
            logger.Info("TestTimers: Max counter = " + counters.Max());
            logger.Info("TestTimers: Expected counter = " + (runTime.TotalSeconds / period.TotalSeconds));
            logger.Info("TestTimers: ActualTicksPerSecond = " + actualTicksPerSecond);
            logger.Info("TestTimers: ExpectedTicksPerSecond = " + expectedTicksPerSecond);

            return actualTicksPerSecond / expectedTicksPerSecond;
        }

        private void TimerCallback(int index, int[] counters, object[] locks)
        {
            if (this.stopwatch == null)
            {
                lock (this.stopwatchLock)
                {
                    if (this.stopwatch == null)
                    {
                        Assert.IsNotNull(WorkerPoolThread.CurrentWorkerThread,
                            "I expect the timer callback to be run from an Orleans worker pool thread.");
                        Assert.AreSame(
                            scheduler, 
                            TaskScheduler.Current,
                            "I expected to find my test orleans scheduler being used as the contextual TPL task scheduler.");
                        logger.Info("TimerCallback: started");
                        this.stopwatch = Stopwatch.StartNew();
                    }
                }
            }

            if (this.stopwatch.IsRunning)
            {
                lock (locks[index])
                {
                    ++counters[index];
                }
            }
        }

        private Task TaskCallback(int index, int[] counters, object[] locks)
        {
            this.TimerCallback(index, counters, locks);
            return TaskDone.Done;
        }

    }
}
