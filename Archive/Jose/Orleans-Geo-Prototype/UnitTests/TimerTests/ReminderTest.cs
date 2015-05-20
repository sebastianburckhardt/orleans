using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using Orleans.AzureUtils;
using Orleans.Runtime.ReminderService;
using UnitTestGrains;

namespace UnitTests.TimerTests
{
    [TestClass]
    public class ReminderTests_TableGrain : UnitTestBase
    {
        private static Options siloOptions = new Options
        {
            StartFreshOrleans = true,
            ReminderServiceType = GlobalConfiguration.ReminderServiceProviderType.ReminderTableGrain
        };

        public ReminderTests_TableGrain()
            : base(siloOptions)
        {
        }

        protected readonly Logger log = Logger.GetLogger("ReminderTests_TableGrain", Logger.LoggerType.Application);

        [TestCleanup]
        public void TestCleanup()
        {
            ReminderTests_AzureTable.Cleanup();
        }

        #region Basic test
        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("ReminderService")]
        public void ReminderTest_StopByRef()
        {
            log.Info("Testing StopByRef");
            IReminderTestGrain grain = ReminderTestGrainFactory.GetGrain(0);

            IOrleansReminder r1 = grain.StartReminder(ReminderTests_AzureTable.DR).Result;
            IOrleansReminder r2 = grain.StartReminder(ReminderTests_AzureTable.DR).Result;
            try
            {
                grain.StopReminder(r1).Wait();
                Assert.Fail("Removed reminder1, which shouldn't be possible.");
            }
            catch
            {
                log.Info("Couldnt remove reminder1, as expected.");
            }

            grain.StopReminder(r2).Wait();
            log.Info("Removed reminder2 successfully");

            // trying to see if readreminder works
            IOrleansReminder o1 = grain.StartReminder(ReminderTests_AzureTable.DR).Result;
            IOrleansReminder o2 = grain.StartReminder(ReminderTests_AzureTable.DR).Result;
            IOrleansReminder o3 = grain.StartReminder(ReminderTests_AzureTable.DR).Result;
            IOrleansReminder o4 = grain.StartReminder(ReminderTests_AzureTable.DR).Result;

            IOrleansReminder r = grain.GetReminderObject(ReminderTests_AzureTable.DR).Result;
            grain.StopReminder(r).Wait();
            log.Info("Removed got reminder successfully");
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("ReminderService")]
        public void ReminderTest_ListOps()
        {
            log.Info("Testing ListOps");
            log.Info("Start");
            IReminderTestGrain grain = ReminderTestGrainFactory.GetGrain(0);
            const int count = 5;
            Task<IOrleansReminder>[] reminders = new Task<IOrleansReminder>[count];
            for (int i = 0; i < count; i++)
            {
                reminders[i] = grain.StartReminder(ReminderTests_AzureTable.DR + "_" + i);
                log.Info("Started {0}_{1}", ReminderTests_AzureTable.DR, i);
            }

            Task.WaitAll(reminders);
            // do comparison on strings
            List<string> registered = (from reminder in reminders select reminder.Result.ReminderName).ToList();

            log.Info("Waited");

            List<IOrleansReminder> get = grain.GetRemindersList().Result;
            List<string> fetched = (from reminder in get select reminder.ReminderName).ToList();

            foreach (var remRegistered in registered)
            {
                Assert.IsTrue(fetched.Remove(remRegistered),
                              string.Format("Couldn't get reminder {0}. Registered list: {1}, fetched list: {2}", remRegistered,
                                            Utils.IEnumerableToString(reminders, toString: r => r.Result.ReminderName),
                                            Utils.IEnumerableToString(get, toString: r => r.ReminderName)));
            }
            Assert.IsTrue(fetched.Count == 0, string.Format("More than registered reminders. Extra: {0}", Utils.IEnumerableToString(fetched)));

            // do some time tests as well
            log.Info("Time tests");
            TimeSpan period = grain.GetReminderPeriod(ReminderTests_AzureTable.DR).Result;
            Thread.Sleep(period.Multiply(2) + ReminderTests_AzureTable.LEEWAY); // giving some leeway
            long cur;
            for (int i = 0; i < count; i++)
            {
                cur = grain.GetCounter(ReminderTests_AzureTable.DR + "_" + i).Result;
                Assert.AreEqual(cur, 2, string.Format("Incorrect ticks for {0}_{1}", ReminderTests_AzureTable.DR, i));
            }
        }
        #endregion

        #region Single join ... multi grain, multi reminders
        [TestMethod, TestCategory("Nightly"), TestCategory("ReminderService")]
        public void ReminderTest_1J_MultiGrainMultiReminders()
        {
            log.Info("Testing 1J_MultiGrainMultiReminders");
            IReminderTestGrain g1 = ReminderTestGrainFactory.GetGrain(1);
            IReminderTestGrain g2 = ReminderTestGrainFactory.GetGrain(2);
            IReminderTestGrain g3 = ReminderTestGrainFactory.GetGrain(3);
            IReminderTestGrain g4 = ReminderTestGrainFactory.GetGrain(4);
            IReminderTestGrain g5 = ReminderTestGrainFactory.GetGrain(5);

            TimeSpan period = g1.GetReminderPeriod(ReminderTests_AzureTable.DR).Result;

            Task[] tasks = new Task[]
            {
                Task.Factory.StartNew(() => ReminderTests_AzureTable.PerGrainMultiReminderTestChurn(g1)),
                Task.Factory.StartNew(() => ReminderTests_AzureTable.PerGrainMultiReminderTestChurn(g2)),
                Task.Factory.StartNew(() => ReminderTests_AzureTable.PerGrainMultiReminderTestChurn(g3)),
                Task.Factory.StartNew(() => ReminderTests_AzureTable.PerGrainMultiReminderTestChurn(g4)),
                Task.Factory.StartNew(() => ReminderTests_AzureTable.PerGrainMultiReminderTestChurn(g5))
            };

            Thread.Sleep(period.Multiply(5));
            // start another silo ... although it will take it a while before it stabilizes
            log.Info("Starting another silo");
            StartAdditionalOrleansRuntimes(1);

            //Block until all tasks complete.
            Task.WaitAll(tasks, ReminderTests_AzureTable.ENDWAIT);
        }
        #endregion
    }

    [TestClass]
    public class ReminderTests_AzureTable : UnitTestBase
    {
        private static string AZURE_DATA_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=orleanstestdata;AccountKey=qFJFT+YAikJPCE8V5yPlWZWBRGns4oti9tqG6/oYAYFGI4kFAnT91HeiWMa6pddUzDcG5OAmri/gk7owTOQZ+A==";

        private static Options siloOptions = new Options
        {
            StartFreshOrleans = true,
            PickNewDeploymentId = true,
            AzureDataConnectionString = AZURE_DATA_CONNECTION_STRING,
            ReminderServiceType = GlobalConfiguration.ReminderServiceProviderType.AzureTable
        };

        public ReminderTests_AzureTable()
            : base(siloOptions) 
        { 
        }

        internal static readonly TimeSpan LEEWAY = TimeSpan.FromMilliseconds(100); // the experiment shouldnt be that long that the sums of leeways exceeds a period
        internal static readonly TimeSpan ENDWAIT = TimeSpan.FromMinutes(5);

        internal const string DR = "DEFAULT_REMINDER";
        internal const string R1 = "REMINDER_1";
        internal const string R2 = "REMINDER_2";

        protected static readonly Logger log = Logger.GetLogger("ReminderTests_AzureTable", Logger.LoggerType.Application);

        // assuming periods for all reminders are the same

        [TestCleanup]
        public void TestCleanup()
        {
            Cleanup();
        }

        internal static void Cleanup()
        {
            CheckForUnobservedPromises();

            // [mlr] ReminderTable.Clear() cannot be called from a non-Orleans thread,
            // so we must proxy the call through a grain.
            var controlProxy = ReminderTestGrainFactory.GetGrain(-1);
            controlProxy.EraseReminderTable().Wait();

            // [mlr] in this context, "reset" means to shut down a silo independent of whether
            // the silo is in-process (AppDomain, which can be stopped) or out-of-process (which
            // must be killed).
            // [mlr][todo] i'd prefer to put these two statements in InitializeTest() but i get a 
            // NullReferenceException if i do. :/
            ResetAllAdditionalRuntimes();
            ResetDefaultRuntimes();
        }

        #region Basic test
        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("ReminderService")]
        public void ReminderTest_Basic()
        {
            log.Info("Testing Basic");
            // [mlr][reverse] start up a test grain and get the period that it's programmed to use.
            IReminderTestGrain grain = ReminderTestGrainFactory.GetGrain(0);
            TimeSpan period = grain.GetReminderPeriod(DR).Result;
            // [mlr][reverse] start up the 'DR' reminder and wait for two ticks to pass.
            grain.StartReminder(DR).Wait();
            Thread.Sleep(period.Multiply(2) + LEEWAY); // giving some leeway
            // [mlr][reverse] retrieve the value of the counter-- it should match the sequence number which is the number of periods
            // we've waited.
            long last = grain.GetCounter(DR).Result;
            Assert.AreEqual(2, last, Time());
            // [mlr][reverse] stop the timer and wait for a whole period.
            grain.StopReminder(DR).Wait();
            Thread.Sleep(period.Multiply(1) + LEEWAY); // giving some leeway
            // [mlr] the counter should not have changed.
            long curr = grain.GetCounter(DR).Result;
            Assert.AreEqual(last, curr, Time());
        }

        [TestMethod, TestCategory("BVT"),  TestCategory("Nightly"), TestCategory("ReminderService")]
        public void ReminderTest_Basic_Restart()
        {
            log.Info("Testing Basic_Restart");
            IReminderTestGrain grain = ReminderTestGrainFactory.GetGrain(0);
            TimeSpan period = grain.GetReminderPeriod(DR).Result;
            grain.StartReminder(DR).Wait();
            Thread.Sleep(period.Multiply(2) + LEEWAY); // giving some leeway
            long last = grain.GetCounter(DR).Result;
            Assert.AreEqual(2, last, Time());

            grain.StopReminder(DR).Wait();
            Thread.Sleep(period.Multiply(1) + LEEWAY); // giving some leeway
            long curr = grain.GetCounter(DR).Result;
            Assert.AreEqual(last, curr, Time());

            // start the same reminder again
            grain.StartReminder(DR).Wait();
            Thread.Sleep(period.Multiply(2) + LEEWAY); // giving some leeway
            curr = grain.GetCounter(DR).Result;
            Assert.AreEqual(2, curr, Time());
            grain.StopReminder(DR).Wait(); // cleanup
        }
        #endregion

        #region Basic single grain multi reminders test
        [TestMethod, TestCategory("Nightly"), TestCategory("ReminderService")]
        public void ReminderTest_MultipleReminders()
        {
            log.Info("Testing MultipleReminders");
            IReminderTestGrain grain = ReminderTestGrainFactory.GetGrain(1);
            PerGrainMultiReminderTest(grain);
        }
        #endregion

        #region Multi grains multi reminders/grain test
        [TestMethod, TestCategory("Nightly"), TestCategory("ReminderService")]
        public void ReminderTest_MultiGrainMultiReminders()
        {
            log.Info("Testing MultiGrainMultiReminders");
            IReminderTestGrain g1 = ReminderTestGrainFactory.GetGrain(1);
            IReminderTestGrain g2 = ReminderTestGrainFactory.GetGrain(2);
            IReminderTestGrain g3 = ReminderTestGrainFactory.GetGrain(3);
            IReminderTestGrain g4 = ReminderTestGrainFactory.GetGrain(4);
            IReminderTestGrain g5 = ReminderTestGrainFactory.GetGrain(5);

            Task[] tasks = new Task[]
            {
                Task.Factory.StartNew(() => PerGrainMultiReminderTest(g1)),
                Task.Factory.StartNew(() => PerGrainMultiReminderTest(g2)),
                Task.Factory.StartNew(() => PerGrainMultiReminderTest(g3)),
                Task.Factory.StartNew(() => PerGrainMultiReminderTest(g4)),
                Task.Factory.StartNew(() => PerGrainMultiReminderTest(g5))
            };

            //Block until all tasks complete.
            Task.WaitAll(tasks, ENDWAIT);
        }

        protected const string info = "Time now: {0}, Grain: {1}, Reminder: {2}"; // print some information on assert failures

        private void PerGrainMultiReminderTest(IReminderTestGrain g)
        {
            TimeSpan period = g.GetReminderPeriod(DR).Result;
            TimeSpan sleepFor = period.Multiply(2) + LEEWAY; // giving some leeway

            // Each reminder is started 2 periods after the previous reminder
            // once all reminders have been started, stop them every 2 periods
            // except the default reminder, which we stop after 3 periods instead
            // just to test and break the symmetry

            // Start Default Reminder
            g.StartReminder(DR).Wait();
            Thread.Sleep(sleepFor);
            long last = g.GetCounter(DR).Result;
            Assert.AreEqual(2, last, string.Format(info, Time(), g.GetPrimaryKeyLong(), DR));

            // Start R1
            g.StartReminder(R1).Wait();
            Thread.Sleep(sleepFor);
            last = g.GetCounter(R1).Result;
            Assert.AreEqual(2, last, string.Format(info, Time(), g.GetPrimaryKeyLong(), R1));

            // Start R2
            g.StartReminder(R2).Wait();
            Thread.Sleep(sleepFor);
            last = g.GetCounter(R1).Result;
            Assert.AreEqual(4, last, string.Format(info, Time(), g.GetPrimaryKeyLong(), R1));
            last = g.GetCounter(R2).Result;
            Assert.AreEqual(2, last, string.Format(info, Time(), g.GetPrimaryKeyLong(), R2));
            last = g.GetCounter(DR).Result;
            Assert.AreEqual(6, last, string.Format(info, Time(), g.GetPrimaryKeyLong(), DR));

            // Stop R1
            g.StopReminder(R1).Wait();
            Thread.Sleep(sleepFor);
            last = g.GetCounter(R1).Result;
            Assert.AreEqual(4, last, string.Format(info, Time(), g.GetPrimaryKeyLong(), R1));
            last = g.GetCounter(R2).Result;
            Assert.AreEqual(4, last, string.Format(info, Time(), g.GetPrimaryKeyLong(), R2));
            last = g.GetCounter(DR).Result;
            Assert.AreEqual(8, last, string.Format(info, Time(), g.GetPrimaryKeyLong(), DR));

            // Stop R2
            g.StopReminder(R2).Wait();
            Thread.Sleep(period.Multiply(3) + LEEWAY); // giving some leeway
            last = g.GetCounter(R1).Result;
            Assert.AreEqual(4, last, string.Format(info, Time(), g.GetPrimaryKeyLong(), R1));
            last = g.GetCounter(R2).Result;
            Assert.AreEqual(4, last, string.Format(info, Time(), g.GetPrimaryKeyLong(), R2));
            last = g.GetCounter(DR).Result;
            Assert.AreEqual(11, last, string.Format(info, Time(), g.GetPrimaryKeyLong(), DR));

            // Stop Default reminder
            g.StopReminder(DR).Wait();
            Thread.Sleep(period.Multiply(1) + LEEWAY); // giving some leeway
            last = g.GetCounter(R1).Result;
            Assert.AreEqual(4, last, string.Format(info, Time(), g.GetPrimaryKeyLong(), R1));
            last = g.GetCounter(R2).Result;
            Assert.AreEqual(4, last, string.Format(info, Time(), g.GetPrimaryKeyLong(), R2));
            last = g.GetCounter(DR).Result;
            Assert.AreEqual(11, last, string.Format(info, Time(), g.GetPrimaryKeyLong(), DR));
        }
        #endregion

        #region Multiple joins ... multi grain, multi reminders
        internal static void PerGrainMultiReminderTestChurn(IReminderTestGrain g)
        {
            // TODO: TMS for churn cases, we do execute start and stop reminders with retries as we don't have the queue-ing functionality implemented on the LocalReminderService yet
            TimeSpan period = g.GetReminderPeriod(DR).Result;

            // Start Default Reminder
            //g.StartReminder(DR, file + "_" + DR).Wait();
            ExecuteWithRetries(g.StartReminder, DR);
            Thread.Sleep(period.Multiply(2));
            // Start R1
            //g.StartReminder(R1, file + "_" + R1).Wait();
            ExecuteWithRetries(g.StartReminder, R1);
            Thread.Sleep(period.Multiply(2));
            // Start R2
            //g.StartReminder(R2, file + "_" + R2).Wait();
            ExecuteWithRetries(g.StartReminder, R2);
            Thread.Sleep(period.Multiply(2));

            Thread.Sleep(period.Multiply(1));

            // Stop R1
            //g.StopReminder(R1).Wait();
            ExecuteWithRetriesStop(g.StopReminder, R1);
            Thread.Sleep(period.Multiply(2));
            // Stop R2
            //g.StopReminder(R2).Wait();
            ExecuteWithRetriesStop(g.StopReminder, R2);
            Thread.Sleep(period.Multiply(1));

            // Stop Default reminder
            //g.StopReminder(DR).Wait();
            ExecuteWithRetriesStop(g.StopReminder, DR);
            Thread.Sleep(period.Multiply(1) + LEEWAY); // giving some leeway

            long last = g.GetCounter(R1).Result;
            //Assert.AreEqual(5, last, String.Format(info, Time(), g.GetPrimaryKeyLong(), R1));
            Assert.IsTrue(5 == last || 4 == last, String.Format(info, Time(), g.GetPrimaryKeyLong(), R1));

            last = g.GetCounter(R2).Result;
            //Assert.AreEqual(5, last, String.Format(info, Time(), g.GetPrimaryKeyLong(), R2));
            Assert.IsTrue(5 == last || 4 == last, String.Format(info, Time(), g.GetPrimaryKeyLong(), R2));

            last = g.GetCounter(DR).Result;
            Assert.AreEqual(10, last, String.Format(info, Time(), g.GetPrimaryKeyLong(), DR));
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("ReminderService")]
        public void ReminderTest_2J_MultiGrainMultiReminders()
        {
            log.Info("Testing 2J_MultiGrainMultiReminders");
            IReminderTestGrain g1 = ReminderTestGrainFactory.GetGrain(1);
            IReminderTestGrain g2 = ReminderTestGrainFactory.GetGrain(2);
            IReminderTestGrain g3 = ReminderTestGrainFactory.GetGrain(3);
            IReminderTestGrain g4 = ReminderTestGrainFactory.GetGrain(4);
            IReminderTestGrain g5 = ReminderTestGrainFactory.GetGrain(5);

            TimeSpan period = g1.GetReminderPeriod(DR).Result;

            Task[] tasks = new Task[]
            {
                Task.Factory.StartNew(() => PerGrainMultiReminderTestChurn(g1)),
                Task.Factory.StartNew(() => PerGrainMultiReminderTestChurn(g2)),
                Task.Factory.StartNew(() => PerGrainMultiReminderTestChurn(g3)),
                Task.Factory.StartNew(() => PerGrainMultiReminderTestChurn(g4)),
                Task.Factory.StartNew(() => PerGrainMultiReminderTestChurn(g5))
            };

            Thread.Sleep(period.Multiply(5));
            // start two silos ... although it will take it a while before they stabilize
            log.Info("Starting 2 silos");
            StartAdditionalOrleansRuntimes(2);
            WaitForLivenessToStabilize();

            //Block until all tasks complete.
            Task.WaitAll(tasks, ENDWAIT);
        }
        #endregion

        #region Secondary failure ... Basic test
        private void PerGrainFailureTest(IReminderTestGrain grain)
        {
            TimeSpan period = grain.GetReminderPeriod(DR).Result;

            grain.StartReminder(DR).Wait();
            Thread.Sleep(period.Multiply(failCheckAfter) + LEEWAY); // giving some leeway
            long last = grain.GetCounter(DR).Result;
            Assert.AreEqual(failCheckAfter, last, string.Format(info, Time(), grain.GetPrimaryKeyLong(), DR));

            grain.StopReminder(DR).Wait();
            Thread.Sleep(period.Multiply(2) + LEEWAY); // giving some leeway
            long curr = grain.GetCounter(DR).Result;
            Assert.AreEqual(last, curr, string.Format(info, Time(), grain.GetPrimaryKeyLong(), DR));
        }

        private const long failAfter = 2; // NOTE: match this sleep with 'failCheckAfter' used in PerGrainFailureTest() so you dont try to get counter immediately after failure as new activation may not have the reminder statistics
        private const long failCheckAfter = 6; // safe value: 9

        [TestMethod, TestCategory("Nightly"), TestCategory("ReminderService")]
        public void ReminderTest_1F_Basic()
        {
            log.Info("Testing 1F_Basic");
            IReminderTestGrain g1 = ReminderTestGrainFactory.GetGrain(1);

            TimeSpan period = g1.GetReminderPeriod(DR).Result;

            AsyncCompletion test = AsyncCompletion.StartNew(() => PerGrainFailureTest(g1));

            Thread.Sleep(period.Multiply(failAfter));
            // stop the secondary silo
            log.Info("Stopping secondary silo");
            StopRuntime(Secondary);

            test.Wait(); // Block until test completes.
        }
        #endregion

        #region Multiple failures ... multiple grains
        [TestMethod, TestCategory("Nightly"), TestCategory("ReminderService")]
        public void ReminderTest_2F_MultiGrain()
        {
            log.Info("Testing 2F_MultiGrain");
            List<SiloHandle> silos = StartAdditionalOrleansRuntimes(2);

            IReminderTestGrain g1 = ReminderTestGrainFactory.GetGrain(1);
            IReminderTestGrain g2 = ReminderTestGrainFactory.GetGrain(2);
            IReminderTestGrain g3 = ReminderTestGrainFactory.GetGrain(3);
            IReminderTestGrain g4 = ReminderTestGrainFactory.GetGrain(4);
            IReminderTestGrain g5 = ReminderTestGrainFactory.GetGrain(5);

            TimeSpan period = g1.GetReminderPeriod(DR).Result;

            Task[] tasks = new Task[]
            {
                Task.Factory.StartNew(() => PerGrainFailureTest(g1)),
                Task.Factory.StartNew(() => PerGrainFailureTest(g2)),
                Task.Factory.StartNew(() => PerGrainFailureTest(g3)),
                Task.Factory.StartNew(() => PerGrainFailureTest(g4)),
                Task.Factory.StartNew(() => PerGrainFailureTest(g5))
            };

            Thread.Sleep(period.Multiply(failAfter));

            // stop a couple of silos
            log.Info("Stopping 2 silos");
            Random r = new Random();
            int i = r.Next(silos.Count);
            StopRuntime(silos[i]);
            silos.RemoveAt(i);
            StopRuntime(silos[r.Next(silos.Count)]);

            Task.WaitAll(tasks, ENDWAIT); // Block until all tasks complete.
        }
        #endregion

        #region 1 join 1 failure simulateneously ... multiple grains
        [TestMethod, TestCategory("Nightly"), TestCategory("ReminderService")]
        public void ReminderTest_1F1J_MultiGrain()
        {
            log.Info("Testing 1F1J_MultiGrain");
            List<SiloHandle> silos = StartAdditionalOrleansRuntimes(1);
            WaitForLivenessToStabilize();

            IReminderTestGrain g1 = ReminderTestGrainFactory.GetGrain(1);
            IReminderTestGrain g2 = ReminderTestGrainFactory.GetGrain(2);
            IReminderTestGrain g3 = ReminderTestGrainFactory.GetGrain(3);
            IReminderTestGrain g4 = ReminderTestGrainFactory.GetGrain(4);
            IReminderTestGrain g5 = ReminderTestGrainFactory.GetGrain(5);

            TimeSpan period = g1.GetReminderPeriod(DR).Result;

            Task[] tasks = new Task[]
            {
                Task.Factory.StartNew(() => PerGrainFailureTest(g1)),
                Task.Factory.StartNew(() => PerGrainFailureTest(g2)),
                Task.Factory.StartNew(() => PerGrainFailureTest(g3)),
                Task.Factory.StartNew(() => PerGrainFailureTest(g4)),
                Task.Factory.StartNew(() => PerGrainFailureTest(g5))
            };

            Thread.Sleep(period.Multiply(failAfter));

            // stop a silo and join a new one in parallel
            log.Info("Stopping a silo and joining a silo");
            Task t1 = Task.Factory.StartNew(() => StopRuntime/*KillRuntime*/(silos[new Random().Next(silos.Count)]));
            Task t2 = Task.Factory.StartNew(() => StartAdditionalOrleansRuntimes(1));
            Task.WaitAll(new Task[] { t1, t2 }, ENDWAIT);

            Task.WaitAll(tasks, ENDWAIT); // Block until all tasks complete.
            log.Info("\n\n\nReminderTest_1F1J_MultiGrain passed OK.\n\n\n");
        }
        #endregion

        #region Register same reminder multiple times
        [TestMethod, TestCategory("Nightly"), TestCategory("ReminderService")]
        public void ReminderTest_RegisterSameReminderTwice()
        {
            log.Info("Testing RegisterSameReminderTwice");
            IReminderTestGrain grain = ReminderTestGrainFactory.GetGrain(1);
            Task<IOrleansReminder> promise1 = grain.StartReminder(DR);
            Task<IOrleansReminder> promise2 = grain.StartReminder(DR);
            Task[] acs = { promise1, promise2 };
            Task.WaitAll(acs, TimeSpan.FromSeconds(15));
            //Assert.AreNotEqual(promise1.Result, promise2.Result);
            // TODO: TMS write tests where period of a reminder is changed
        }
        #endregion

        #region Multiple grain types
        [TestMethod, TestCategory("Nightly"), TestCategory("ReminderService")]
        public void ReminderTest_GT_Basic()
        {
            log.Info("Testing GT_Basic");
            IReminderTestGrain g1 = ReminderTestGrainFactory.GetGrain(1);
            IReminderTestCopyGrain g2 = ReminderTestCopyGrainFactory.GetGrain(2);
            TimeSpan period = g1.GetReminderPeriod(DR).Result; // using same period

            g1.StartReminder(DR).Wait();
            Thread.Sleep(period.Multiply(2) + LEEWAY); // giving some leeway
            g2.StartReminder(DR).Wait();
            Thread.Sleep(period.Multiply(2) + LEEWAY); // giving some leeway
            long last1 = g1.GetCounter(DR).Result;
            Assert.AreEqual(4, last1, string.Format("{0} Grain fault", Time()));
            long last2 = g2.GetCounter(DR).Result;
            Assert.AreEqual(2, last2, string.Format("{0} CopyGrain fault", Time()));

            g1.StopReminder(DR).Wait();
            Thread.Sleep(period.Multiply(2) + LEEWAY); // giving some leeway
            g2.StopReminder(DR).Wait();
            long curr1 = g1.GetCounter(DR).Result;
            Assert.AreEqual(last1, curr1, string.Format("{0} Grain fault", Time()));
            long curr2 = g2.GetCounter(DR).Result;
            Assert.AreEqual(4, curr2, string.Format("{0} CopyGrain fault", Time()));
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("ReminderService")]
        public void ReminderTest_GT_1F1J_MultiGrain()
        {
            log.Info("Testing GT_1F1J_MultiGrain");
            List<SiloHandle> silos = StartAdditionalOrleansRuntimes(1);
            WaitForLivenessToStabilize();

            IReminderTestGrain g1 = ReminderTestGrainFactory.GetGrain(1);
            IReminderTestGrain g2 = ReminderTestGrainFactory.GetGrain(2);
            IReminderTestCopyGrain g3 = ReminderTestCopyGrainFactory.GetGrain(3);
            IReminderTestCopyGrain g4 = ReminderTestCopyGrainFactory.GetGrain(4);

            TimeSpan period = g1.GetReminderPeriod(DR).Result;

            Task[] tasks = new Task[]
            {
                Task.Factory.StartNew(() => PerGrainFailureTest(g1)),
                Task.Factory.StartNew(() => PerGrainFailureTest(g2)),
                Task.Factory.StartNew(() => PerCopyGrainFailureTest(g3)),
                Task.Factory.StartNew(() => PerCopyGrainFailureTest(g4)),
            };

            Thread.Sleep(period.Multiply(failAfter));

            // stop a silo and join a new one in parallel
            log.Info("Stopping a silo and joining a silo");
            Task t1 = Task.Factory.StartNew(() => StopRuntime/*KillRuntime*/(silos[new Random().Next(silos.Count)]));
            Task t2 = Task.Factory.StartNew(() => StartAdditionalOrleansRuntimes(1));
            Task.WaitAll(new Task[] { t1, t2 }, ENDWAIT);

            Task.WaitAll(tasks, ENDWAIT); // Block until all tasks complete.
        }

        private void PerCopyGrainFailureTest(IReminderTestCopyGrain grain)
        {
            TimeSpan period = grain.GetReminderPeriod(DR).Result;

            grain.StartReminder(DR).Wait();
            Thread.Sleep(period.Multiply(failCheckAfter) + LEEWAY); // giving some leeway
            long last = grain.GetCounter(DR).Result;
            Assert.AreEqual(failCheckAfter, last, string.Format("{0} CopyGrain {1} Reminder {2}", Time(), grain.GetPrimaryKeyLong(), DR));

            grain.StopReminder(DR).Wait();
            Thread.Sleep(period.Multiply(2) + LEEWAY); // giving some leeway
            long curr = grain.GetCounter(DR).Result;
            Assert.AreEqual(last, curr, string.Format("{0} CopyGrain {1} Reminder {2}", Time(), grain.GetPrimaryKeyLong(), DR));
        }
        #endregion

        // TODO: TMS implement more complicated tests that include join and failures, and potential race conditions        

        #region Testing things that should fail

        #region Lower than allowed reminder period
        [TestMethod, TestCategory("Nightly"), TestCategory("ReminderService")]
        [ExpectedException(typeof(ArgumentException), "Should not be possible to register a reminder with a period of 1 second.")]
        public async Task ReminderTest_Wrong_LowerThanAllowedPeriod()
        {
            log.Info("Testing Wrong_LowerThanAllowedPeriod");
            try
            {
                IReminderTestGrain grain = ReminderTestGrainFactory.GetGrain(1);
                await grain.StartReminder(DR, period: TimeSpan.FromMilliseconds(1000), validate: true);
            }
            catch (Exception exc)
            {
                log.Info("Failed to register reminder: {0}", exc.Message);
                throw exc.GetBaseException();
            }
        }
        #endregion

        #region The wrong reminder grain
        [TestMethod, TestCategory("Nightly"), TestCategory("ReminderService")]
        [ExpectedException(typeof(InvalidOperationException), "Should not be possible to register a reminder when the grain doesn't extend IRemindable.")]
        public void ReminderTest_Wrong_Grain()
        {
            log.Info("Testing Wrong_Grain");
            try
            {
                IReminderGrainWrong grain = ReminderGrainWrongFactory.GetGrain(0);
                bool success = grain.StartReminder(DR).Result; // should throw exception
                Assert.IsFalse(success);
            }
            catch (Exception exc)
            {
                log.Info("Failed to register reminder: {0}", exc.Message);
                throw exc.GetBaseException();
            }
        }
        #endregion

        #endregion

        private const long retries = 3;

        protected static string Time()
        {
            return DateTime.UtcNow.ToString("hh:mm:ss.fff");
        }

        protected static void ExecuteWithRetries(Func<string, TimeSpan?, bool, Task> function, string reminderName, TimeSpan? period = null, bool validate = false)
        {
            for (long i = 1; i <= retries; i++)
            {
                try
                {
                    function(reminderName, period, validate).Wait(); //.ContinueWith(task => { }); //.Wait();
                    return; // success ... no need to retry
                }
                catch (ReminderException exc)
                {
                    log.Info("Operation failed {0} on attempt {1}", exc, i);
                    Thread.Sleep(TimeSpan.FromMilliseconds(10)); // sleep a bit before retrying
                }
            }
        }
        // Func<> doesnt take optional parameters, thats why we need a separate method
        protected static void ExecuteWithRetriesStop(Func<string, Task> function, string reminderName)
        {
            for (long i = 1; i <= retries; i++)
            {
                try
                {
                    function(reminderName).Wait(); //.ContinueWith(task => { }); //.Wait();
                    return; // success ... no need to retry
                }
                catch (ReminderException exc)
                {
                    log.Info("Operation failed {0} on attempt {1}", exc.ToString(), i);
                    Thread.Sleep(TimeSpan.FromMilliseconds(10)); // sleep a bit before retrying
                }
            }
        }

        //[TestMethod]
        public async Task ReminderTest_AzureTableInsertRate()
        {
            IReminderTable table = await AzureBasedReminderTable.GetAzureBasedReminderTable("TMSLocalTesting", AZURE_DATA_CONNECTION_STRING);

            UnitTestBase.ConfigureClientThreadPoolSettingsForStorageTests(1000);

            TestTableInsertRate(table, 10);
            TestTableInsertRate(table, 500);
        }

        private void TestTableInsertRate(IReminderTable reminderTable, double numOfInserts)
        {
            DateTime startedAt = DateTime.UtcNow;

            try
            {
                List<AsyncCompletion> promises = new List<AsyncCompletion>();
                for (int i = 0; i < numOfInserts; i++)
                {
                    //"177BF46E-D06D-44C0-943B-C12F26DF5373"
                    string s = string.Format("177BF46E-D06D-44C0-943B-C12F26D{0:d5}", i);

                    var e = new ReminderEntry
                    {
                        //GrainId = GrainId.GetGrainId(new Guid(s)),
                        GrainId = GrainId.NewId(),
                        ReminderName = "MY_REMINDER_" + i,
                        Period = TimeSpan.FromSeconds(5),
                        StartAt = DateTime.UtcNow
                    };

                    int capture = i;
                    var promise1 = reminderTable.UpsertRow(e);
                    var promise2 = AsyncCompletion.FromTask(promise1).ContinueWith(() => Console.WriteLine("Done " + capture));
                    promises.Add(promise2);
                    log.Info("Started " + capture);
                    //promises.Add(promise1);
                }
                log.Info("Started all, now waiting...");
                AsyncCompletion.JoinAll(promises).Wait(TimeSpan.FromSeconds(500));
            }
            catch (Exception exc)
            {
                log.Info("Exception caught {0}", exc);
            }
            TimeSpan dur = DateTime.UtcNow - startedAt;
            log.Info("Inserted {0} rows in {1}, i.e., {2:f2} upserts/sec", numOfInserts, dur, (numOfInserts / dur.TotalSeconds));
        }
    }
}
