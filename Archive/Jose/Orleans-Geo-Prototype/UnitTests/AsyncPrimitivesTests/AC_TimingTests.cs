using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using System.Diagnostics;
using System.Threading;
using Orleans.Scheduler;


#pragma warning disable 618

namespace UnitTests
{
    /// <summary>
    /// Summary description for UnitTest1
    /// </summary>
    [TestClass]
    public class AC_TimingTests
    {
        private Logger logger = Logger.GetLogger("AC_TimingTests", Logger.LoggerType.Application);

        public AC_TimingTests()
        {
            logger.Info("----------------------------- STARTING AC_TimingTests -------------------------------------");
        }

        [ClassInitialize()]
        public static void MyClassInitialize(TestContext testContext)
        {
            Logger.Initialize(ClientConfiguration.StandardLoad());
        }

        private bool prevTrackObservations;

        [TestInitialize]
        public void TestInitialize()
        {
            prevTrackObservations = AsyncCompletion.TrackObservations;
            AsyncCompletion.TrackObservations = true;
            UnitTestBase.CheckForUnobservedPromises();
            OrleansTask.Reset();
        }

        [TestCleanup]
        public void TestCleanup()
        {
            UnitTestBase.CheckForUnobservedPromises();
            AsyncCompletion.TrackObservations = prevTrackObservations;
            OrleansTask.Reset();
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_WithTimeout_1()
        {
            TimeSpan timeout = TimeSpan.FromMilliseconds(2000);
            TimeSpan sleepTime = TimeSpan.FromMilliseconds(4000);
            TimeSpan delta = TimeSpan.FromMilliseconds(200);
            Stopwatch watch = new Stopwatch();
            watch.Start();

            AsyncValue<int> promise = AsyncValue<int>.StartNew(() =>
                {
                    Thread.Sleep(sleepTime);
                    return 5;
                }).WithTimeout(timeout);

            bool hasThrown = false;
            try
            {
                promise.Wait();
            }
            catch (Exception exc)
            {
                hasThrown = true;
                Assert.IsTrue(exc.GetBaseException().GetType().Equals(typeof(TimeoutException)), exc.ToString());
            }
            watch.Stop();

            Assert.IsTrue(hasThrown);
            Assert.IsTrue(watch.Elapsed >= timeout - delta, watch.Elapsed.ToString());
            Assert.IsTrue(watch.Elapsed <= timeout + delta, watch.Elapsed.ToString());
            Assert.IsTrue(watch.Elapsed < sleepTime, watch.Elapsed.ToString());
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_DelayedExecutor_1()
        {
            TimeSpan delay = TimeSpan.FromMilliseconds(2000);
            Stopwatch watch = new Stopwatch();
            TimeSpan delta = TimeSpan.FromMilliseconds(200);
            watch.Start();

            AsyncCompletion promise = DelayedExecutor.Execute(() =>
            {
                return AsyncValue<int>.StartNew(() =>
                {
                    return 5;
                });
            }, delay);
    
            promise.Wait();
            watch.Stop();

            Assert.IsTrue(watch.Elapsed >= delay - delta, watch.Elapsed.ToString());
            Assert.IsTrue(watch.Elapsed <= delay + delta, watch.Elapsed.ToString());
        }
    }
}

#pragma warning restore 618

