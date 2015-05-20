using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;

using SimpleGrain;
using UnitTestGrains;

#pragma warning disable 618

namespace UnitTests
{
    /// <summary>
    /// Summary description for ObserverTests
    /// </summary>
    [TestClass]
    public class ObserverTests : UnitTestBase
    {
        private readonly TimeSpan timeout = Debugger.IsAttached ? TimeSpan.FromMinutes(5) : TimeSpan.FromSeconds(10);
        private int callbackCounter;
        private readonly bool[] callbacksRecieved = new bool[2];

        // [mlr] we keep the observer objects as instance variables to prevent them from
        // being garbage collected permaturely (the runtime stores them as weak references).
        private SimpleGrainObserver observer1;
        private SimpleGrainObserver observer2;

        [ClassCleanup]
        public static void MyClassCleanup()
        {
            //ResetDefaultRuntimes();
        }

        /// <summary>
        ///Gets or sets the test context which provides
        ///information about and functionality for the current test run.
        ///</summary>
        public TestContext TestContext { get; set; }

        [TestInitialize]
        public void TestInitialize()
        {
            callbackCounter = 0;
            callbacksRecieved[0] = false;
            callbacksRecieved[1] = false;

            this.observer1 = null;
            this.observer2 = null;
        }

        #region Additional test attributes
        //
        // You can use the following additional attributes as you write your tests:
        //
        // Use ClassInitialize to run code before running the first test in the class
        // [ClassInitialize()]
        // public static void MyClassInitialize(TestContext testContext) { }
        //
        // Use ClassCleanup to run code after all tests in a class have run
        // [ClassCleanup()]
        // public static void MyClassCleanup() { }
        //
        // Use TestInitialize to run code before running each test 
        // [TestInitialize()]
        // public void MyTestInitialize() { }
        //
        // Use TestCleanup to run code after each test has run
        // [TestCleanup()]
        // public void MyTestCleanup() { }
        //
        #endregion

        [TestMethod, TestCategory("Nightly"), TestCategory("General"), TestCategory("Observers")]
        public async Task ObserverTest_SimpleNotification()
        {
            ResultHandle result = new ResultHandle();

            ISimpleGrain grain = SimpleGrainFactory.GetGrain(GetRandomGrainId(), "SimpleGrain");
            this.observer1 = new SimpleGrainObserver(ObserverTest_SimpleNotification_Callback, result);
            ISimpleGrainObserver reference = await SimpleGrainObserverFactory.CreateObjectReference(this.observer1);
            grain.Subscribe(reference).Wait();
            grain.SetA(3).Wait();
            grain.SetB(2).Wait();

            Assert.IsTrue(result.WaitForFinished(timeout), "Should not timeout waiting {0} for {1}", timeout, "SetB");

            SimpleGrainObserverFactory.DeleteObjectReference(reference);
        }

        //[TestMethod, TestCategory("Observers"), TestCategory("Failures")]
        public async Task ObserverTest_SimpleNotificationBrokenConnection()
        {
            ResultHandle result = new ResultHandle();

            ISimpleGrain grain = SimpleGrainFactory.GetGrain(GetRandomGrainId(), "SimpleGrain");
            this.observer1 = new SimpleGrainObserver(ObserverTest_SimpleNotification_Callback, result);
            ISimpleGrainObserver reference = await SimpleGrainObserverFactory.CreateObjectReference(this.observer1);
            grain.Subscribe(reference).Wait();
            grain.SetA(3).Wait();
            grain.SetB(2).Wait();
            ((OutsideGrainClient)GrainClient.InternalCurrent).Disconnect();
            Thread.Sleep(TimeSpan.FromSeconds(5));
            ((OutsideGrainClient)GrainClient.InternalCurrent).Reconnect();

            Assert.IsTrue(result.WaitForFinished(timeout), "Should not timeout waiting {0} for {1}", timeout, "Reconnect");

            SimpleGrainObserverFactory.DeleteObjectReference(reference);
        }

        void ObserverTest_SimpleNotification_Callback(int a, int b, ResultHandle result)
        {
            callbackCounter++;
            logger.Info("Invoking ObserverTest_SimpleNotification_Callback for {0} time with a = {1} and b = {2}", callbackCounter, a, b);

            if (a == 3 && b == 0) 
                callbacksRecieved[0] = true;
            else if (a == 3 && b == 2) 
                callbacksRecieved[1] = true;
            else 
                throw new ArgumentOutOfRangeException("Unexpected callback with values: a=" + a + ",b=" + b);

            if (callbackCounter == 1)
            {
                // Allow for callbacks occurring in any order
                Assert.IsTrue(callbacksRecieved[0] || callbacksRecieved[1], "Received one callback ok");
            }
            else if (callbackCounter == 2)
            {
                Assert.IsTrue(callbacksRecieved[0] && callbacksRecieved[1], "Received two callbacks ok");
                result.Done = true;
            }
            else
            {
                Assert.Fail("Callback has been called more times than was expected.");
            }
        }
        void ObserverTest_DoubleSubscriptionSameReference_Callback(int a, int b, ResultHandle result)
        {
            callbackCounter++;
            logger.Info("Invoking ObserverTest_DoubleSubscriptionSameReference_Callback for {0} time with a={1} and b={2}", callbackCounter, a, b);
            Assert.IsTrue(callbackCounter <= 2, "Callback has been called more times than was expected {0}", callbackCounter);

            if (callbackCounter == 2)
            {
                result.Continue = true;
            }
        }


        [TestMethod, TestCategory("Nightly"), TestCategory("General"), TestCategory("Observers")]
        public async Task ObserverTest_DoubleSubscriptionSameReference()
        {
            ResultHandle result = new ResultHandle();

            ISimpleGrain grain = SimpleGrainFactory.GetGrain(GetRandomGrainId(), "SimpleGrain");
            this.observer1 = new SimpleGrainObserver(ObserverTest_DoubleSubscriptionSameReference_Callback, result);
            ISimpleGrainObserver reference = await SimpleGrainObserverFactory.CreateObjectReference(this.observer1);
            grain.Subscribe(reference).Wait();
            grain.SetA(1).Wait(); // Use grain
            try
            {
                bool ok = grain.Subscribe(reference).Wait(timeout);
                if (!ok) throw new TimeoutException();
            }
            catch (TimeoutException)
            {
                throw;
            }
            catch (Exception exc)
            {
                Exception baseException = exc.GetBaseException();
                Console.WriteLine("Received exception: {0}", baseException);
                Assert.IsInstanceOfType(baseException, typeof(OrleansException));
                if (!baseException.Message.StartsWith("Cannot subscribe already subscribed observer"))
                {
                    Assert.Fail("Unexpected exception message: " + baseException);
                }
            }
            grain.SetA(2).Wait(); // Use grain

            Assert.IsFalse(result.WaitForFinished(timeout), "Should timeout waiting {0} for {1}", timeout, "SetA(2)");

            SimpleGrainObserverFactory.DeleteObjectReference(reference);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("General"), TestCategory("Observers")]
        public async Task ObserverTest_SubscribeUnsubscribe()
        {
            ResultHandle result = new ResultHandle();

            ISimpleGrain grain = SimpleGrainFactory.GetGrain(GetRandomGrainId(), "SimpleGrain");
            this.observer1 = new SimpleGrainObserver(ObserverTest_SubscribeUnsubscribe_Callback, result);
            ISimpleGrainObserver reference = await SimpleGrainObserverFactory.CreateObjectReference(this.observer1);
            grain.Subscribe(reference).Wait();
            grain.SetA(5).Wait();
            Assert.IsTrue(result.WaitForContinue(timeout), "Should not timeout waiting {0} for {1}", timeout, "SetA");
            grain.Unsubscribe(reference).Wait();
            grain.SetB(3).Wait();

            Assert.IsFalse(result.WaitForFinished(timeout), "Should timeout waiting {0} for {1}", timeout, "SetB");

            SimpleGrainObserverFactory.DeleteObjectReference(reference);
        }

        void ObserverTest_SubscribeUnsubscribe_Callback(int a, int b, ResultHandle result)
        {
            callbackCounter++;
            logger.Info("Invoking ObserverTest_SubscribeUnsubscribe_Callback for {0} time with a = {1} and b = {2}", callbackCounter, a, b);
            Assert.IsTrue(callbackCounter < 2, "Callback has been called more times than was expected.");

            Assert.AreEqual(5, a);
            Assert.AreEqual(0, b);

            result.Continue = true;
        }


        [TestMethod, TestCategory("Nightly"), TestCategory("General"), TestCategory("Observers")]
        public async Task ObserverTest_Unsubscribe()
        {
            ISimpleGrain grain = SimpleGrainFactory.GetGrain(GetRandomGrainId(), "SimpleGrain");
            this.observer1 = new SimpleGrainObserver(null, null);
            ISimpleGrainObserver reference = await SimpleGrainObserverFactory.CreateObjectReference(this.observer1);
            try
            {
                bool ok = grain.Unsubscribe(reference).Wait(timeout);
                if (!ok) throw new TimeoutException();

                SimpleGrainObserverFactory.DeleteObjectReference(reference);
            }
            catch (TimeoutException)
            {
                throw;
            }
            catch (Exception exc)
            {
                Exception baseException = exc.GetBaseException();
                if (!(baseException is OrleansException))
                    Assert.Fail("Unexpected exception type {0}", baseException);
            }
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("General"), TestCategory("Observers")]
        public async Task ObserverTest_DoubleSubscriptionDifferentReferences()
        {
            ResultHandle result = new ResultHandle();

            ISimpleGrain grain = SimpleGrainFactory.GetGrain(GetRandomGrainId(), "SimpleGrain");
            this.observer1 = new SimpleGrainObserver(ObserverTest_DoubleSubscriptionDifferentReferences_Callback, result);
            ISimpleGrainObserver reference1 = await SimpleGrainObserverFactory.CreateObjectReference(this.observer1);
            this.observer2 = new SimpleGrainObserver(ObserverTest_DoubleSubscriptionDifferentReferences_Callback, result);
            ISimpleGrainObserver reference2 = await SimpleGrainObserverFactory.CreateObjectReference(this.observer2);
            grain.Subscribe(reference1).Wait();
            grain.Subscribe(reference2).Wait();
            grain.SetA(6).Ignore();
            
            Assert.IsTrue(result.WaitForFinished(timeout), "Should not timeout waiting {0} for {1}", timeout, "SetA");

            SimpleGrainObserverFactory.DeleteObjectReference(reference1);
            SimpleGrainObserverFactory.DeleteObjectReference(reference2);
        }

        void ObserverTest_DoubleSubscriptionDifferentReferences_Callback(int a, int b, ResultHandle result)
        {
            callbackCounter++;
            logger.Info("Invoking ObserverTest_DoubleSubscriptionDifferentReferences_Callback for {0} time with a = {1} and b = {2}", callbackCounter, a, b);
            Assert.IsTrue(callbackCounter < 3, "Callback has been called more times than was expected.");

            Assert.AreEqual(6, a);
            Assert.AreEqual(0, b);

            if(callbackCounter == 2)
                result.Done= true;
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("General"), TestCategory("Observers")]
        public async Task ObserverTest_DeleteObject()
        {
            ResultHandle result = new ResultHandle();

            ISimpleGrain grain = SimpleGrainFactory.GetGrain(GetRandomGrainId(), "SimpleGrain");
            this.observer1 = new SimpleGrainObserver(ObserverTest_DeleteObject_Callback, result);
            ISimpleGrainObserver reference = await SimpleGrainObserverFactory.CreateObjectReference(this.observer1);
            grain.Subscribe(reference).Wait();
            grain.SetA(5).Wait();
            Assert.IsTrue(result.WaitForContinue(timeout), "Should not timeout waiting {0} for {1}", timeout, "SetA");
            SimpleGrainObserverFactory.DeleteObjectReference(reference);
            grain.SetB(3).Wait();

            Assert.IsFalse(result.WaitForFinished(timeout), "Should timeout waiting {0} for {1}", timeout, "SetB");
        }

        void ObserverTest_DeleteObject_Callback(int a, int b, ResultHandle result)
        {
            callbackCounter++;
            logger.Info("Invoking ObserverTest_DeleteObject_Callback for {0} time with a = {1} and b = {2}", callbackCounter, a, b);
            Assert.IsTrue(callbackCounter < 2, "Callback has been called more times than was expected.");

            Assert.AreEqual(5, a);
            Assert.AreEqual(0, b);

            result.Continue = true;
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("Observers")]
        public async Task ObserverTest_GrainObserver()
        {
            ResultHandle result = new ResultHandle();

            ISimpleGrain grain = SimpleGrainFactory.GetGrain(GetRandomGrainId(), "SimpleGrain");
            IObserverGrain observer = ObserverGrainFactory.GetGrain(GetRandomGrainId());
            observer.SetTarget(grain).Wait();
            this.observer1 = new SimpleGrainObserver(ObserverTest_GrainObserver_Callback, result);
            ISimpleGrainObserver reference = await SimpleGrainObserverFactory.CreateObjectReference(this.observer1);
            observer.Subscribe(reference).Wait();
            grain.SetA(3).Wait();
            grain.SetB(2).Wait();

            Assert.IsTrue(result.WaitForFinished(timeout), "Should not timeout waiting {0} for {1}", timeout, "SetB");

            SimpleGrainObserverFactory.DeleteObjectReference(reference);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("General"), TestCategory("Observers")]
        [ExpectedException(typeof(NotSupportedException))]
        public void ObserverTest_SubscriberMustBeGrainReference()
        {
            ResultHandle result = new ResultHandle();

            ISimpleGrain grain = SimpleGrainFactory.GetGrain(GetRandomGrainId(), "SimpleGrain");
            this.observer1 = new SimpleGrainObserver(ObserverTest_SimpleNotification_Callback, result);
            ISimpleGrainObserver reference = this.observer1; // Should be: SimpleGrainObserverFactory.CreateObjectReference(obj);
            grain.Subscribe(reference).Wait();
            // Not reached
        }

        void ObserverTest_GrainObserver_Callback(int a, int b, ResultHandle result)
        {
            lock (result) // This gets called from within a TPL task, not within an Orleans turn, and so needs a lock
            {
                logger.Info("Invoking ObserverTest_GrainObserver_Callback with a = {0} and b = {1}", a, b);
                ObserverTest_SimpleNotification_Callback(a, b, result);
            }
        }

        [TestMethod, TestCategory("Revisit"), TestCategory("Observers")]
        public void ObserverTest_Disconnect()
        {
            ObserverTest_Disconnect(false);
        }

        [TestMethod, TestCategory("Revisit"), TestCategory("Observers")]
        public void ObserverTest_Disconnect2()
        {
            ObserverTest_Disconnect(true);
        }

        public void ObserverTest_Disconnect(bool observeTwice)
        {
            // this is for manual repro & validation in the debugger
            // wait to send event because it takes 60s to drop client grain
            //var simple1 = SimpleGrainFactory.GetGrain(GetRandomGrainId(), "SimpleGrain");
            //var simple2 = SimpleGrainFactory.Cast(Domain.Current.Create(typeof(ISimpleGrain).FullName,
            //    new Dictionary<string, object> { { "EventDelay", 70000 } }));
            //var result = new ResultHandle();
            //var callback = new SimpleGrainObserver((a, b, r) =>
            //{
            //    r.Done = (a == 10);
            //    Console.WriteLine("Received observer callback: A={0} B={1} Done={2}", a, b, r.Done);
            //}, result);
            //var observer = SimpleGrainObserverFactory.CreateObjectReference(callback);
            //if (observeTwice)
            //{
            //    simple1.Subscribe(observer).Wait();
            //    simple1.SetB(1).Wait(); // send a message to the observer to get it in the cache
            //}
            //simple2.Subscribe(observer).Wait();
            //simple2.SetA(10).Wait();
            //Thread.Sleep(2000);
            //OrleansClient.Uninitialize();
            //var timeout80sec = TimeSpan.FromSeconds(80);
            //Assert.IsFalse(result.WaitForFinished(timeout80sec), "WaitforFinished Timeout=" + timeout80sec);
            //// prevent silo from shutting down right away
            //Thread.Sleep(Debugger.IsAttached ? TimeSpan.FromMinutes(2) : TimeSpan.FromSeconds(5));
        }

        internal class SimpleGrainObserver : ISimpleGrainObserver
        {
            readonly Action<int, int, ResultHandle> action;
            readonly ResultHandle result;

            public SimpleGrainObserver(Action<int, int, ResultHandle> action, ResultHandle result)
            {
                this.action = action;
                this.result = result;
            }

            #region ISimpleGrainObserver Members

            public void StateChanged(int a, int b)
            {
                Console.WriteLine("SimpleGrainObserver.StateChanged a={0} b={1}", a, b);
                if (action != null)
                {
                    action(a, b, result);
                }
            }

            #endregion
        }
    }
}

#pragma warning restore 618
