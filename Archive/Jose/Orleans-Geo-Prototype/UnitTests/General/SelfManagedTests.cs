using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using UnitTestGrainInterfaces;

#pragma warning disable 618

namespace UnitTests.General
{
    [TestClass]
    public class SelfManagedTests : UnitTestBase
    {

        public SelfManagedTests()
            : base(true)
        {
        }

        public SelfManagedTests(int dummy)
            : base(new Options { StartPrimary = true, StartSecondary = false, StartClient = true })
        {
        }

        public SelfManagedTests(bool startClientOnly)
            : base(startClientOnly ?
                        new Options { StartPrimary = false, StartSecondary = false, StartClient = true } :
                        new Options { StartFreshOrleans = true })
        {
        }

        // Use ClassInitialize to run code before running the first test in the class
        [ClassInitialize]
        public static void MyClassInitialize(TestContext testContext)
        {
            ResetDefaultRuntimes();
        }

        // Use ClassCleanup to run code after all tests in a class have run
        [ClassCleanup]
        public static void MyClassCleanup()
        {
            ResetDefaultRuntimes();
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public void SelfManaged_ActivateAndUpdate()
        {
            ISimpleSelfManagedGrain g1 = SimpleSelfManagedGrainFactory.GetGrain(1);
            ISimpleSelfManagedGrain g2 = SimpleSelfManagedGrainFactory.GetGrain(2);
            Assert.AreEqual(1L, g1.GetPrimaryKeyLong());
            Assert.AreEqual(1L, g1.GetKey().Result);
            Assert.AreEqual("1", g1.GetLabel().Result);
            Assert.AreEqual(2L, g2.GetKey().Result);
            Assert.AreEqual("2", g2.GetLabel().Result);

            g1.SetLabel("one").Wait();
            Assert.AreEqual("one", g1.GetLabel().Result);
            Assert.AreEqual("2", g2.GetLabel().Result);

            ISimpleSelfManagedGrain g1a = SimpleSelfManagedGrainFactory.GetGrain(1);
            Assert.AreEqual("one", g1a.GetLabel().Result);
        }
        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public void SelfManaged_Guid_ActivateAndUpdate()
        {
            Guid guid1 = Guid.NewGuid();
            Guid guid2 = Guid.NewGuid();

            IGuidSimpleSelfManagedGrain g1 = GuidSimpleSelfManagedGrainFactory.GetGrain(guid1);
            IGuidSimpleSelfManagedGrain g2 = GuidSimpleSelfManagedGrainFactory.GetGrain(guid2);
            Assert.AreEqual(guid1, g1.GetPrimaryKey());
            Assert.AreEqual(guid1, g1.GetKey().Result);
            Assert.AreEqual(guid1.ToString(), g1.GetLabel().Result);
            Assert.AreEqual(guid2, g2.GetKey().Result);
            Assert.AreEqual(guid2.ToString(), g2.GetLabel().Result);

            g1.SetLabel("one").Wait();
            Assert.AreEqual("one", g1.GetLabel().Result);
            Assert.AreEqual(guid2.ToString(), g2.GetLabel().Result);

            IGuidSimpleSelfManagedGrain g1a = GuidSimpleSelfManagedGrainFactory.GetGrain(guid1);
            Assert.AreEqual("one", g1a.GetLabel().Result);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public void SelfManaged_Fail()
        {
            bool failed;
            long key = 0;
            try
            {
                // Key values of -2 are not allowed in this case
                ISimpleSelfManagedGrain fail = SimpleSelfManagedGrainFactory.GetGrain(-2);
                key = fail.GetKey().Result;
                failed = false;
            }
            catch (Exception e)
            {
                Assert.IsInstanceOfType(e.GetBaseException(), typeof(OrleansException));
                failed = true;
            }

            if (!failed) Assert.Fail("Should have failed, but instead returned " + key);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public void SelfManaged_ULong_MaxValue()
        {
            ulong key1AsUlong = UInt64.MaxValue; // == -1L
            long key1 = (long)key1AsUlong;

            ISimpleSelfManagedGrain g1 = SimpleSelfManagedGrainFactory.GetGrain(key1);
            Assert.AreEqual(key1, g1.GetPrimaryKeyLong());
            Assert.AreEqual((long)key1AsUlong, g1.GetPrimaryKeyLong());
            Assert.AreEqual(key1, g1.GetKey().Result);
            Assert.AreEqual(key1.ToString(CultureInfo.InvariantCulture), g1.GetLabel().Result);

            g1.SetLabel("MaxValue").Wait();
            Assert.AreEqual("MaxValue", g1.GetLabel().Result);

            ISimpleSelfManagedGrain g1a = SimpleSelfManagedGrainFactory.GetGrain((long)key1AsUlong);
            Assert.AreEqual("MaxValue", g1a.GetLabel().Result);
            Assert.AreEqual(key1, g1a.GetPrimaryKeyLong());
            Assert.AreEqual((long)key1AsUlong, g1a.GetKey().Result);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public void SelfManaged_ULong_MinValue()
        {
            ulong key1AsUlong = UInt64.MinValue; // == zero
            long key1 = (long)key1AsUlong;

            ISimpleSelfManagedGrain g1 = SimpleSelfManagedGrainFactory.GetGrain(key1);
            Assert.AreEqual(key1, g1.GetPrimaryKeyLong());
            Assert.AreEqual((long)key1AsUlong, g1.GetPrimaryKeyLong());
            Assert.AreEqual(key1, g1.GetPrimaryKeyLong());
            Assert.AreEqual(key1, g1.GetKey().Result);
            Assert.AreEqual(key1.ToString(CultureInfo.InvariantCulture), g1.GetLabel().Result);

            g1.SetLabel("MinValue").Wait();
            Assert.AreEqual("MinValue", g1.GetLabel().Result);

            ISimpleSelfManagedGrain g1a = SimpleSelfManagedGrainFactory.GetGrain((long)key1AsUlong);
            Assert.AreEqual("MinValue", g1a.GetLabel().Result);
            Assert.AreEqual(key1, g1a.GetPrimaryKeyLong());
            Assert.AreEqual((long)key1AsUlong, g1a.GetKey().Result);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public void SelfManaged_Long_MaxValue()
        {
            long key1 = Int32.MaxValue;
            ulong key1AsUlong = (ulong)key1;

            ISimpleSelfManagedGrain g1 = SimpleSelfManagedGrainFactory.GetGrain(key1);
            Assert.AreEqual(key1, g1.GetPrimaryKeyLong());
            Assert.AreEqual((long)key1AsUlong, g1.GetPrimaryKeyLong());
            Assert.AreEqual(key1, g1.GetKey().Result);
            Assert.AreEqual(key1.ToString(CultureInfo.InvariantCulture), g1.GetLabel().Result);

            g1.SetLabel("MaxValue").Wait();
            Assert.AreEqual("MaxValue", g1.GetLabel().Result);

            ISimpleSelfManagedGrain g1a = SimpleSelfManagedGrainFactory.GetGrain((long)key1AsUlong);
            Assert.AreEqual("MaxValue", g1a.GetLabel().Result);
            Assert.AreEqual(key1, g1a.GetPrimaryKeyLong());
            Assert.AreEqual((long)key1AsUlong, g1a.GetKey().Result);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public void SelfManaged_Long_MinValue()
        {
            long key1 = Int64.MinValue;
            ulong key1AsUlong = (ulong)key1;

            ISimpleSelfManagedGrain g1 = SimpleSelfManagedGrainFactory.GetGrain(key1);
            Assert.AreEqual((long)key1AsUlong, g1.GetPrimaryKeyLong());
            Assert.AreEqual(key1, g1.GetPrimaryKeyLong());
            Assert.AreEqual(key1, g1.GetKey().Result);
            Assert.AreEqual(key1.ToString(CultureInfo.InvariantCulture), g1.GetLabel().Result);

            g1.SetLabel("MinValue").Wait();
            Assert.AreEqual("MinValue", g1.GetLabel().Result);

            ISimpleSelfManagedGrain g1a = SimpleSelfManagedGrainFactory.GetGrain((long)key1AsUlong);
            Assert.AreEqual("MinValue", g1a.GetLabel().Result);
            Assert.AreEqual(key1, g1a.GetPrimaryKeyLong());
            Assert.AreEqual((long)key1AsUlong, g1a.GetKey().Result);
        }

        [TestMethod, TestCategory("Revisit"), TestCategory("General")]
        public void SelfManaged_Placement()
        {
            //IProxyGrain proxy0 = ProxyGrainFactory.CreateGrain(new[] { GrainStrategy.PartitionPlacement(0) });
            //IProxyGrain proxy1 = ProxyGrainFactory.CreateGrain(new[] { GrainStrategy.PartitionPlacement(1) });

            //proxy0.CreateProxy(1).Wait(;)
            //proxy1.CreateProxy(1).Wait();

            //var silo0 = proxy0.GetRuntimeInstanceId().Result;
            //var proxySilo0 = proxy0.GetProxyRuntimeInstanceId().Result;
            //var silo1 = proxy1.GetRuntimeInstanceId().Result;
            //var proxySilo1 = proxy1.GetProxyRuntimeInstanceId().Result;

            //if (silo0.Equals(silo1))
            //    Assert.Inconclusive("Only one active silo, cannot test placement");

            //Assert.AreEqual(silo0, proxySilo0, "Self-managed grain should be created on local silo");
            //Assert.AreEqual(silo1, proxySilo1, "Self-managed grain should be created on local silo");
        }

        //[TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        //public void SelfManaged_Observer()
        //{
        //    var simple = SimpleSelfManagedGrainFactory.GetGrain(3);
        //    simple.SetLabel("one").Wait();
        //    var stream = Stream_OLDFactory.Cast(simple);
        //    stream.Wait();
        //    stream.Next("+two");
        //    Thread.Sleep(100); // wait for stream message to propagate
        //    var label = simple.GetLabel().Result;
        //    Assert.AreEqual("one+two", label, "Stream message should have arrived");
        //}

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public void SelfManaged_MultipleGrainInterfaces()
        {
            ISimpleSelfManagedGrain simple = SimpleSelfManagedGrainFactory.GetGrain(50);

            simple.GetMultipleGrainInterfaces_List().Wait();
            logger.Info("GetMultipleGrainInterfaces_List() worked");

            simple.GetMultipleGrainInterfaces_Array().Wait();

            logger.Info("GetMultipleGrainInterfaces_Array() worked");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public void SelfManaged_Reentrant_RecoveryAfterExpiredMessage()
        {
            List<Task> promises = new List<Task>();
            TimeSpan prevTimeout = GrainClient.Current.GetResponseTimeout();

            // set short response time and ask to do long operation, to trigger expired msgs in the silo queues.
            TimeSpan shortTimeout = TimeSpan.FromMilliseconds(1000);
            GrainClient.Current.SetResponseTimeout(shortTimeout);

            ISimpleSelfManagedGrain grain = SimpleSelfManagedGrainFactory.GetGrain(12);
            int num = 10;
            for (long i = 0; i < num; i++)
            {
                Task task = grain.DoLongAction(shortTimeout.Multiply(3), "A_" + i);
                promises.Add(task);
            }
            try
            {
                Task.WhenAll(promises).Wait();
            }catch(Exception)
            {
                logger.Info("Done with stress iteration.");
            }

            // wait a bit to make sure expired msgs in the silo is trigered.
            Thread.Sleep(TimeSpan.FromSeconds(10));

            // set the regular response time back, expect msgs ot succeed.
            GrainClient.Current.SetResponseTimeout(prevTimeout);

            logger.Info("About to send a next legit request that should succeed.");
            grain.DoLongAction(TimeSpan.FromMilliseconds(1), "B_" + 0).Wait();
            logger.Info("The request succeeded.");
        }

        [TestMethod, TestCategory("Revisit"), TestCategory("General")]
        public void SelfManaged_Deactivate()
        {
            //for (long i = 0; i < 100; i += 2)
            //{
            //    long key1 = i;
            //    long key2 = key1 + 1;
            //    ISimpleSelfManagedGrain g1 = SimpleSelfManagedGrainFactory.GetGrain(key1);
            //    ISimpleSelfManagedGrain g2 = SimpleSelfManagedGrainFactory.GetGrain(key2);    
            //    g1.SetLabel("AAA").Wait();
            //    g2.SetLabel("BBB").Wait();

            //    AsyncCompletion promise = g1.Deactivate(key2);
            //    promise.Wait();

            //    string label = g2.GetLabel().Result;
            //    Assert.AreEqual(key2.ToString(), label); // Previous label got lost, since activation was deactivated.
            //    logger.Info("Iteration {0} of SelfManaged_Deactivate passed OK.", i);
            //}
        }

        [TestMethod, TestCategory("Revisit"), TestCategory("General")]
        public void SelfManaged_Delete()
        {
            //for (long i = 0; i < 100; i++)
            //{
            //    try
            //    {
            //        ISimpleSelfManagedGrain g1 = SimpleSelfManagedGrainFactory.GetGrain(i);
            //        g1.SetLabel("AAA-" + i).Wait();

            //        AsyncCompletion promise = Domain.Current.Delete(typeof(ISimpleSelfManagedGrain).FullName, g1);
            //        promise.Wait();
            //        Assert.Fail("Delete of self-managed grain should have failed");
            //    }
            //    catch (Exception exc)
            //    {
            //        exc = exc.GetBaseException();
            //        Assert.IsInstanceOfType(exc, typeof(InvalidOperationException));
            //        Assert.AreEqual("Cannot delete self-managed grain", exc.Message, "Exception failure message");
            //    }
            //}
        }

        public static void SelfManaged_StressClient_Runner()
        {
            SelfManagedTests test = new SelfManagedTests();
            test.SelfManaged_StressClient();
        }

        // ReSharper disable FunctionNeverReturns
        public void SelfManaged_StressClient()
        {
            Stopwatch st = new Stopwatch();
            st.Start();
            int iteration = 0;
            List<AsyncCompletion> promises = new List<AsyncCompletion>();

            //while (true)
            {
                int numGrains = 100;
                int numErrors = 0;
                long lastElapsed = st.ElapsedMilliseconds;
                for (long i = 0; i < numGrains; i++)
                {
                    //IReentrantStressSelfManagedGrain g1 = ReentrantStressSelfManagedGrainFactory.GetGrain(i);
                    //AsyncValue<GrainId> cwPromise = g1.GetGrainId();
                    //GrainId id = cwPromise.Result;
                    //int hashCode1 = id.GetHashCode();
                    //int hashCode2 = id.GetHashCode();
                    //logger.Info("GrainId = " + id + ". GrainId.GetHashCode() = " + hashCode1);
                    //logger.Info("FullGrainId = " + id.ToFullString());

                    //ActivationAddress address = ActivationAddress.NewActivationAddress(SiloAddress.New(new IPEndPoint(IPAddress.Any, 11111), 89898989), GrainId.NewId());
                    //logger.Info("FullActivationAddress = " + address.ToFullString());

                    ISimpleSelfManagedGrain g1 = SimpleSelfManagedGrainFactory.GetGrain(i);
                    AsyncCompletion cwPromise = AsyncCompletion.FromTask(g1.SetLabel("A_" + i)).ContinueWith(() => { },
                        (Exception exc) =>
                        {
                            numErrors++;
                            logger.Error(1, "SelfManaged_StressClient_Runner got exception ", exc);
                        });

                    promises.Add(cwPromise);
                    cwPromise.Wait();
                }
                AsyncCompletion.JoinAll(promises).Wait();
                long elapsed = st.ElapsedMilliseconds - lastElapsed;
                logger.Info("Iteration {0} of SelfManaged_StressClient of {1} operations took {2} milliseconds, TPS={3:00} per sec. {4}",
                        iteration, numGrains, elapsed, ((double)numGrains) * 1000.0 / ((double)elapsed), numErrors > 0 ? " numErrors = " + numErrors : "");
                iteration++;
            }
        }
        // ReSharper restore FunctionNeverReturns

        // TODO: [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        [TestMethod]
        public void SelfManaged_MissingActivation_1()
        {
            for (int i = 0; i < 10; i++)
            {
                SelfManaged_MissingActivation_Runner(i, false);
            }
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("General")]
        public void SelfManaged_MissingActivation_2()
        {
            for (int i = 0; i < 10; i++)
            {
                SelfManaged_MissingActivation_Runner(i, true);
            }
        }

        private void SelfManaged_MissingActivation_Runner(int grainId, bool DoLazyDeregistration)
        {
            IStressSelfManagedGrain g = StressSelfManagedGrainFactory.GetGrain(grainId);
            g.SetLabel("hello_" + grainId).Wait();
            var grain = g.GetGrainId().Result;

            // Call again to make sure the grain is in all silo caches
            for (int i = 0; i < 10; i++)
            {
                var label = g.GetLabel().Result;
            }

            TimeSpan LazyDeregistrationDelay;
            if (DoLazyDeregistration)
            {
                LazyDeregistrationDelay = TimeSpan.FromSeconds(2);
                // disable retries in this case, to make test more predictable.
                Primary.Silo.TestHookup.SetMaxForwardCount_ForTesting(0);
                Secondary.Silo.TestHookup.SetMaxForwardCount_ForTesting(0);
            }
            else
            {
                LazyDeregistrationDelay = TimeSpan.FromMilliseconds(-1);
                Primary.Silo.TestHookup.SetMaxForwardCount_ForTesting(0);
                Secondary.Silo.TestHookup.SetMaxForwardCount_ForTesting(0);
            }
            Primary.Silo.TestHookup.SetDirectoryLazyDeregistrationDelay_ForTesting(LazyDeregistrationDelay);
            Secondary.Silo.TestHookup.SetDirectoryLazyDeregistrationDelay_ForTesting(LazyDeregistrationDelay);

            // Now we know that there's an activation; try both silos and deactivate it incorrectly
            int primaryActivation = Primary.Silo.TestHookup.UnregisterGrainForTesting(grain);
            int secondaryActivation = Secondary.Silo.TestHookup.UnregisterGrainForTesting(grain);
            Assert.AreEqual(1, primaryActivation + secondaryActivation, "Test deactivate didn't find any activations");

            // If we try again, we shouldn't find any
            primaryActivation = Primary.Silo.TestHookup.UnregisterGrainForTesting(grain);
            secondaryActivation = Secondary.Silo.TestHookup.UnregisterGrainForTesting(grain);
            Assert.AreEqual(0, primaryActivation + secondaryActivation, "Second test deactivate found an activation");

            //g1.DeactivateSelf().Wait();
            // Now send a message again; it should fail);
            try
            {
                var newLabel = g.GetLabel().Result;
                logger.Info("After 1nd call. newLabel = " + newLabel);
                Assert.Fail("First message after incorrect deregister should fail!");
            }
            catch (Exception exc)
            {
                logger.Info("Got 1st exception - " + exc.GetBaseException().Message);
                Exception baseExc = exc.GetBaseException();
                if (baseExc is AssertFailedException) throw;
                Assert.IsInstanceOfType(baseExc, typeof(OrleansException), "Unexpected exception type: " + baseExc);
                // Expected
                Assert.IsTrue(baseExc.Message.Contains("Non-existent activation"), "1st exception message");
                logger.Info("Got 1st Non-existent activation Exception, as expected.");
            }

            if (DoLazyDeregistration)
            {
                // Wait a bit
                TimeSpan pause = LazyDeregistrationDelay + TimeSpan.FromSeconds(1);
                logger.Info("Pausing for {0} because DoLazyDeregistration={1}", pause, DoLazyDeregistration);
                Thread.Sleep(pause);
            }

            // Try again; it should succeed or fail, based on DoLazyDeregistration
            try
            {
                var newLabel = g.GetLabel().Result;
                logger.Info("After 2nd call. newLabel = " + newLabel);

                if (!DoLazyDeregistration)
                {
                    Assert.Fail("Exception should have been thrown when DoLazyDeregistration=" + DoLazyDeregistration);
                }
            }
            catch (Exception exc)
            {
                logger.Info("Got 2nd exception - " + exc.GetBaseException().Message);
                if (DoLazyDeregistration)
                {
                    Assert.Fail("Second message after incorrect deregister failed, while it should have not! Exception=" + exc);
                }
                else
                {
                    Exception baseExc = exc.GetBaseException();
                    if (baseExc is AssertFailedException) throw;
                    Assert.IsInstanceOfType(baseExc, typeof(OrleansException), "Unexpected exception type: " + baseExc);
                    // Expected
                    Assert.IsTrue(baseExc.Message.Contains("Non-existent activation"), "2nd exception message");
                    logger.Info("Got 2nd Non-existent activation Exception, as expected.");
                }
            }
        }

        public void SimpleSelfManagedGrain_BatchStress()
        {
            List<AsyncCompletion> list = new List<AsyncCompletion>();
            for (int i = 0; i < 100; i++)
            {
                int capture = i;
                int grainId = GetRandomGrainId();
                ISimpleSelfManagedGrain grain = SimpleSelfManagedGrainFactory.GetGrain(grainId);
                AsyncCompletion setPromise = AsyncCompletion.FromTask(grain.SetLabel("Soramichi"));
                AsyncCompletion contPromise = setPromise.ContinueWith(() =>
                {
                    logger.Info("Call {0} to {1} has finished.", capture, grainId);
                });
                list.Add(contPromise);
            }
            AsyncCompletion.JoinAll(list).Wait();
        }

        public void SelfManaged_TestRequestContext()
        {
            ISimpleSelfManagedGrain g1 = SimpleSelfManagedGrainFactory.GetGrain(1);
            Task<Tuple<string, string>> promise1 = g1.TestRequestContext();
            Tuple<string, string> requstContext = promise1.Result;
            logger.Info("Request Context is: " + requstContext);
            Assert.IsNotNull(requstContext.Item2, "Item2=" + requstContext.Item2);
            Assert.IsNotNull(requstContext.Item1, "Item1=" + requstContext.Item1);
        }
    }
}

#pragma warning restore 618
