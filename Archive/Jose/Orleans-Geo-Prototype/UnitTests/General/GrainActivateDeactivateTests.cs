using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;

using UnitTestGrainInterfaces;

namespace UnitTests.General
{
    [TestClass]
    public class GrainActivateDeactivateTests : UnitTestBase
    {
        private IActivateDeactivateWatcherGrain watcher;
        private static readonly Random random = new Random();

        public GrainActivateDeactivateTests()
            : base(new Options { StartPrimary = true, StartSecondary = false }) // Only need single silo
        {
        }

        [ClassInitialize]
        public static void ClassInitialize(TestContext testContext)
        {
            ResetDefaultRuntimes();
        }
        [ClassCleanup]
        public static void ClassCleanup()
        {
            ResetDefaultRuntimes();
        }
        [TestInitialize]
        public void TestInitialize()
        {
            watcher = ActivateDeactivateWatcherGrainFactory.GetGrain(0);
            watcher.Clear().Wait();
        }
        [TestCleanup]
        public void TestCleanup()
        {
            if (watcher != null)
            {
                watcher.Clear().Wait();
                watcher = null;
            }
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("ActivateDeactivate")]
        public async Task WatcherGrain_GetGrain()
        {
            IActivateDeactivateWatcherGrain grain = ActivateDeactivateWatcherGrainFactory.GetGrain(1);
            await grain.Clear();
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("ActivateDeactivate")]
        public async Task Activate_Simple()
        {
            int id = random.Next();
            ISimpleActivateDeactivateTestGrain grain = SimpleActivateDeactivateTestGrainFactory.GetGrain(id);

            ActivationId activation = await grain.DoSomething();

            await CheckNumActivateDeactivateCalls(1, 0, activation, "After activation");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("ActivateDeactivate")]
        public async Task Deactivate_Simple()
        {
            int id = random.Next();
            ISimpleActivateDeactivateTestGrain grain = SimpleActivateDeactivateTestGrainFactory.GetGrain(id);

            // Activate
            ActivationId activation = await grain.DoSomething();

            // Deactivate
            await grain.DoDeactivate();
            Thread.Sleep(TimeSpan.FromSeconds(2)); // Allow some time for deactivate to happen

            await CheckNumActivateDeactivateCalls(1, 1, activation, "After deactivation");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("ActivateDeactivate")]
        public async Task Reactivate_Simple()
        {
            int id = random.Next();
            ISimpleActivateDeactivateTestGrain grain = SimpleActivateDeactivateTestGrainFactory.GetGrain(id);

            // Activate
            ActivationId activation = await grain.DoSomething();
            // Deactivate
            await grain.DoDeactivate();
            Thread.Sleep(TimeSpan.FromSeconds(2)); // Allow some time for deactivate to happen

            await CheckNumActivateDeactivateCalls(1, 1, activation, "After deactivation");

            // Reactivate
            ActivationId activation2 = await grain.DoSomething();

            Assert.AreNotEqual(activation, activation2, "New activation created after re-activate");
            await CheckNumActivateDeactivateCalls(2, 1, new[] { activation, activation2 }, "After reactivation");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("ActivateDeactivate")]
        public async Task Activate_TailCall()
        {
            int id = random.Next();
            ITailCallActivateDeactivateTestGrain grain = TailCallActivateDeactivateTestGrainFactory.GetGrain(id);

            ActivationId activation = await grain.DoSomething();

            await CheckNumActivateDeactivateCalls(1, 0, activation, "After activation");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("ActivateDeactivate")]
        public async Task Deactivate_TailCall()
        {
            int id = random.Next();
            ITailCallActivateDeactivateTestGrain grain = TailCallActivateDeactivateTestGrainFactory.GetGrain(id);

            // Activate
            ActivationId activation = await grain.DoSomething();

            // Deactivate
            await grain.DoDeactivate();
            Thread.Sleep(TimeSpan.FromSeconds(2)); // Allow some time for deactivate to happen

            await CheckNumActivateDeactivateCalls(1, 1, activation, "After deactivation");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("ActivateDeactivate")]
        public async Task Reactivate_TailCall()
        {
            int id = random.Next();
            ITailCallActivateDeactivateTestGrain grain = TailCallActivateDeactivateTestGrainFactory.GetGrain(id);

            // Activate
            ActivationId activation = await grain.DoSomething();
            // Deactivate
            await grain.DoDeactivate();
            Thread.Sleep(TimeSpan.FromSeconds(2)); // Allow some time for deactivate to happen

            await CheckNumActivateDeactivateCalls(1, 1, activation, "After deactivation");

            // Reactivate
            ActivationId activation2 = await grain.DoSomething();

            Assert.AreNotEqual(activation, activation2, "New activation created after re-activate");
            await CheckNumActivateDeactivateCalls(2, 1, new[] { activation, activation2 }, "After reactivation");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("ActivateDeactivate"), TestCategory("Reentrancy")]
        public async Task LongRunning_Deactivate()
        {
            int id = random.Next();
            ILongRunningActivateDeactivateTestGrain grain = LongRunningActivateDeactivateTestGrainFactory.GetGrain(id);

            // Activate
            ActivationId activation = grain.DoSomething().Result;

            await CheckNumActivateDeactivateCalls(1, 0, activation, "Before deactivation");
            
            // Deactivate
            grain.DoDeactivate().Wait();
            //await grain.DoDeactivate();
            Thread.Sleep(TimeSpan.FromSeconds(2)); // Allow some time for deactivate to happen

            await CheckNumActivateDeactivateCalls(1, 1, activation, "After deactivation");

            // Reactivate
            ActivationId activation2 = grain.DoSomething().Result;

            Assert.AreNotEqual(activation, activation2, "New activation created after re-activate");
            await CheckNumActivateDeactivateCalls(2, 1, new[] { activation, activation2}, "After reactivation");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("ActivateDeactivate")]
        public async Task BadActivate_Await()
        {
            try
            {
                int id = random.Next();
                IBadActivateDeactivateTestGrain grain = BadActivateDeactivateTestGrainFactory.GetGrain(id);

                await grain.ThrowSomething();

                Assert.Fail("Expected ThrowSomething call to fail as unable to Activate grain");
            }
            catch (Exception exc)
            {
                Console.WriteLine("Received exception: " + exc);
                Exception e = exc.GetBaseException();
                Console.WriteLine("Nested exception type: " + e.GetType().FullName);
                Console.WriteLine("Nested exception message: " + e.Message);
                Assert.IsInstanceOfType(e, typeof(ApplicationException), "Did not get expected exception type returned: " + e);
                Assert.IsNotInstanceOfType(e, typeof(InvalidOperationException), "Did not get expected exception type returned: " + e);
                Assert.IsTrue(e.Message.Contains("ActivateAsync"), "Did not get expected exception message returned: " + e.Message);
            }
        }


        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("ActivateDeactivate")]
        public void BadActivate_Wait()
        {
            try
            {
                int id = random.Next();
                IBadActivateDeactivateTestGrain grain = BadActivateDeactivateTestGrainFactory.GetGrain(id);

                grain.ThrowSomething().Wait();

                Assert.Fail("Expected ThrowSomething call to fail as unable to Activate grain");
            }
            catch (Exception exc)
            {
                Console.WriteLine("Received exception: " + exc);
                Exception e = exc.GetBaseException();
                Console.WriteLine("Nested exception type: " + e.GetType().FullName);
                Console.WriteLine("Nested exception message: " + e.Message);
                Assert.IsInstanceOfType(e, typeof(ApplicationException), "Did not get expected exception type returned: " + e);
                Assert.IsNotInstanceOfType(e, typeof(InvalidOperationException), "Did not get expected exception type returned: " + e);
                Assert.IsTrue(e.Message.Contains("ActivateAsync"), "Did not get expected exception message returned: " + e.Message);
            }
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("ActivateDeactivate")]
        public void BadActivate_GetValue()
        {
            try
            {
                int id = random.Next();
                IBadActivateDeactivateTestGrain grain = BadActivateDeactivateTestGrainFactory.GetGrain(id);

                long key = grain.GetKey().Result;

                Assert.Fail("Expected ThrowSomething call to fail as unable to Activate grain, but returned " + key);
            }
            catch (Exception exc)
            {
                Console.WriteLine("Received exception: " + exc);
                Exception e = exc.GetBaseException();
                Console.WriteLine("Nested exception type: " + e.GetType().FullName);
                Console.WriteLine("Nested exception message: " + e.Message);
                Assert.IsInstanceOfType(e, typeof(ApplicationException), "Did not get expected exception type returned: " + e);
                Assert.IsNotInstanceOfType(e, typeof(InvalidOperationException), "Did not get expected exception type returned: " + e);
                Assert.IsTrue(e.Message.Contains("ActivateAsync"), "Did not get expected exception message returned: " + e.Message);
            }
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("ActivateDeactivate")]
        public void Constructor_Bad_Wait()
        {
            try
            {
                int id = random.Next();
                IBadConstructorTestGrain grain = BadConstructorTestGrainFactory.GetGrain(id);

                grain.DoSomething().Wait();

                Assert.Fail("Expected ThrowSomething call to fail as unable to Activate grain");
            }
            catch (TimeoutException te)
            {
                Console.WriteLine("Received timeout: " + te);
                throw; // Fail test
            }
            catch (Exception exc)
            {
                Console.WriteLine("Received exception: " + exc);
                Exception e = exc.GetBaseException();
                Console.WriteLine("Nested exception type: " + e.GetType().FullName);
                Console.WriteLine("Nested exception message: " + e.Message);
                Assert.IsInstanceOfType(e, typeof (ApplicationException),
                                        "Did not get expected exception type returned: " + e);
                Assert.IsNotInstanceOfType(e, typeof (InvalidOperationException),
                                           "Did not get expected exception type returned: " + e);
                Assert.IsTrue(e.Message.Contains("Constructor"),
                              "Did not get expected exception message returned: " + e.Message);
            }
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("ActivateDeactivate")]
        public async Task Constructor_Bad_Await()
        {
            try
            {
                int id = random.Next();
                IBadConstructorTestGrain grain = BadConstructorTestGrainFactory.GetGrain(id);

                await grain.DoSomething();

                Assert.Fail("Expected ThrowSomething call to fail as unable to Activate grain");
            }
            catch (TimeoutException te)
            {
                Console.WriteLine("Received timeout: " + te);
                throw; // Fail test
            }
            catch (Exception exc)
            {
                Console.WriteLine("Received exception: " + exc);
                Exception e = exc.GetBaseException();
                Console.WriteLine("Nested exception type: " + e.GetType().FullName);
                Console.WriteLine("Nested exception message: " + e.Message);
                Assert.IsInstanceOfType(e, typeof(ApplicationException),
                                        "Did not get expected exception type returned: " + e);
                Assert.IsNotInstanceOfType(e, typeof(InvalidOperationException),
                                           "Did not get expected exception type returned: " + e);
                Assert.IsTrue(e.Message.Contains("Constructor"),
                              "Did not get expected exception message returned: " + e.Message);
            }
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("ActivateDeactivate")]
        public async Task Constructor_CreateGrainReference()
        {
            int id = random.Next();
            ICreateGrainReferenceTestGrain grain = CreateGrainReferenceTestGrainFactory.GetGrain(id);

            ActivationId activation = await grain.DoSomething();
            Assert.IsNotNull(activation);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("General")]
        public async Task TaskAction_Deactivate()
        {
            int id = random.Next();
            ITaskActionActivateDeactivateTestGrain grain = TaskActionActivateDeactivateTestGrainFactory.GetGrain(id);

            // Activate
            ActivationId activation = await grain.DoSomething();

            // Deactivate
            await grain.DoDeactivate();
            Thread.Sleep(TimeSpan.FromSeconds(2)); // Allow some time for deactivate to happen

            await CheckNumActivateDeactivateCalls(1, 1, activation);
        }

        private async Task CheckNumActivateDeactivateCalls(
            int expectedActivateCalls,
            int expectedDeactivateCalls,
            ActivationId forActivation,
            string when = null)
        {
            await CheckNumActivateDeactivateCalls(
                expectedActivateCalls,
                expectedDeactivateCalls,
                new ActivationId[] { forActivation },
                when )
            ;
        }

        private async Task CheckNumActivateDeactivateCalls(
            int expectedActivateCalls, 
            int expectedDeactivateCalls,
            ActivationId[] forActivations,
            string when = null)
        {
            ActivationId[] activateCalls = await watcher.ActivateCalls;
            Assert.AreEqual(expectedActivateCalls, activateCalls.Length, "Number of Activate calls {0}", when);

            ActivationId[] deactivateCalls = await watcher.DeactivateCalls;
            Assert.AreEqual(expectedDeactivateCalls, deactivateCalls.Length, "Number of Deactivate calls {0}", when);

            for (int i = 0; i < expectedActivateCalls; i++)
            {
                Assert.AreEqual(forActivations[i], activateCalls[i], "Activate call #{0} was by expected activation {1}", (i+1), when);
            }

            for (int i = 0; i < expectedDeactivateCalls; i++)
            {
                Assert.AreEqual(forActivations[i], deactivateCalls[i], "Deactivate call #{0} was by expected activation {1}", (i + 1), when);
            }
        }
    }
}
