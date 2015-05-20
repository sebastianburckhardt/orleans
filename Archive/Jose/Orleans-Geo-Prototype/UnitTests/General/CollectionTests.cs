using System;
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
    public class CollectionTests
    {
        private static readonly Options TestOptions = new Options
        {
            StartFreshOrleans = true,
            StartSecondary = false,
            DefaultCollectionAgeLimit = TimeSpan.Zero,
            CollectionTotalMemoryLimit = 0,
        };

        [TestCleanup]
        public void Reset()
        {
            try
            {
                UnitTestBase.ResetDefaultRuntimes();
            }
            catch (Exception ex)
            {
                Console.WriteLine("MyClassCleanup failed with {0}: {1}", ex, ex.StackTrace);
            }
        }
        
        //[TestMethod]
        public void CollectionTestAge()
        {
            var options = TestOptions.Copy();
            options.DefaultCollectionAgeLimit = TimeSpan.FromSeconds(2);
            CollectionTestRun(options);
        }

        //[TestMethod]
        public void CollectionTestCount()
        {
            var options = TestOptions.Copy();
            CollectionTestRun(options);
        }

        [TestMethod, TestCategory("Revisit"), TestCategory("GC")]
        public void ActivateGrainsTest()
        {
            //UnitTestBase.Initialize(TestOptions.Copy());
            ////Thread.Sleep(5000);

            //// activate all the grains
            //var grains = Enumerable.Range(0, 20).Select(i => CollectionTestGrainFactory.GetGrain(i)).ToList();
            //Domain.Current.ActivateGrains(grains).Wait();

            //// wait and then make sure they have all been activated
            //Thread.Sleep(1100);
            //var ages = grains.Select(g => g.GetAge()).ToList()
            //    .Select(v => (int) v.Result.TotalMilliseconds).ToList();

            //Assert.IsTrue(ages.All(a => a > 1000), "All grains should have been activated" + ages.ToStrings());
        }

        [TestMethod, TestCategory("Revisit"), TestCategory("GC")]
        public void DeactivateOnIdleTest()
        {
            //UnitTestBase.Initialize(TestOptions.Copy());
            ////Thread.Sleep(5000);

            //// activate all the grains
            //var grains = Enumerable.Range(0, 30).Select(i => CollectionTestGrainFactory.GetGrain(i)).ToList();
            //AsyncCompletion.JoinAll(grains.Select(g => g.GetAge())).Wait();

            //// tell first 10 to go away
            //AsyncCompletion.JoinAll(grains.Take(10).Select(g => g.DeactivateSelf())).Wait();
            //Thread.Sleep(1100);

            //var ages = grains.Select(g => g.GetAge()).ToList().Select(a => (int) a.Result.TotalMilliseconds).ToList();
            //Assert.IsTrue(ages.Take(10).All(x => x < 200), "First 10 grains should have been deactivated");
            //Assert.IsTrue(ages.Skip(10).All(x => x > 1000), "Rest of grains should not have been deactivated");

            //Domain.Current.DeactivateGrainsOnIdle(grains.Skip(10).Take(10).Cast<IAddressable>().ToList()).Wait();
            //Thread.Sleep(1100);
            //ages = grains.Select(g => g.GetAge()).ToList().Select(a => (int)a.Result.TotalMilliseconds).ToList();
            //Assert.IsTrue(ages.Skip(10).Take(10).All(x => x < 200), "Next 10 grains should have been deactivated");
            //Assert.IsTrue(ages.Skip(20).All(x => x > 1000), "Rest of grains should not have been deactivated");
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("GC")]
        public void DeactivateOnIdleTestInside()
        {
            UnitTestBase.Initialize(TestOptions.Copy());
            //Thread.Sleep(5000);

            var a = CollectionTestGrainFactory.GetGrain(1);
            var b = CollectionTestGrainFactory.GetGrain(2);
            a.SetOther(b).Wait();
            a.GetOtherAge().Wait(); // prime a's routing cache
            b.DeactivateSelf().Wait();
            Thread.Sleep(5000);
            try
            {
                var age = AsyncValue.FromTask(a.GetOtherAge()).GetValue(TimeSpan.FromMilliseconds(2000));
                Assert.IsTrue(age.TotalMilliseconds < 2000, "Should be newly activated grain");
            }
            catch (TimeoutException)
            {
                Assert.Fail("Should not time out when reactivating grain");
            }
        }


        [TestMethod, TestCategory("Nightly"), TestCategory("General"), TestCategory("GC")]
        public void DeactivateOnIdle_NonExistentActivation_1()
        {
            DeactivateOnIdle_NonExistentActivation_Runner(0);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("General"), TestCategory("GC")]
        public void DeactivateOnIdle_NonExistentActivation_2()
        {
            DeactivateOnIdle_NonExistentActivation_Runner(1);
        }

        private void DeactivateOnIdle_NonExistentActivation_Runner(int forwardCount)
        {
            // Fix SiloGenerationNumber and later play with grain id to map grain id to the right directory partition.
            Options options = new Options { MaxForwardCount = forwardCount, SiloGenerationNumber = 13 };
            UnitTestBase unitTest = new UnitTestBase(options);

            ICollectionTestGrain grain = PickGrain(unitTest);
            Assert.AreNotEqual(null, grain, "Could not create a grain that matched the desired requirements");

            TimeSpan age = grain.GetAge().Result;
            unitTest.logger.Info(age.ToString());

            grain.DeactivateSelf().Wait();
            Thread.Sleep(3000);
            bool didThrow = false;
            bool didThrowCorrectly = false;
            Exception thrownException = null;
            try
            {
                age = grain.GetAge().Result;
                unitTest.logger.Info(age.ToString());
            }
            catch (Exception exc)
            {
                didThrow = true;
                thrownException = exc;
                Exception baseException = exc.GetBaseException();
                didThrowCorrectly = baseException.GetType().Equals(typeof(OrleansException)) && baseException.Message.StartsWith("Non-existent activation");
            }

            if (forwardCount == 0)
            {
                Assert.IsTrue(didThrow, "The call did not throw exception as expected.");
                Assert.IsTrue(didThrowCorrectly, "The call did not throw Non-existent activation Exception as expected. Instead it has thrown: " + thrownException);
                unitTest.logger.Info(
                    "\nThe 1st call after DeactivateSelf has thrown Non-existent activation exception as expected, since forwardCount is {0}.\n",
                    forwardCount);
            }
            else
            {
                Assert.IsFalse(didThrow, "The call has throw an exception, which was not expected. The exception is: " + (thrownException == null ? "" : thrownException.ToString()));
                unitTest.logger.Info("\nThe 1st call after DeactivateSelf has NOT thrown any exception as expected, since forwardCount is {0}.\n", forwardCount);
            }

            if (forwardCount == 0)
            {
                didThrow = false;
                thrownException = null;
                // try sending agan now and see it was fixed.
                try
                {
                    age = grain.GetAge().Result;
                    unitTest.logger.Info(age.ToString());
                }
                catch (Exception exc)
                {
                    didThrow = true;
                    thrownException = exc;
                }
                Assert.IsFalse(didThrow, "The 2nd call has throw an exception, which was not expected. The exception is: " + (thrownException == null ? "" : thrownException.ToString()));
                unitTest.logger.Info("\nThe 2nd call after DeactivateSelf has NOT thrown any exception as expected, despite the fact that forwardCount is {0}, since we send CacheMgmtHeader.\n", forwardCount);
            }
        }

        private ICollectionTestGrain PickGrain(UnitTestBase unitTest)
        {
            ICollectionTestGrain grain = null;
            for (int i = 0; i < 100; i++)
            {
                // Create grain such that:
                    // Its directory owner is not the GW silo. This way GW will use its directory cache.
                    // Its activation is located on the non GW silo as well.
                grain = CollectionTestGrainFactory.GetGrain(i);
                GrainId grainId = grain.GetGrainId().Result;
                SiloAddress primaryForGrain = UnitTestBase.Primary.Silo.LocalGrainDirectory.GetPrimaryForGrain(grainId);
                if (primaryForGrain.Equals(UnitTestBase.Primary.Silo.SiloAddress))
                {
                    continue;
                }
                string siloHostingActivation = grain.GetRuntimeInstanceId().Result;
                if (UnitTestBase.Primary.Silo.SiloAddress.ToLongString().Equals(siloHostingActivation))
                {
                    continue;
                }
                unitTest.logger.Info("\nCreated grain with key {0} whose primary directory owner is silo {1} and which was activated on silo {2}\n", i, primaryForGrain.ToLongString(), siloHostingActivation);
                return grain;
            }
            return null;
        }

        //[TestMethod]
        // todo: the timing on this test is unreliable
        //public void CollectionTestForceCount()
        //{
        //    var options = TestOptions.Copy();
        //    CollectionTestRun(options, () =>
        //    {
        //        OrleansManagementGrainFactory.GetGrain(RuntimeConstants.SystemManagementId)
        //            .ForceActivationCollection(null, new ActivationLimits { ActivationLimit = 15 })
        //            .Wait();
        //        Thread.Sleep(500);
        //    });
        //}

        private static void CollectionTestRun(Options options, Action after = null)
        {
            UnitTestBase.Initialize(options);
            //Thread.Sleep(5000);

            // create grains every 100 ms
            var grains = new List<ICollectionTestGrain>();
            var done = new List<Task>();
            for (var i = 0; i < 40; i++)
            {
                // touch grain and add to list
                var grain = CollectionTestGrainFactory.GetGrain(i);
                done.Add(grain.GetAge());
                grains.Add(grain);

                // keep retouching oldest
                if (i >= 10)
                    done.Add(grains[i % 10].GetAge());

                Thread.Sleep(100);
            }
            if (after != null)
                after();
            Task.WhenAll(done).Wait();
            const int t = 100;

            var ages = grains.Select(g => g.GetAge()).ToList()
                .Select(v => (int) v.Result.TotalMilliseconds).ToList();

            Assert.IsTrue(ages.Skip(11).Take(8).All(a => a < t),
                "Almost-oldest grains should have been collected & reactivated: " + ages.ToStrings());
            Assert.IsTrue(ages.Skip(32).All(a => a >= t),
                "Newest grains should not have been collected: " + ages.ToStrings());
            Assert.IsTrue(ages.Skip(1).Take(8).All(a => a >= t),
                "Oldest but busy grains should not have been collected: " + ages.ToStrings());
        }
    }
}
