using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;

using Orleans.Management;
using Orleans.Runtime;


using UnitTestGrainInterfaces;

namespace UnitTests
{
    [TestClass]
    public class GrainPlacementTests : UnitTestBase
    {
        public GrainPlacementTests()
            : base(new Options
                    {
                        StartFreshOrleans = true,
                    }, new ClientOptions
                    {
                        ProxiedGateway = true,
                        Gateways = new List<IPEndPoint>(new IPEndPoint[] { new IPEndPoint(IPAddress.Loopback, 30000), new IPEndPoint(IPAddress.Loopback, 30001) }),
                        PreferedGatewayIndex = -1
                    })
        {
        }

        [TestCleanup]
        public void TestCleanup()
        {
            CheckForUnobservedPromises();
        }

        [ClassCleanup]
        public static void ClassCleanup()
        {
            ResetDefaultRuntimes();
        }

        private void TestSilosStarted(int expected)
        {
            IOrleansManagementGrain mgmtGrain = OrleansManagementGrainFactory.GetGrain(RuntimeInterfaceConstants.SystemManagementId);

            Dictionary<SiloAddress, SiloStatus> statuses = mgmtGrain.GetHosts(onlyActive: true).Result;
            foreach (var pair in statuses)
            {
                Console.WriteLine(String.Format("       ######## Silo {0}, status: {1}", pair.Key, pair.Value));
                Assert.AreEqual(
                    SiloStatus.Active,
                    pair.Value,
                    "Failed to confirm start of {0} silos ({1} confirmed).",
                    pair.Value,
                    SiloStatus.Active);
            }
            Assert.AreEqual(expected, statuses.Count);
        }

        private SiloHandle TestExplicitPlacementHappyPath(out IEnumerable<IPlacementTestGrain> grains, string name, IPEndPoint endpoint = null)
        {
            WaitForLivenessToStabilize();
            logger.Info("********************** Starting the test {0} ******************************", name);
            TestSilosStarted(2);

            SiloHandle siloh = null;
            if (null == endpoint)
            {
                siloh = StartAdditionalOrleans();
                TestSilosStarted(3);
                endpoint = siloh.Silo.SiloAddress.Endpoint;
            }
            else
            {
                TestSilosStarted(2);
            }

            grains =
                Enumerable.Range(0, 20).
                Select(
                    n =>
                        ExplicitPlacementTestGrainFactory.GetGrain((long)n, endpoint));
            var places = grains.Select(
                g =>
                    g.GetEndpoint().Result);
            Assert.IsTrue(places.All(endpoint.Equals));
            return siloh;
        }

        [TestMethod, TestCategory("Placement"), TestCategory("Nightly")]
        public void ExplicitlyPlacedGrainShouldExplicitlyPlaceActivation()
        {
            IEnumerable<IPlacementTestGrain> unused;
            TestExplicitPlacementHappyPath(out unused, "ExplicitlyPlacedGrainShouldExplicitlyPlaceActivation");
        }

        [ExpectedException(typeof(OrleansException))]
        [TestMethod, TestCategory("Placement"), TestCategory("Nightly")]
        public void ExplicitlyPlacedGrainOnNonExistentSiloShouldThrowAnException()
        {
            // todo:[mlr] the type of exception we're looking for is ambiguous, so this isn't a 100% reliable test. we would need
            // a more specific exception class (e.g. Orleans.MessageRejectedException) if we wanted to get closer to that. i haven't
            // implemented it that way because of the amount of refactoring that would need to occur, which is contrary to the goals
            // of my task.
            try
            {
                // [mlr] attempt to explicitly place on a silo that doesn't exist.
                IEnumerable<IPlacementTestGrain> unused;
                TestExplicitPlacementHappyPath(out unused, "ExplicitlyPlacedGrainOnNonExistentSiloShouldThrowAnException", new IPEndPoint(IPAddress.Loopback, 1));
            }
            catch (AggregateException e)
            {
                if (e.InnerExceptions.Count == 1 && e.GetBaseException() is OrleansException)
                    throw (OrleansException)e.GetBaseException();
                else
                    throw;
            }
        }

        [ExpectedException(typeof(OrleansException))]
        [TestMethod, TestCategory("Placement"), TestCategory("Nightly")]
        public void ExplicitlyPlacedGrainOnNewlyMissingSiloShouldThrowAnException()
        {
            // [mlr] attempt to explicitly place on a silo that doesn't exist.
            IEnumerable<IPlacementTestGrain> grains;
            var siloh = TestExplicitPlacementHappyPath(out grains, "ExplicitlyPlacedGrainOnNewlyMissingSiloShouldThrowAnException");
            KillRuntime(siloh);
            try
            {
                // [mlr] attempt to explicitly place a grain on a silo that no longer exists after the first method call.
                grains.First().GetEndpoint().Wait();
            }
            catch (AggregateException e)
            {
                // todo:[mlr] the type of exception we're looking for is ambiguous, so this isn't a 100% reliable test. we would need
                // a more specific exception class (e.g. Orleans.MessageRejectedException) if we wanted to get closer to that. i haven't
                // implemented it that way because of the amount of refactoring that would need to occur, which is contrary to the goals
                // of my task.
                if (e.InnerExceptions.Count == 1 && e.GetBaseException() is OrleansException)
                    throw (OrleansException)e.GetBaseException();
                else
                    throw;
            }
        }

        [TestMethod, TestCategory("Placement"), TestCategory("Nightly")]
        public void DefaultPlacementShouldBeRandom()
        {
            WaitForLivenessToStabilize();
            logger.Info("********************** Starting the test DefaultPlacementShouldBeRandom ******************************");
            TestSilosStarted(2);

            Assert.AreEqual(
                RandomPlacement.Singleton,
                PlacementStrategy.GetDefault(),
                "The default placement strategy is expected to be random.");
        }

        [TestMethod, TestCategory("Placement"), TestCategory("Nightly")]
        public void RandomlyPlacedGrainShouldPlaceActivationsRandomly()
        {
            WaitForLivenessToStabilize();
            logger.Info("********************** Starting the test RandomlyPlacedGrainShouldPlaceActivationsRandomly ******************************");
            TestSilosStarted(2);

            logger.Info("********************** TestSilosStarted passed OK. ******************************");

            var placement = RandomPlacement.Singleton;
            var grains =
                Enumerable.Range(0, 20).
                Select(
                    n =>
                        RandomPlacementTestGrainFactory.GetGrain((long)n));
            var places = grains.Select(g => g.GetRuntimeInstanceId().Result);
            var placesAsArray = places as string[] ?? places.ToArray();
            // todo:[mlr] it seems like we should check that we get close to a 50/50 split for placement.
            var groups = placesAsArray.GroupBy(s => s);
            Assert.IsTrue(groups.Count() > 1,
                "Grains should be on different silos, but they are on " + Utils.IEnumerableToString(placesAsArray.ToArray())); // will randomly fail one in a million times if RNG is good :-)
        }

        //[TestMethod, TestCategory("Placement"), TestCategory("Nightly")]
        //public void PreferLocalPlacedGrainShouldPlaceActivationsLocally_OneHop()
        //{
        //    WaitForLivenessToStabilize();
        //    logger.Info("********************** Starting the test PreferLocalPlacedGrainShouldPlaceActivationsLocally ******************************");
        //    TestSilosStarted(2);

        //    int numGrains = 20;
        //    var preferLocalGrain =
        //        Enumerable.Range(0, numGrains).
        //            Select(
        //                n =>
        //                    PreferLocalPlacementTestGrainFactory.GetGrain((long)n)).ToList();
        //    var preferLocalGrainPlaces = preferLocalGrain.Select(g => g.GetRuntimeInstanceId().Result).ToList();

        //    // check that every "prefer local grain" was placed on the same silo with its requesting random grain
        //    foreach (int key in Enumerable.Range(0, numGrains))
        //    {
        //        string preferLocal = preferLocalGrainPlaces.ElementAt(key);
        //        logger.Info(preferLocal);
        //    }
        //}

        [TestMethod, TestCategory("Placement"), TestCategory("Nightly")]
        public void PreferLocalPlacedGrainShouldPlaceActivationsLocally_TwoHops()
        {
            WaitForLivenessToStabilize();
            logger.Info("********************** Starting the test PreferLocalPlacedGrainShouldPlaceActivationsLocally ******************************");
            TestSilosStarted(2);

            int numGrains = 20;
            var randomGrains =
                Enumerable.Range(0, numGrains).
                    Select(
                        n =>
                            RandomPlacementTestGrainFactory.GetGrain((long)n)).ToList();
            var randomGrainPlaces = randomGrains.Select(g => g.GetRuntimeInstanceId().Result).ToList();

            var preferLocalGrainKeys =
                randomGrains.
                    Select(
                        (IRandomPlacementTestGrain g) =>
                            g.StartPreferLocalGrain(g.GetPrimaryKeyLong()).Result).ToList();
            var preferLocalGrainPlaces = preferLocalGrainKeys.Select(key => PreferLocalPlacementTestGrainFactory.GetGrain(key).GetRuntimeInstanceId().Result).ToList();

            // check that every "prefer local grain" was placed on the same silo with its requesting random grain
            foreach(int key in Enumerable.Range(0, numGrains))
            {
                string random = randomGrainPlaces.ElementAt(key);
                string preferLocal = preferLocalGrainPlaces.ElementAt(key);
                Assert.AreEqual(random, preferLocal,
                    "Grains should be on the same silos, but they are on " + random + " and " + preferLocal);
            }
        }

        private IEnumerable<IPEndPoint> SampleEndpoint(IPlacementTestGrain grain, int sampleSize)
        {
            for (var i = 0; i < sampleSize; ++i)
                yield return grain.GetEndpoint().Result;
        }

        private IEnumerable<ActivationId> CollectActivationIds(IPlacementTestGrain grain, int sampleSize)
        {
            for (var i = 0; i < sampleSize; ++i)
                yield return grain.GetActivationId().Result;
        }

        private int ActivationCount(IEnumerable<ActivationId> ids)
        {
            return ids.GroupBy(id => id).Count();
        }

        private int ActivationCount(IPlacementTestGrain grain, int sampleSize)
        {
            return ActivationCount(CollectActivationIds(grain, sampleSize));
        }

        [TestMethod, TestCategory("Placement"), TestCategory("Nightly")]
        public void LocallyPlacedGrainShouldCreateSpecifiedNumberOfMultipleActivations()
        {
            WaitForLivenessToStabilize();
            logger.Info("********************** Starting the test LocallyPlacedGrainShouldCreateSpecifiedNumberOfMultipleActivations ******************************");
            TestSilosStarted(2);

            // [mlr] note: this amount should agree with both the specified minimum and maximum in the LocalPlacement attribute
            // associated with ILocalPlacementTestGrain.
            const int expected = 10;
            var grain = LocalPlacementTestGrainFactory.GetGrain(0);
            int actual = ActivationCount(grain, expected * 5);
            Assert.AreEqual(expected, actual,
                "A grain instantiated with the local placement strategy should create multiple activations acording to the parameterization of the strategy.");
        }

        [TestMethod, TestCategory("Placement"), TestCategory("Nightly")]
        public void LocallyPlacedGrainShouldCreateActivationsOnLocalSilo()
        {
            WaitForLivenessToStabilize();
            logger.Info("********************** Starting the test LocallyPlacedGrainShouldCreateActivationsOnLocalSilo ******************************");
            TestSilosStarted(2);

            const int sampleSize = 5;
            var placement = new LocalPlacement(sampleSize);
            var proxy = RandomPlacementTestGrainFactory.GetGrain(-1);
            proxy.StartLocalGrains(new List<long> { 0 }).Wait();
            var expected = proxy.GetEndpoint().Result;
            // [mlr] locally placed grains are multi-activation and stateless. this means that we have to sample the value of
            // the result, rather than simply ask for it once in order to get a consensus of the result.
            var actual = proxy.SampleLocalGrainEndpoint(0, sampleSize).Result;
            Assert.IsTrue(actual.All(expected.Equals),
                "A grain instantiated with the local placement strategy should create activations on the local silo.");
        }

        private void LoadAwareGrainPlacementTest(
            Func<IPlacementTestGrain, Task> taint,
            Func<IPlacementTestGrain, Task> restore,
            string name,
            string assertMsg)
        {
            WaitForLivenessToStabilize();
            logger.Info("********************** Starting the test {0} ******************************", name);
            var taintedSilo = StartAdditionalOrleans().Silo;
            TestSilosStarted(3);

            const long sampleSize = 10;

            var taintedGrain =
                ExplicitPlacementTestGrainFactory.GetGrain(-1, taintedSilo.SiloAddress.Endpoint);
            var testGrains =
                Enumerable.Range(0, (int)sampleSize).
                Select(
                    n =>
                        LoadAwarePlacementTestGrainFactory.GetGrain((long)n));

            // [mlr] make the grain's silo undesirable for new grains.
            taint(taintedGrain).Wait();
            List<IPEndPoint> actual = null;
            try
            {
                actual =
                    testGrains.Select(
                        g =>
                            g.GetEndpoint().Result).ToList();
            }
            finally
            {
                // [mlr] i don't know if this necessary but to be safe, i'll restore the silo's desirability.
                logger.Info("********************** Finalizing the test {0} ******************************", name);
                restore(taintedGrain).Wait();
            }

            var unexpected = taintedSilo.SiloAddress.Endpoint;
            Assert.IsTrue(
                actual.All(
                    i =>
                        !i.Equals(unexpected)),
                assertMsg);
        }

        [TestMethod, TestCategory("Placement"), TestCategory("Nightly")]
        public void LoadAwareGrainShouldNotAttemptToCreateActivationsOnOverloadedSilo()
        {
            WaitForLivenessToStabilize();
            LoadAwareGrainPlacementTest(
                g =>
                    g.LatchOverloaded(),
                g =>
                    g.UnlatchOverloaded(),
                "LoadAwareGrainShouldNotAttemptToCreateActivationsOnOverloadedSilo",
                "A grain instantiated with the load-aware placement strategy should not attempt to create activations on an overloaded silo.");
        }

        [TestMethod, TestCategory("Placement"), TestCategory("Nightly")]
        public void LoadAwareGrainShouldNotAttemptToCreateActivationsOnBusySilos()
        {
            WaitForLivenessToStabilize();
            // [mlr] a CPU usage of 110% will disqualify a silo from getting new grains.
            const float undesirability = (float)110.0;
            LoadAwareGrainPlacementTest(
                g =>
                    g.LatchCpuUsage(undesirability),
                g =>
                    g.UnlatchCpuUsage(),
                "LoadAwareGrainShouldNotAttemptToCreateActivationsOnBusySilos",
                "A grain instantiated with the load-aware placement strategy should not attempt to create activations on a busy silo.");
        }
    }
}