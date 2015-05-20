using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using Orleans.Runtime;
using Orleans.Runtime.MembershipService;


namespace UnitTests.General
{
    [TestClass]
    public class ConsistentRingProviderTests : UnitTestBase
    {
        public ConsistentRingProviderTests() : base(true) { }

        private const int numAdditionalSilos = 3;
        private readonly Random random = new Random();
        private readonly TimeSpan failureTimeout = TimeSpan.FromSeconds(17); // safe value: 30
        private readonly TimeSpan endWait = TimeSpan.FromMinutes(5);

        private Logger log = Logger.GetLogger("ConsistentRingTest", Logger.LoggerType.Application);

        enum Fail { First, Random, Last }

        [TestCleanup]
        public void Cleanup()
        {
            ResetAllAdditionalRuntimes();
            ResetDefaultRuntimes();
            CheckForUnobservedPromises();
        }

        #region Tests

        [TestMethod, TestCategory("Nightly"), TestCategory("Ring")]
        public void Ring_Basic()
        {
            StartAdditionalOrleansRuntimes(numAdditionalSilos);
            WaitForLivenessToStabilize();
            VerificationScenario();
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Ring")]
        public void Ring_1F_Random()
        {
            FailureTest(Fail.Random, 1);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Ring")]
        public void Ring_1F_Beginning()
        {
            FailureTest(Fail.First, 1);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Ring")]
        public void Ring_1F_End()
        {
            FailureTest(Fail.Last, 1);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Ring")]
        public void Ring_2F_Random()
        {
            FailureTest(Fail.Random, 2);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Ring")]
        public void Ring_2F_Beginning()
        {
            FailureTest(Fail.First, 2);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Ring")]
        public void Ring_2F_End()
        {
            FailureTest(Fail.Last, 2);
        }

        private void FailureTest(Fail failCode, int numOfFailures)
        {
            StartAdditionalOrleansRuntimes(numAdditionalSilos);
            WaitForLivenessToStabilize();

            List<SiloHandle> failures = getSilosToFail(failCode, numOfFailures);
            foreach (SiloHandle fail in failures) // verify before failure
            {
                VerificationScenario(fail.Silo.SiloAddress.GetConsistentHashCode());
            }

            log.Info("FailureTest {0}, Code {1}, Stopping silos: {2}", numOfFailures, failCode, Utils.IEnumerableToString(failures, handle => handle.Silo.SiloAddress.ToString()));
            List<int> keysToTest = new List<int>();
            foreach (SiloHandle fail in failures) // verify before failure
            {
                keysToTest.Add(fail.Silo.SiloAddress.GetConsistentHashCode());
                StopRuntime(fail);
            }
            WaitForLivenessToStabilize();
            Thread.Sleep(failureTimeout);
            foreach (int key in keysToTest) // verify after failure
            {
                VerificationScenario(key);
            }
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Ring")]
        public void Ring_1J()
        {
            JoinTest(1);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Ring")]
        public void Ring_2J()
        {
            JoinTest(2);
        }

        private void JoinTest(int numOfJoins)
        {
            log.Info("JoinTest {0}", numOfJoins);
            StartAdditionalOrleansRuntimes(numAdditionalSilos - numOfJoins);
            WaitForLivenessToStabilize();

            List<SiloHandle> silos = StartAdditionalOrleansRuntimes(numOfJoins);
            WaitForLivenessToStabilize();
            foreach (SiloHandle sh in silos)
            {
                VerificationScenario(sh.Silo.SiloAddress.GetConsistentHashCode(), sh.Silo.SiloAddress); // verify newly joined silo's key
            }
            Thread.Sleep(TimeSpan.FromSeconds(15));
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Ring")]
        public void Ring_1F1J()
        {
            StartAdditionalOrleansRuntimes(numAdditionalSilos);
            WaitForLivenessToStabilize();
            List<SiloHandle> failures = getSilosToFail(Fail.Random, 1);
            int keyToCheck = failures[0].Silo.SiloAddress.GetConsistentHashCode();
            List<SiloHandle> joins = null;

            // kill a silo and join a new one in parallel
            log.Info("Killing silo {0} and joining a silo", failures[0].Silo.SiloAddress);
            AsyncCompletion[] tasks = new AsyncCompletion[2]
            {
                AsyncCompletion.StartNew(() => StopRuntime(failures[0])),
                AsyncCompletion.StartNew(() => joins = StartAdditionalOrleansRuntimes(1))
            };
            AsyncCompletion.WaitAll(tasks, endWait);

            WaitForLivenessToStabilize();
            Thread.Sleep(failureTimeout);

            VerificationScenario(keyToCheck); // verify failed silo's key
            VerificationScenario(joins[0].Silo.SiloAddress.GetConsistentHashCode(), joins[0].Silo.SiloAddress); // verify newly joined silo's key
        }

        // failing the secondary in this scenario exposed the bug in DomainGrain ... so, we keep it as a separate test than Ring_1F1J
        [TestMethod, TestCategory("Nightly"), TestCategory("Ring")]
        public void Ring_1Fsec1J()
        {
            StartAdditionalOrleansRuntimes(numAdditionalSilos);
            WaitForLivenessToStabilize();
            //List<SiloHandle> failures = getSilosToFail(Fail.Random, 1);
            SiloHandle fail = Secondary;
            int keyToCheck = fail.Silo.SiloAddress.GetConsistentHashCode();
            List<SiloHandle> joins = null;

            // kill a silo and join a new one in parallel
            log.Info("Killing secondary silo {0} and joining a silo", fail.Silo.SiloAddress);
            AsyncCompletion[] tasks = new AsyncCompletion[2]
            {
                AsyncCompletion.StartNew(() => StopRuntime(fail)),
                AsyncCompletion.StartNew(() => joins = StartAdditionalOrleansRuntimes(1))
            };
            AsyncCompletion.WaitAll(tasks, endWait);

            WaitForLivenessToStabilize();
            Thread.Sleep(failureTimeout);

            VerificationScenario(keyToCheck); // verify failed silo's key
            VerificationScenario(joins[0].Silo.SiloAddress.GetConsistentHashCode(), joins[0].Silo.SiloAddress); // verify newly joined silo's key
        }

        #endregion

        #region Utility methods

        private void VerificationScenario(int testKey = 0, SiloAddress expectedForTestKey = null)
        {
            // setup
            List<SiloAddress> silos = new List<SiloAddress>();

            foreach (var siloHandle in GetActiveSilos())
            {
                long hash = siloHandle.Silo.SiloAddress.GetConsistentHashCode();
                int index = silos.FindLastIndex(siloAddr => siloAddr.GetConsistentHashCode() < hash) + 1;
                silos.Insert(index, siloHandle.Silo.SiloAddress);
            }

            // verify parameter key
            VerifyKey(testKey, silos, expectedForTestKey);
            // verify some other keys as well, apart from the parameter key            
            // some random keys
            for (int i = 0; i < 3; i++)
            {
                VerifyKey(random.Next(), silos);
            }
            // lowest key
            int lowest = silos.First().GetConsistentHashCode() - 1;
            VerifyKey(lowest, silos);
            // highest key
            int highest = silos.Last().GetConsistentHashCode() + 1;
            VerifyKey(lowest, silos);
        }

        private void VerifyKey(int key, List<SiloAddress> silos, SiloAddress expected = null)
        {
            SiloAddress truth = expected;
            if (truth == null) // if the truth isn't passed, we compute it here
            {
                truth = silos.Find(siloAddr => (key <= siloAddr.GetConsistentHashCode()));
                if (truth == null)
                {
                    truth = silos.First();
                }
            }

            // lookup for 'key' should return 'truth' on all silos
            foreach (var siloHandle in GetActiveSilos()) // do this for each silo
            {
                SiloAddress s = siloHandle.Silo.TestHookup.ConsistentRingProvider.GetPrimary(key);
                Assert.AreEqual(truth, s, string.Format("Lookup wrong for key: {0} on silo: {1}", key, siloHandle.Silo.SiloAddress));
            }
        }

        private List<SiloHandle> getSilosToFail(Fail fail, int numOfFailures)
        {
            List<SiloHandle> failures = new List<SiloHandle>();
            int count = 0, index = 0;

            SortedList<int, SiloHandle> ids = new SortedList<int, SiloHandle>();
            foreach (var siloHandle in GetActiveSilos())
            {
                ids.Add(siloHandle.Silo.SiloAddress.GetConsistentHashCode(), siloHandle);
            }
            // we should not fail the primary!
            // we can't guarantee semantics of 'Fail' if it evalutes to the primary's address
            switch (fail)
            {
                case Fail.First:
                    index = 0;
                    while (count++ < numOfFailures)
                    {
                        while (Primary.Equals(ids.Values[index]) || failures.Contains(ids.Values[index]))
                        {
                            index++;
                        }
                        failures.Add(ids.Values[index]);
                    }
                    break;
                case Fail.Last:
                    index = ids.Count - 1;
                    while (count++ < numOfFailures)
                    {
                        while (Primary.Equals(ids.Values[index]) || failures.Contains(ids.Values[index]))
                        {
                            index--;
                        }
                        failures.Add(ids.Values[index]);
                    }
                    break;
                case Fail.Random:
                default:
                    while (count++ < numOfFailures)
                    {
                        SiloHandle r = ids.Values[random.Next(ids.Count)];
                        while (Primary.Equals(r) || failures.Contains(r))
                        {
                            r = ids.Values[random.Next(ids.Count)];
                        }
                        failures.Add(r);
                    }
                    break;
            }
            return failures;
        }

        // for debugging only
        private void printSilos(string msg)
        {
            SortedList<int, SiloAddress> ids = new SortedList<int, SiloAddress>(numAdditionalSilos + 2);
            foreach (var siloHandle in GetActiveSilos())
            {
                ids.Add(siloHandle.Silo.SiloAddress.GetConsistentHashCode(), siloHandle.Silo.SiloAddress);
            }
            log.Info("{0} list of silos: ", msg);
            foreach (var id in ids.Keys.ToList())
            {
                log.Info("{0} -> {1}", ids[id], id);
            }
        }
        #endregion

    }

    [TestClass]
    public class RingTests : UnitTestBase
    {
        public RingTests()
            : base(new Options
                {
                    StartFreshOrleans = true,
                    StartPrimary = true,
                    StartSecondary = false
                }) { }

        private const int count = 5;
        private Random random = new Random();
        private TimeSpan failureTimeout = TimeSpan.FromSeconds(15);

        private Logger log = Logger.GetLogger("RingTest", Logger.LoggerType.Application);

        [TestCleanup]
        public void Cleanup()
        {
            ResetAllAdditionalRuntimes();
            ResetDefaultRuntimes();
            CheckForUnobservedPromises();
        }
        
        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("RingStandalone")]
        public void RingStandalone_Basic()
        {
            Dictionary<SiloAddress, ConsistentRingProvider> rings = CreateServers(count);
            rings = Combine(rings, rings);
            VerifyRing(rings);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("RingStandalone")]
        public void RingStandalone_Failures()
        {
            TestChurn(new int[] { 0 }, new int[] { });    // 1 failure in the beginning
            TestChurn(new int[] { 0, 1 }, new int[] { }); // 2 failures in the beginning

            TestChurn(new int[] { count - 1 }, new int[] { });            // 1 failure at the end
            TestChurn(new int[] { count - 1, count - 2 }, new int[] { }); // 2 failures at the end

            TestChurn(new int[] { count / 2 }, new int[] { });                // 1 failure in the middle
            TestChurn(new int[] { count / 2, 1 + count / 2 }, new int[] { }); // 2 failures in the middle

            TestChurn(new int[] { 1, count - 2 }, new int[] { }); // 2 failures at some distance
            TestChurn(new int[] { 0, count - 1 }, new int[] { }); // 2 failures at some distance
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("RingStandalone")]
        public void RingStandalone_Joins()
        {
            TestChurn(new int[] { }, new int[] { 0 });     // 1 join in the beginning
            TestChurn(new int[] { }, new int[] { 0, 1 });  // 2 joins in the beginning

            TestChurn(new int[] { }, new int[] { count - 1 });             // 1 join at the end
            TestChurn(new int[] { }, new int[] { count - 1, count - 2 });  // 2 joins at the end

            TestChurn(new int[] { }, new int[] { count / 2 });                 // 1 join in the middle
            TestChurn(new int[] { }, new int[] { count / 2, 1 + count / 2 });  // 2 joins in the middle

            TestChurn(new int[] { }, new int[] { 1, count - 2 });  // 2 joins at some distance
            TestChurn(new int[] { }, new int[] { 0, count - 1 });  // 2 joins at some distance
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("RingStandalone")]
        public void RingStandalone_Mixed()
        {
            TestChurn(new int[] { 0 }, new int[] { 1 });     // FJ in the beginning
            TestChurn(new int[] { 1 }, new int[] { 0 });     // JF in the beginning

            TestChurn(new int[] { count - 2 }, new int[] { count - 1 });     // FJ at the end
            TestChurn(new int[] { count - 1 }, new int[] { count - 2 });     // JF at the end

            TestChurn(new int[] { count / 2 }, new int[] { 1 + count / 2 });     // FJ in the middle
            TestChurn(new int[] { 1 + count / 2 }, new int[] { count / 2 });     // JF in the middle

            TestChurn(new int[] { 0 }, new int[] { count - 1 });     // F first, J at the end
            TestChurn(new int[] { count - 1 }, new int[] { 0 });     // F last, J at the beginning

        }

        private void TestChurn(int[] indexesFails, int[] indexesJoins)
        {
            Dictionary<SiloAddress, ConsistentRingProvider> rings, holder = new Dictionary<SiloAddress, ConsistentRingProvider>();
            List<SiloAddress> sortedServers, fail = new List<SiloAddress>();

            rings = CreateServers(count);

            sortedServers = rings.Keys.AsList();
            sortedServers.Sort(CompareSiloAddressesByHash);
            // failures
            foreach (int i in indexesFails)
            {
                fail.Add(sortedServers[i]);
            }
            // joins
            foreach (int i in indexesJoins)
            {
                holder.Add(sortedServers[i], rings[sortedServers[i]]);
                rings.Remove(sortedServers[i]);
            }
            rings = Combine(rings, rings);
            RemoveServers(rings, fail);     // fail nodes
            rings = Combine(rings, holder); // join the new nodes
            VerifyRing(rings);
        }

        #region Util methods

        private Dictionary<SiloAddress, ConsistentRingProvider> CreateServers(int n)
        {
            Dictionary<SiloAddress, ConsistentRingProvider> rings = new Dictionary<SiloAddress, ConsistentRingProvider>();

            for (int i = 1; i <= n; i++)
            {
                SiloAddress addr = SiloAddress.NewLocalAddress(i);
                rings.Add(addr, new ConsistentRingProvider(addr));
            }
            return rings;
        }

        private Dictionary<SiloAddress, ConsistentRingProvider> Combine(Dictionary<SiloAddress, ConsistentRingProvider> set1, Dictionary<SiloAddress, ConsistentRingProvider> set2)
        {
            // tell set1 about every node in set2
            foreach (ConsistentRingProvider crp in set1.Values)
            {
                foreach (ConsistentRingProvider other in set2.Values)
                {
                    if (!crp.MyAddress.Equals(other.MyAddress))
                    {
                        other.AddServer(crp.MyAddress);
                    }
                }
            }

            // tell set2 about every node in set1 ... even if set1 and set2 overlap, ConsistentRingProvider should be able to handle
            foreach (ConsistentRingProvider crp in set2.Values)
            {
                foreach (ConsistentRingProvider other in set1.Values)
                {
                    if (!crp.MyAddress.Equals(other.MyAddress))
                    {
                        other.AddServer(crp.MyAddress);
                    }
                }
            }

            // tell set2 about every node in set2 ... note that set1 already knows about nodes in set1
            foreach (ConsistentRingProvider crp in set2.Values)
            {
                foreach (ConsistentRingProvider other in set2.Values)
                {
                    if (!crp.MyAddress.Equals(other.MyAddress))
                    {
                        other.AddServer(crp.MyAddress);
                    }
                }
            }

            // merge the sets
            return set1.Union(set2).ToDictionary(pair => pair.Key, pair => pair.Value);
        }

        private void RemoveServers(Dictionary<SiloAddress, ConsistentRingProvider> rings, List<SiloAddress> remove)
        {
            foreach (SiloAddress addr in remove)
            {
                rings.Remove(addr);
            }

            // tell every node about the failed nodes
            foreach (ConsistentRingProvider crp in rings.Values)
            {
                foreach (SiloAddress addr in remove)
                {
                    crp.RemoveServer(addr);
                }
            }
        }

        private void VerifyRing(Dictionary<SiloAddress, ConsistentRingProvider> rings)
        {
            RangeBreakable fullring = new RangeBreakable();

            foreach (ConsistentRingProvider r in rings.Values)
            {
                // see if there is no overlap between the responsibilities of two nodes
                Assert.IsTrue(fullring.Remove(r.GetMyRange()), string.Format("Couldn't find & break range {0} in {1}. Some other node already claimed responsibility.", r.GetMyRange(), fullring));
            }
            Assert.IsTrue(fullring.NumRanges == 0, string.Format("Range not completely covered. Uncovered ranges: {0}", fullring));
        }

        /// <summary>
        /// Compare SiloAddress-es based on their consistent hash code
        /// </summary>
        /// <param name="x"></param>
        /// <param name="y"></param>
        /// <returns></returns>
        private static int CompareSiloAddressesByHash(SiloAddress x, SiloAddress y)
        {
            if (x == null)
            {
                return y == null ? 0 : -1;
            }
            else
            {
                if (y == null)
                {
                    return 1;
                }
                else
                {
                    // real comparison is here
                    return x.GetConsistentHashCode().CompareTo(y.GetConsistentHashCode());
                }
            }
        }

        #endregion
    }

    internal class RangeBreakable
    {
        private List<SingleRange> ranges { get; set; }
        internal int NumRanges { get { return ranges.Count(); } } 

        public RangeBreakable() 
        {
            ranges = new List<SingleRange>(1);
            ranges.Add(new SingleRange(0, 0));
        }

        public bool Remove(IRingRange range)
        {
            bool wholerange = true;
            foreach (SingleRange s in RangeFactory.GetSubRanges(range))
            {
                bool found = false;
                foreach (SingleRange m in ranges)
                {
                    if (m.Begin == m.End) // treat full range as special case
                    {
                        found = true;
                        ranges.Remove(m);
                        if (s.Begin != s.End) // if s is full range as well, then end of story ... whole range is covered
                        {
                            ranges.Add(new SingleRange(s.End, s.Begin));
                        }
                        break;
                    }

                    if (m.InRange(s.Begin + 1) && m.InRange(s.End)) // s cant overlap two singleranges
                    {
                        found = true;
                        ranges.Remove(m);
                        if (s.Begin != m.Begin)
                        {
                            ranges.Add(new SingleRange(m.Begin, s.Begin));
                        }
                        if (s.End != m.End)
                        {
                            ranges.Add(new SingleRange(s.End, m.End));
                        }
                        break;
                    }
                }
                wholerange = wholerange && found;
            }
            return wholerange;
        }
    }
}
