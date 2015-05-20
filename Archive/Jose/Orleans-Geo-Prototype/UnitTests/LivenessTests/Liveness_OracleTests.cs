using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using Orleans;

using Orleans.Runtime;
using Orleans.Management;

using System.Net;

namespace UnitTests
{
    /// <summary>
    /// Summary description for ErrorHandlingGrainTest
    /// </summary>
    [TestClass]
    public class Liveness_OracleTests : UnitTestBase
    {
        public Liveness_OracleTests()
            : base(new Options
            {
                StartFreshOrleans = true,
                StartPrimary = true,
                StartSecondary = true
            })
        {
        }

        [TestCleanup()]
        public void Cleanup()
        {
            ResetAllAdditionalRuntimes();
            ResetDefaultRuntimes();
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Liveness")]
        public void Liveness_OracleTest_1()
        {
            SiloHandle orleans2 = StartAdditionalOrleans();

            IOrleansManagementGrain mgmtGrain = OrleansManagementGrainFactory.GetGrain(RuntimeInterfaceConstants.SystemManagementId);

            Dictionary<SiloAddress, SiloStatus> statuses = mgmtGrain.GetHosts(false).Result;
            foreach (var pair in statuses)
            {
                Console.WriteLine(String.Format("       ######## Silo {0}, status: {1}", pair.Key, pair.Value));
                Assert.AreEqual(SiloStatus.Active, pair.Value);
            }
            Assert.AreEqual(3, statuses.Count);

            IPEndPoint address = orleans2.Endpoint;
            Console.WriteLine(String.Format("About to reset {0}", address));
            ResetRuntime(orleans2);

            Console.WriteLine("----------------");

            foreach (var pair in mgmtGrain.GetHosts(false).Result)
            {
                Console.WriteLine(String.Format("       ######## Silo {0}, status: {1}", pair.Key, pair.Value));
                if (pair.Key.Endpoint.Equals(address))
                {
                    Assert.IsTrue(pair.Value.Equals(SiloStatus.ShuttingDown) || pair.Value.Equals(SiloStatus.Stopping) || pair.Value.Equals(SiloStatus.Dead), pair.Value.ToString());
                }
                else
                {
                    Assert.AreEqual(SiloStatus.Active, pair.Value);
                }
            }
        }
    }
}
