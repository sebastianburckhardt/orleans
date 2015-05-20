using System;
using System.Net;
using System.Threading;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;

using SimpleGrain;
using UnitTestGrainInterfaces;

#pragma warning disable 618

namespace UnitTests
{
    /// <summary>
    /// </summary>
    [TestClass]
    public class Liveness_Set_2_SelfMngGrain : UnitTestBase
    {
        private const int numAdditionalSilos = 1;
        private const int numGrains = 100;

        public Liveness_Set_2_SelfMngGrain()
            : base(new Options
                    {
                        StartFreshOrleans = true,
                        StartPrimary = true,
                        StartSecondary = true,
                        StartOutOfProcess = false
                    }, new ClientOptions
                    {
                        ProxiedGateway = true,
                        Gateways = new List<IPEndPoint>(new IPEndPoint[] { new IPEndPoint(IPAddress.Loopback, 30000), new IPEndPoint(IPAddress.Loopback, 30001) }),
                        PreferedGatewayIndex = 1
                    })
        {
        }


        [TestCleanup()]
        public void TestCleanup()
        {
            ResetAllAdditionalRuntimes();
            ResetDefaultRuntimes();
        }

        public void Liveness_Set_2_Kill_Primary()
        {
            Liveness_Set_2_Runner(0);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Liveness")]
        public void Liveness_Set_2_Kill_GW()
        {
            Liveness_Set_2_Runner(1);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Liveness")]
        public void Liveness_Set_2_Kill_Silo_1()
        {
            Liveness_Set_2_Runner(2);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Liveness")]
        public void Liveness_Set_2_Kill_Silo_1_With_Timers()
        {
            Liveness_Set_2_Runner(2, false, true);
        }

        private void Liveness_Set_2_Runner(int silo2Stop, bool softKill = true, bool startTimers = false)
        {
            List<SiloHandle> additionalSilos = StartAdditionalOrleansRuntimes(numAdditionalSilos);
            WaitForLivenessToStabilize();

            List<ISimpleSelfManagedGrain> grains = new List<ISimpleSelfManagedGrain>();
            for (int i = 0; i < numGrains; i++)
            {
                long key = i + 1;
                ISimpleSelfManagedGrain g1 = SimpleSelfManagedGrainFactory.GetGrain(key);
                grains.Add(g1);
                Assert.AreEqual(key, g1.GetPrimaryKeyLong());
                //Assert.AreEqual(key, g1.GetKey().Result);
                Assert.AreEqual(key.ToString(), g1.GetLabel().Result);
                if (startTimers)
                {
                    g1.StartTimer().Wait();
                }
                logger.Info("Grain {0}, activation {1} on {2}", g1.GetGrainId().Result, g1.GetActivationId().Result, g1.GetRuntimeInstanceId().Result);
            }

            SiloHandle silo2Kill = null;
            if (silo2Stop==0)
                silo2Kill = Primary;
            else if (silo2Stop == 1)
                silo2Kill = Secondary;
            else
                silo2Kill = additionalSilos[silo2Stop - 2];

            logger.Info("\n\n\n\nAbout to kill {0}\n\n\n", silo2Kill.Endpoint);

            if (softKill)
                ResetRuntime(silo2Kill);
            else
                KillRuntime(silo2Kill);

            WaitForLivenessToStabilize(softKill);

            logger.Info("\n\n\n\nAbout to start sending msg to grain again\n\n\n");

            for (int i = 0; i < grains.Count; i++)
            {
                long key = i + 1;
                ISimpleSelfManagedGrain g1 = grains[(int)i];
                Assert.AreEqual(key, g1.GetPrimaryKeyLong());
                Assert.AreEqual(key.ToString(), g1.GetLabel().Result);
                logger.Info("Grain {0}, activation {1} on {2}", g1.GetGrainId().Result, g1.GetActivationId().Result, g1.GetRuntimeInstanceId().Result);
            }

            for (int i = numGrains; i < 2 * numGrains; i++)
            {
                long key = i + 1;
                ISimpleSelfManagedGrain g1 = SimpleSelfManagedGrainFactory.GetGrain(key);
                grains.Add(g1);
                Assert.AreEqual(key, g1.GetPrimaryKeyLong());
                Assert.AreEqual(key.ToString(), g1.GetLabel().Result);
                logger.Info("Grain {0}, activation {1} on {2}", g1.GetGrainId().Result, g1.GetActivationId().Result, g1.GetRuntimeInstanceId().Result);
            }
            logger.Info("======================================================");
        }
    }
}

#pragma warning restore 618
