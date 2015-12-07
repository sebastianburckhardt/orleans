/*
Project Orleans Cloud Service SDK ver. 1.0
 
Copyright (c) Microsoft Corporation
 
All rights reserved.
 
MIT License

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and 
associated documentation files (the ""Software""), to deal in the Software without restriction,
including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense,
and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using Orleans.Runtime;
using Orleans.Runtime.Configuration;
using Orleans.MultiCluster;
using System.Net;
using Orleans.Runtime.MultiClusterNetwork;
using Orleans.TestingHost;


// ReSharper disable InconsistentNaming

namespace Tests.GeoClusterTests
{
    // We need use ClientWrapper to load a client object in a new app domain. 
    // This allows us to create multiple clients that are connected to different silos.

    [TestClass]
    [DeploymentItem("OrleansAzureUtils.dll")]
    [DeploymentItem("TestGrainInterfaces.dll")]
    [DeploymentItem("TestGrains.dll")]
    [DeploymentItem("ClientConfigurationForTesting.xml")]
    [DeploymentItem("OrleansConfigurationForTesting.xml")]
    public class MultiClusterNetworkTests : TestingClusterHost
    {
        [ClassInitialize]
        public static void ClassInitialize(TestContext testContext)
        {
            TestingSiloHost.StopAllSilos();
        }

        // Kill all clients and silos.
        [TestCleanup]
        public void TestCleanup()
        {
            try
            {
                StopAllClientsAndClusters();
            }
            catch (Exception e)
            {
                WriteLog("Exception caught in test cleanup function: {0}", e);
            }
        }

        public class ClientWrapper : ClientWrapperBase
        {
            public ClientWrapper(string name, int gatewayport) : base(name, gatewayport)
            {
                systemManagement = GrainClient.GrainFactory.GetGrain<IManagementGrain>(RuntimeInterfaceConstants.SYSTEM_MANAGEMENT_ID);
            }
            IManagementGrain systemManagement;

            public MultiClusterConfiguration InjectMultiClusterConf(IEnumerable<string> clusters, string comment = "")
            {
                return systemManagement.InjectMultiClusterConfiguration(clusters, comment).Result;
            }

            public MultiClusterConfiguration GetMultiClusterConfiguration()
            {
                return systemManagement.GetMultiClusterConfiguration().Result;
            }

            public List<IMultiClusterGatewayInfo> GetMultiClusterGateways()
            {
                return systemManagement.GetMultiClusterGateways().Result;
            }

            public Dictionary<SiloAddress,SiloStatus> GetHosts()
            {
                return systemManagement.GetHosts().Result;
            }
        }


 


        [TestMethod, TestCategory("GeoCluster"), TestCategory("Functional")]
        [Timeout(120000)]
        public async Task TestMultiClusterConf_1_1()
        {
            // use a random global service id for testing purposes
            var globalserviceid = "testservice" + new Random().Next();

            // create cluster A and clientA
            Action<ClusterConfiguration> customizerA = (ClusterConfiguration c) =>
            {
                c.Globals.GlobalServiceId = globalserviceid;
                c.Globals.ClusterId = "A";
                c.Globals.DefaultMultiCluster = null;
            };
            var clusterA = NewCluster(1, customizerA);
            var siloA = clusters[clusterA].Silos[0].Silo.SiloAddress.Endpoint;
            var clientA = NewClient<ClientWrapper>(clusterA, 0);

            var cur = clientA.GetMultiClusterConfiguration();
            Assert.IsNull(cur, "no configuration should be there yet");

            await TestingSiloHost.WaitForMultiClusterGossipToStabilizeAsync(false);

            cur = clientA.GetMultiClusterConfiguration();
            Assert.IsNull(cur, "no configuration should be there yet");

            var gateways = clientA.GetMultiClusterGateways();
            Assert.AreEqual(1, gateways.Count, "Expect 1 gateway");
            Assert.AreEqual("A", gateways[0].ClusterId);
            Assert.AreEqual(siloA, gateways[0].SiloAddress.Endpoint);
            Assert.AreEqual(GatewayStatus.Active, gateways[0].Status);

            // create cluster B and clientB
            Action<ClusterConfiguration> customizerB = (ClusterConfiguration c) =>
            {
                c.Globals.GlobalServiceId = globalserviceid;
                c.Globals.ClusterId = "B";
                c.Globals.DefaultMultiCluster = null;
            };
            var clusterB = NewCluster(1, customizerB);
            var siloB = clusters[clusterB].Silos[0].Silo.SiloAddress.Endpoint;
            var clientB = NewClient<ClientWrapper>(clusterB, 0);

            cur = clientB.GetMultiClusterConfiguration();
            Assert.IsNull(cur, "no configuration should be there yet");

            await TestingSiloHost.WaitForMultiClusterGossipToStabilizeAsync(false);

            cur = clientB.GetMultiClusterConfiguration();
            Assert.IsNull(cur, "no configuration should be there yet");

            gateways = clientA.GetMultiClusterGateways();
            Assert.AreEqual(2, gateways.Count, "Expect 2 gateways");
            gateways.Sort((a, b) => a.ClusterId.CompareTo(b.ClusterId));
            Assert.AreEqual("A", gateways[0].ClusterId);
            Assert.AreEqual(siloA, gateways[0].SiloAddress.Endpoint);
            Assert.AreEqual(GatewayStatus.Active, gateways[0].Status);
            Assert.AreEqual("B", gateways[1].ClusterId);
            Assert.AreEqual(siloB, gateways[1].SiloAddress.Endpoint);
            Assert.AreEqual(GatewayStatus.Active, gateways[1].Status);

            for (int i = 0; i < 2; i++)
            {
                // test injection
                var conf = clientA.InjectMultiClusterConf("A,B".Split(','), "my conf " + i);

                // immediately visible on A, visible after stabilization on B
                cur = clientA.GetMultiClusterConfiguration();
                Assert.IsTrue(conf.Equals(cur));
                await TestingSiloHost.WaitForMultiClusterGossipToStabilizeAsync(false);
                cur = clientA.GetMultiClusterConfiguration();
                Assert.IsTrue(conf.Equals(cur));
                cur = clientB.GetMultiClusterConfiguration();
                Assert.IsTrue(conf.Equals(cur));
            }

            // shut down cluster B
            TestingSiloHost.StopSilo(clusters[clusterB].Silos[0]);
            await TestingSiloHost.WaitForLivenessToStabilizeAsync();

            // expect disappearance of gateway from multicluster network
            await TestingSiloHost.WaitForMultiClusterGossipToStabilizeAsync(false);
            gateways = clientA.GetMultiClusterGateways();
            Assert.AreEqual(2, gateways.Count, "Expect 2 gateways");
            var activegateways = gateways.Where(g => g.Status == GatewayStatus.Active).ToList();
            Assert.AreEqual(1, activegateways.Count, "Expect 1 active gateway");
            Assert.AreEqual("A", activegateways[0].ClusterId);

            StopAllClientsAndClusters();
        }

        private void AssertSameList(List<IMultiClusterGatewayInfo> a, List<IMultiClusterGatewayInfo> b)
        {
            Comparison<IMultiClusterGatewayInfo> comparer = (x, y) => x.SiloAddress.Endpoint.ToString().CompareTo(y.SiloAddress.Endpoint.ToString());
            a.Sort(comparer);
            b.Sort(comparer);
            Assert.AreEqual(a.Count, b.Count, "number of gateways must match");
            for (int i = 0; i < a.Count; i++) {
                Assert.AreEqual(a[i].SiloAddress, b[i].SiloAddress, "silo address at pos " + i + " must match");
                Assert.AreEqual(a[i].ClusterId, b[i].ClusterId, "cluster id at pos " + i + " must match");
                Assert.AreEqual(a[i].Status, b[i].Status, "status at pos " + i + " must match");
            }
        }

  

        [TestMethod, TestCategory("GeoCluster"), TestCategory("Functional")]
        [Timeout(120000)]
        public async Task TestMultiClusterConf_3_3()
        {
            // use a random global service id for testing purposes
            var globalserviceid = "testservice" + new Random().Next();

            // create cluster A and clientA
            Action<ClusterConfiguration> customizerA = (ClusterConfiguration c) =>
            {
                c.Globals.GlobalServiceId = globalserviceid;
                c.Globals.ClusterId = "A";
                c.Globals.DefaultMultiCluster = null;
                c.Globals.NumMultiClusterGateways = 2;
                c.Globals.DefaultMultiCluster = "A,B".Split(',').ToList();
            };
            var clusterA = NewCluster(3, customizerA);
            var clientA = NewClient<ClientWrapper>(clusterA, 0);
            // create cluster B and clientB
            Action<ClusterConfiguration> customizerB = (ClusterConfiguration c) =>
            {
                c.Globals.GlobalServiceId = globalserviceid;
                c.Globals.ClusterId = "B";
                c.Globals.DefaultMultiCluster = null;
            };
            var clusterB = NewCluster(3, customizerB);
            var clientB = NewClient<ClientWrapper>(clusterB, 0);

            await TestingSiloHost.WaitForMultiClusterGossipToStabilizeAsync(false);

            // check that default configuration took effect
            var cur = clientA.GetMultiClusterConfiguration();
            Assert.IsTrue(cur != null && string.Join(",", cur.Clusters) == "A,B");
            AssertSameList(clientA.GetMultiClusterGateways(), clientB.GetMultiClusterGateways());

            // expect 4 active gateways, two per cluster
            var activegateways = clientA.GetMultiClusterGateways().Where(g => g.Status == GatewayStatus.Active).ToList();
            Assert.AreEqual("21111,21112", string.Join(",", activegateways.Where(g => g.ClusterId == "A").Select(g => g.SiloAddress.Endpoint.Port).OrderBy(x => x)));
            Assert.AreEqual("21116,21117", string.Join(",", activegateways.Where(g => g.ClusterId == "B").Select(g => g.SiloAddress.Endpoint.Port).OrderBy(x => x)));
            var activegatewaysB = clientB.GetMultiClusterGateways().Where(g => g.Status == GatewayStatus.Active).ToList();
 
            // shut down one of the gateways in cluster B gracefully
            var target = clusters[clusterB].Silos.Where(h => h.Endpoint.Port == 21117).FirstOrDefault();
            Assert.IsNotNull(target);
            TestingSiloHost.StopSilo(target);
            await TestingSiloHost.WaitForLivenessToStabilizeAsync();

            // expect disappearance and replacement of gateway from multicluster network
            await TestingSiloHost.WaitForMultiClusterGossipToStabilizeAsync(false);
            AssertSameList(clientA.GetMultiClusterGateways(), clientB.GetMultiClusterGateways());
            activegateways = clientA.GetMultiClusterGateways().Where(g => g.Status == GatewayStatus.Active).ToList();
            Assert.AreEqual("21111,21112", string.Join(",", activegateways.Where(g => g.ClusterId == "A").Select(g => g.SiloAddress.Endpoint.Port).OrderBy(x => x)));
            Assert.AreEqual("21116,21118", string.Join(",", activegateways.Where(g => g.ClusterId == "B").Select(g => g.SiloAddress.Endpoint.Port).OrderBy(x => x)));
     

            // kill one of the gateways in cluster A
            target = clusters[clusterA].Silos.Where(h => h.Endpoint.Port == 21112).FirstOrDefault();
            Assert.IsNotNull(target);
            TestingSiloHost.KillSilo(target);
            await TestingSiloHost.WaitForLivenessToStabilizeAsync();

            // wait for time necessary before peer removal can kick in
            await Task.Delay(MultiClusterOracle.CleanupSilentGoneGatewaysAfter);

            // wait for membership protocol to determine death of A
            while (true)
            {
                var hosts = clientA.GetHosts();
                var killedone = hosts.Where(kvp => kvp.Key.Endpoint.Port == 21112).FirstOrDefault();
                Assert.IsTrue(killedone.Value != SiloStatus.None);
                if (killedone.Value == SiloStatus.Dead)
                    break;
                await Task.Delay(100);
            }

            // wait for gossip propagation
            await TestingSiloHost.WaitForMultiClusterGossipToStabilizeAsync(false);

            AssertSameList(clientA.GetMultiClusterGateways(), clientB.GetMultiClusterGateways());
            activegateways = clientA.GetMultiClusterGateways().Where(g => g.Status == GatewayStatus.Active).ToList();
            Assert.AreEqual("21111,21113", string.Join(",", activegateways.Where(g => g.ClusterId == "A").Select(g => g.SiloAddress.Endpoint.Port).OrderBy(x => x)));
            Assert.AreEqual("21116,21118", string.Join(",", activegateways.Where(g => g.ClusterId == "B").Select(g => g.SiloAddress.Endpoint.Port).OrderBy(x => x)));

            StopAllClientsAndClusters();

        }
    }
}
