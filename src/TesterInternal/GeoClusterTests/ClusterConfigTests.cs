using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans.Runtime.Configuration;

namespace Tests.GeoClusterTests
{
    [TestClass]
    [DeploymentItem("Config_Cluster0.xml")]
    [DeploymentItem("Config_Cluster1.xml")]
    public class ClusterConfigTests
    {
        [TestMethod, TestCategory("BVT"), TestCategory("Functional"), TestCategory("GeoCluster")]
        public void LoadClusterGatewayConfig()
        {
            string configCluster1 = TestingClusterHost.GetConfigFile("Config_Cluster0.xml");
            string configCluster2 = TestingClusterHost.GetConfigFile("Config_Cluster1.xml");
            ClusterConfiguration cfg1 = new ClusterConfiguration();
            ClusterConfiguration cfg2 = new ClusterConfiguration();
            cfg1.LoadFromFile(configCluster1);
            cfg2.LoadFromFile(configCluster2);
            Assert.AreEqual("0", cfg1.Globals.ClusterId, "ClusterId");
            Assert.AreEqual("1", cfg2.Globals.ClusterId, "ClusterId");
        }
    }
}
