using System;
using System.Net;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans.Messaging;

using Orleans;
using Orleans.AzureUtils;

namespace UnitTests.MessageCenterTests
{
    [TestClass]
    public class GatewaySelectionTest
    {
        private class TestListProvider : IGatewayListProvider
        {
            private List<IPEndPoint> list;

            public TestListProvider()
            {
                list = new List<IPEndPoint>();
                list.Add(new IPEndPoint(IPAddress.Loopback, 1));
                list.Add(new IPEndPoint(IPAddress.Loopback, 2));
                list.Add(new IPEndPoint(IPAddress.Loopback, 3));
                list.Add(new IPEndPoint(IPAddress.Loopback, 4));
            }

            #region Implementation of IGatewayListProvider

            public List<IPEndPoint> GetGateways()
            {
                return list;
            }

            public TimeSpan MaxStaleness
            {
                get { return TimeSpan.FromMinutes(1); }
            }

            public bool IsUpdatable
            {
                get { return false; }
            }

            #endregion
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public void GatewaySelection()
        {
            var listProvider = new TestListProvider();
            var cfg = new ClientConfiguration();
            cfg.Gateways = listProvider.GetGateways().ToList();
            var gatewayManager = new GatewayManager(cfg, listProvider);

            var counts = new int[4];

            for (int i = 0; i < 2300; i++)
            {
                var ip = gatewayManager.GetLiveGateway();
                Assert.AreEqual(IPAddress.Loopback, ip.Address, "Incorrect IP address returned for gateway");
                Assert.IsTrue((0 < ip.Port) && (ip.Port < 5), "Incorrect IP port returned for gateway");
                counts[ip.Port - 1]++;
            }

            // The following needed to be changed as the gateway manager now round-robins through the available gateways, rather than
            // selecting randomly based on load numbers.
            //Assert.IsTrue((500 < counts[0]) && (counts[0] < 1500), "Gateway selection is incorrectly skewed");
            //Assert.IsTrue((500 < counts[1]) && (counts[1] < 1500), "Gateway selection is incorrectly skewed");
            //Assert.IsTrue((125 < counts[2]) && (counts[2] < 375), "Gateway selection is incorrectly skewed");
            //Assert.IsTrue((25 < counts[3]) && (counts[3] < 75), "Gateway selection is incorrectly skewed");
            //Assert.IsTrue((287 < counts[0]) && (counts[0] < 1150), "Gateway selection is incorrectly skewed");
            //Assert.IsTrue((287 < counts[1]) && (counts[1] < 1150), "Gateway selection is incorrectly skewed");
            //Assert.IsTrue((287 < counts[2]) && (counts[2] < 1150), "Gateway selection is incorrectly skewed");
            //Assert.IsTrue((287 < counts[3]) && (counts[3] < 1150), "Gateway selection is incorrectly skewed");

            int low = 2300 / 4;
            int up = 2300 / 4;
            Assert.IsTrue((low <= counts[0]) && (counts[0] <= up), "Gateway selection is incorrectly skewed. " + counts[0]);
            Assert.IsTrue((low <= counts[1]) && (counts[1] <= up), "Gateway selection is incorrectly skewed. " + counts[1]);
            Assert.IsTrue((low <= counts[2]) && (counts[2] <= up), "Gateway selection is incorrectly skewed. " + counts[2]);
            Assert.IsTrue((low <= counts[3]) && (counts[3] <= up), "Gateway selection is incorrectly skewed. " + counts[3]);
        }

        [TestMethod]
        public void GatewaySelection_EmptyList()
        {
            var cfg = new ClientConfiguration();
            cfg.Gateways = null;
            bool failed = false;
            try
            {
                OrleansClient.Initialize(cfg);
            }
            catch (Exception exc)
            {
                Console.WriteLine(exc);
                failed = true;
            }
            Assert.IsTrue(failed, "GatewaySelection_EmptyList failed as GatewayManager did not throw on empty GW list.");

            var listProvider = new TestListProvider();
            cfg.Gateways = listProvider.GetGateways().ToList();
            OrleansClient.Initialize(cfg);
        } 
    }
}
