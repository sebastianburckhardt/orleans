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
using System.Net;
using Orleans.TestingHost;


// ReSharper disable InconsistentNaming

namespace Tests.GeoClusterTests
{
    // We need use ClientWrapper to load a client object in a new app domain. 
    // This allows us to create multiple clients that are connected to different silos.

    [TestClass]
    [DeploymentItem("ClientConfigurationForTesting.xml")]
    [DeploymentItem("OrleansConfigurationForTesting.xml")]
    public class BasicMultiClusterTest : TestingClusterHost
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

            public Dictionary<SiloAddress,SiloStatus> GetHosts()
            {
                return systemManagement.GetHosts().Result;
            }
        }

        [TestMethod, TestCategory("GeoCluster"), TestCategory("Functional")]
        [Timeout(120000)]
        public void CreateTwoIndependentClusters()
        {
           
            // create cluster A with one silo and clientA
            var clusterA = "A";
            NewCluster(clusterA, 1);
            var clientA = NewClient<ClientWrapper>(clusterA, 0);

            // create cluster B with 5 silos and clientB
            var clusterB = "B";
            NewCluster(clusterB, 5);
            var clientB = NewClient<ClientWrapper>(clusterB, 0);

            // call management grain in each cluster to count the silos
            var silos_in_A = clientA.GetHosts().Count;
            var silos_in_B = clientB.GetHosts().Count;

            Assert.AreEqual(1, silos_in_A);
            Assert.AreEqual(5, silos_in_B);

            StopAllClientsAndClusters();
        }

       

    }
}
