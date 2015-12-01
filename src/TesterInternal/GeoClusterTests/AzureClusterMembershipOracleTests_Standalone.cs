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
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using Orleans.Runtime;
using Orleans.Runtime.Configuration;
using Orleans.TestingHost;
using Orleans.MultiCluster;
using UnitTests.StorageTests;

namespace Tests.GeoClusterTests
{
    
        /*
         
    // This class tests just the table based functionality for
    // the Cluster Membership Oracle

    // The rest of the tests require running in the
    // context of a silo (are found in the Tester project)
    [TestClass]
    [DeploymentItem("OrleansAzureUtils.dll")]
    public class AzureClusterMembershipOracleTests_Standalone
    {
        private static readonly TimeSpan timeout = TimeSpan.FromMinutes(1);
        private ClusterMembershipTableManager clusterMembershipTable;
        private ClusterMembershipOracleTableBased clusterMembershipOracle;

        private string protocolID1;
        private string protocolID2;

        private static DateTime ts1;
        private static DateTime ts2;

        [ClassInitialize]
        public static void ClassInitialize(TestContext testContext)
        {
            TraceLogger.Initialize(new NodeConfiguration());
        }

        [TestInitialize]
        public void TestInitialize()
        {
            clusterMembershipOracle = new ClusterMembershipOracleTableBased();

            clusterMembershipOracle.Initialize(StorageTestConstants.DataConnectionString)
                .WaitWithThrow(timeout);

            clusterMembershipTable = clusterMembershipOracle.tableManager;

            protocolID1 = "1";
            protocolID2 = "2";

            ts1 = new DateTime(year: 2011, month: 3, day: 1);
            ts2 = new DateTime(year: 2010, month: 1, day: 1);
        }

        [TestCleanup]
        public void TestCleanup()
        {
            if (clusterMembershipTable != null && SiloInstanceTableTestConstants.DeleteEntriesAfterTest)
            {
                clusterMembershipTable.DeleteTable();
                clusterMembershipTable = null;
            }
        }

        [TestMethod, TestCategory("Functional"), TestCategory("Replication"), TestCategory("Azure"), TestCategory("Storage")]
        public async Task AzureClusterMembership_TableBasedAPITest()
        {
            // start clean
            await clusterMembershipTable.DeleteAllEntriesForProtocol(protocolID1);
            await clusterMembershipTable.DeleteAllEntriesForProtocol(protocolID2);
            await clusterMembershipTable.DeleteCurrentConfigEntry();

            // get the current replica view 
            var answer = await clusterMembershipOracle.GetCurrentReplicaView(protocolID1);
            Assert.IsTrue(answer.rv.replicas == null);

            // install a new configuration
            await clusterMembershipTable.TryUpdateCurrentConfigurationAsync(ts1, "A,B,C");

            // get the current replica view 
            answer = await clusterMembershipOracle.GetCurrentReplicaView(protocolID1);
            Assert.IsTrue(answer.rv.replicas != null);
            Assert.IsTrue(answer.rv.replicas.Count() == 3);
            Assert.IsTrue(answer.rv.replicas.Contains("A"));
            Assert.IsTrue(answer.rv.replicas.Contains("B"));
            Assert.IsTrue(answer.rv.replicas.Contains("C"));
            Assert.IsTrue(answer.acks.Count == 0);
            Assert.IsTrue(answer.rv.viewId == ts1);

            // try to install an older config
            await clusterMembershipTable.TryUpdateCurrentConfigurationAsync(ts2, "A,B");

            // get the current replica view, make sure it stayed the same
            answer = await clusterMembershipOracle.GetCurrentReplicaView(protocolID1);
            Assert.IsTrue(answer.rv.replicas != null);
            Assert.IsTrue(answer.rv.replicas.Count() == 3);
            Assert.IsTrue(answer.rv.replicas.Contains("A"));
            Assert.IsTrue(answer.rv.replicas.Contains("B"));
            Assert.IsTrue(answer.rv.replicas.Contains("C"));
            Assert.IsTrue(answer.acks.Count == 0);
            Assert.IsTrue(answer.rv.viewId == ts1);

            // ack the current config
            var response = Encoding.UTF8.GetBytes("response");
            await clusterMembershipOracle.AckReplicaView(ts1, protocolID1, "A", response);

            // get the current replica view, make sure I get the ack
            answer = await clusterMembershipOracle.GetCurrentReplicaView(protocolID1);
            Assert.IsTrue(answer.rv.replicas != null);
            Assert.IsTrue(answer.rv.replicas.Count() == 3);
            Assert.IsTrue(answer.rv.replicas.Contains("A"));
            Assert.IsTrue(answer.rv.replicas.Contains("B"));
            Assert.IsTrue(answer.rv.replicas.Contains("C"));
            Assert.IsTrue(answer.rv.viewId == ts1);
            Assert.IsTrue(answer.acks.Count > 0);
            Assert.IsTrue(answer.acks["A"][0] == response[0]);
        }
         
    }
         */
}