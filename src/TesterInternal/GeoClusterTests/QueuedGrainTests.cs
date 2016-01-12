﻿/*
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
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using Orleans.TestingHost;
using UnitTests.GrainInterfaces;
using Orleans.Runtime.Configuration;
using Orleans.Runtime;
using Orleans.MultiCluster;
using Tests.GeoClusterTests;

namespace Tester.GeoClusterTests
{
    [TestClass]
    [DeploymentItem("TestGrainInterfaces.dll")]
    [DeploymentItem("TestGrains.dll")]
    [DeploymentItem("OrleansAzureUtils.dll")]
    [DeploymentItem("OrleansProviders.dll")]
    [DeploymentItem("OrleansConfigurationForTesting.xml")]
    [DeploymentItem("ClientConfigurationForTesting.xml")]
    public class QueuedGrainTests
    {

        private static TestingClusterHost host;

        private const string Cluster0 = "A";
        private const string Cluster1 = "B";

        private static ClientWrapper Client0;
        private static ClientWrapper Client1;

        // change this as needed for debugging failing tests
        private const Severity LogViewProviderTraceLevel = Severity.Verbose2;

        [ClassInitialize]
        public static void SetupMultiCluster(TestContext c)
        {
            TimeSpan waitTimeout = TimeSpan.FromSeconds(60);

            // use a random global service id for testing purposes
            var globalserviceid = "testservice" + new Random().Next();

            // configure replication providers
            Action<ClusterConfiguration> configurationcustomizer = (ClusterConfiguration cc) =>
                {
                    ReplicationProviderConfiguration.Adjust(cc);
                    foreach (var o in cc.Overrides)
                      o.Value.TraceLevelOverrides.Add(new Tuple<string, Severity>("LogViews", Severity.Verbose2));
                };

            host = new TestingClusterHost();

            // Create two clusters, each with 2 silos. 
            host.NewGeoCluster(globalserviceid, Cluster0, 2, configurationcustomizer);
            host.NewGeoCluster(globalserviceid, Cluster1, 2, configurationcustomizer);

            TestingSiloHost.WaitForLivenessToStabilizeAsync().WaitWithThrow(waitTimeout);

            // Create clients.
            Client0 = host.NewClient<ClientWrapper>(Cluster0, 0);
            Client1 = host.NewClient<ClientWrapper>(Cluster1, 0);

            Client1.InjectClusterConfiguration(Cluster0, Cluster1);
            TestingSiloHost.WaitForMultiClusterGossipToStabilizeAsync(false).WaitWithThrow(waitTimeout);

        }

        // Kill all clients and silos.
        [ClassCleanup]
        public static void CleanupCluster()
        {
            try
            {
                host.StopAllClientsAndClusters();
                host = null;
            }
            catch (Exception e)
            {
                TestingSiloHost.WriteLog("Exception caught in test cleanup function: {0}", e);
            }
        }

        #region client wrappers

        public class ClientWrapper : Tests.GeoClusterTests.TestingClusterHost.ClientWrapperBase
        {
            public ClientWrapper(string name, int gatewayport)
               : base(name, gatewayport)
            {
                 systemManagement = GrainClient.GrainFactory.GetGrain<IManagementGrain>(RuntimeInterfaceConstants.SYSTEM_MANAGEMENT_ID);
            }

            public string GetGrainRef(string grainclass, int i)
            {
                return GrainClient.GrainFactory.GetGrain<ISimpleQueuedGrain>(i, grainclass).ToString();
            }

            public void SetALocal(string grainclass, int i, int a)
            {
                var grainRef = GrainClient.GrainFactory.GetGrain<ISimpleQueuedGrain>(i, grainclass);
                grainRef.SetALocal(a).Wait();
            }

            public void SetAGlobal(string grainclass, int i, int a)
            {
                var grainRef = GrainClient.GrainFactory.GetGrain<ISimpleQueuedGrain>(i, grainclass);
                grainRef.SetAGlobal(a).Wait();
            }

            public void IncrementAGlobal(string grainclass, int i)
            {
                var grainRef = GrainClient.GrainFactory.GetGrain<ISimpleQueuedGrain>(i, grainclass);
                grainRef.IncrementAGlobal().Wait();
            }

            public void IncrementALocal(string grainclass, int i)
            {
                var grainRef = GrainClient.GrainFactory.GetGrain<ISimpleQueuedGrain>(i, grainclass);
                grainRef.IncrementALocal().Wait();
            }

            public int GetAGlobal(string grainclass, int i)
            {
                var grainRef = GrainClient.GrainFactory.GetGrain<ISimpleQueuedGrain>(i, grainclass);
                return grainRef.GetAGlobal().Result;
            }

            public int GetALocal(string grainclass, int i)
            {
                var grainRef = GrainClient.GrainFactory.GetGrain<ISimpleQueuedGrain>(i, grainclass);
                return grainRef.GetALocal().Result;
            }
            public void SetBLocal(string grainclass, int i, int a)
            {
                var grainRef = GrainClient.GrainFactory.GetGrain<ISimpleQueuedGrain>(i, grainclass);
                grainRef.SetBLocal(a).Wait();
            }

            public void SetBGlobal(string grainclass, int i, int a)
            {
                var grainRef = GrainClient.GrainFactory.GetGrain<ISimpleQueuedGrain>(i, grainclass);
                grainRef.SetBGlobal(a).Wait();
            }

            public void AddReservationLocal(string grainclass, int i, int x)
            {
                var grainRef = GrainClient.GrainFactory.GetGrain<ISimpleQueuedGrain>(i, grainclass);
                grainRef.AddReservationLocal(x).Wait();
            }

            public void RemoveReservationLocal(string grainclass, int i, int x)
            {
                var grainRef = GrainClient.GrainFactory.GetGrain<ISimpleQueuedGrain>(i, grainclass);
                grainRef.RemoveReservationLocal(x).Wait();
            }

            public int[] GetReservationsGlobal(string grainclass, int i)
            {
                var grainRef = GrainClient.GrainFactory.GetGrain<ISimpleQueuedGrain>(i, grainclass);
                return grainRef.GetReservationsGlobal().Result;
            }

            public void Synchronize(string grainclass, int i)
            {
                var grainRef = GrainClient.GrainFactory.GetGrain<ISimpleQueuedGrain>(i, grainclass);
                grainRef.SynchronizeGlobalState().Wait();
            }

            public void InjectClusterConfiguration(params string[] clusters)
            {
                systemManagement.InjectMultiClusterConfiguration(clusters).Wait();
            }
            IManagementGrain systemManagement;

        }

        #endregion

  
        [TestMethod, TestCategory("Functional"), TestCategory("Replication"), TestCategory("Azure")]
        public async Task ReplicationTestBattery_SharedStorageProvider()
        {
            await DoReplicationTests("UnitTests.Grains.SimpleQueuedGrainSharedStorage");
        }
    

        private async Task DoReplicationTests(string grainClass, int phases = 100)
        {
            await FourCheckers(grainClass, phases);
        }

     

        private async Task FourCheckers(string grainClass, int phases)
        {
            Random random = new Random();

            Func<int> GetRandom = () =>
            {
               lock (random)
                   return random.Next();
            };

             Func<Task> checker1 = () => Task.Run(() =>
            {
                int x = GetRandom();
                // force creation of replicas
                Assert.AreEqual(0, Client0.GetALocal(grainClass, x));
                Assert.AreEqual(0, Client1.GetALocal(grainClass, x));
                // write global on client 0
                Client0.SetAGlobal(grainClass, x, 333);
                // read global on client 1
                int r = Client1.GetAGlobal(grainClass, x);
                Assert.AreEqual(333, r, "grainref={0}", Client0.GetGrainRef(grainClass, x));
                // check local stability
                Assert.AreEqual(333, Client0.GetALocal(grainClass, x), "grainref={0}", Client0.GetGrainRef(grainClass, x));
                Assert.AreEqual(333, Client1.GetALocal(grainClass, x), "grainref={0}", Client0.GetGrainRef(grainClass, x));
            });

            Func<Task> checker2 = () => Task.Run(() =>
            {
                int x = GetRandom();
                // increment on replica 1
                Client1.IncrementAGlobal(grainClass, x);
                // expect on replica 0
                int r = Client0.GetAGlobal(grainClass, x);
                Assert.AreEqual(1, r, "grainref={0}", Client0.GetGrainRef(grainClass, x));
            });

            Func<Task> checker2b = () => Task.Run(() =>
            {
                int x = GetRandom();
                // force creation on replica 0
                Assert.AreEqual(0, Client0.GetAGlobal(grainClass, x));
                // increment on replica 1
                Client1.IncrementAGlobal(grainClass, x);
                // expect on replica 0
                int r = Client0.GetAGlobal(grainClass, x);
                Assert.AreEqual(1, r, "grainref={0}", Client0.GetGrainRef(grainClass, x));
            });

            Func<int,Task> checker3 = (int numupdates) => Task.Run(() =>
            {
                int x = GetRandom();

                // concurrently chaotically increment (numupdates) times
                Parallel.For(0, numupdates, i => (i % 2 == 0 ? Client0 : Client1).IncrementALocal(grainClass, x));

                Client0.Synchronize(grainClass, x); // push all changes
                Assert.AreEqual(numupdates, Client1.GetAGlobal(grainClass, x), "grainref={0}", Client0.GetGrainRef(grainClass, x)); // push & get all
                Assert.AreEqual(numupdates, Client0.GetAGlobal(grainClass, x), "grainref={0}", Client0.GetGrainRef(grainClass, x)); // get all
            });

            Func<Task> checker4 = () => Task.Run(() =>
            {
                int x = GetRandom();
                Task.WaitAll(
                  Task.Run(() => Assert.IsTrue(Client0.GetALocal(grainClass, x) == 0)),
                  Task.Run(() => Assert.IsTrue(Client1.GetALocal(grainClass, x) == 0)),
                  Task.Run(() => Assert.IsTrue(Client0.GetAGlobal(grainClass, x) == 0)),
                  Task.Run(() => Assert.IsTrue(Client1.GetAGlobal(grainClass, x) == 0))
               );
            });

            Func<Task> checker5 = () => Task.Run(() =>
            {
                var x = GetRandom();
                Task.WaitAll(
                   Task.Run(() =>
                  {
                     Client0.AddReservationLocal(grainClass, x, 0);
                     Client0.RemoveReservationLocal(grainClass, x, 0);
                     Client0.Synchronize(grainClass, x);
                 }),
                 Task.Run(() =>
                 {
                     Client1.AddReservationLocal(grainClass, x, 1);
                     Client1.RemoveReservationLocal(grainClass, x, 1);
                     Client1.AddReservationLocal(grainClass, x, 2);
                     Client1.Synchronize(grainClass, x);
                 })
               );
                var result = Client0.GetReservationsGlobal(grainClass, x);
                Assert.AreEqual(1, result.Length);
                Assert.AreEqual(2, result[0]);
            });

            Func<int, Task> checker6 = async (int preload) => 
            {
                var x = GetRandom();
               
                if (preload % 2 == 0)
                    Client1.GetAGlobal(grainClass, x);
                if ((preload / 2) % 2 == 0)
                    Client0.GetAGlobal(grainClass, x);

                bool done = false;

                await Task.WhenAny(
                    Task.Delay(20000),
                    Task.WhenAll(
                       Task.Run(() =>
                       {
                           while (Client1.GetALocal(grainClass, x) != 1)
                             System.Threading.Thread.Sleep(100);
                           done = true;
                       }),
                       Task.Run(() =>
                       {
    
                           Client0.SetALocal(grainClass, x, 1);
                       }))
                );

                Assert.AreEqual(true, done, "checker6({0}): update did not propagate within 20 sec", preload);
            };

            // first, run short ones in sequence
            await checker1();
            await checker2();
            await checker2b();
            await checker3(4);
            await checker3(20);
            await checker4();
            await checker5();

            await checker6(0);
            await checker6(1);
            await checker6(2);
            await checker6(3);

            // then, run slightly longer tests
            if (phases != 0)
            {
                await checker3(20);
                await checker3(phases);
            }

            // finally run many test instances concurrently
            var tasks = new List<Task>();
            for (int i = 0; i < phases; i++)
            {
                tasks.Add(checker1());
                tasks.Add(checker2());
                tasks.Add(checker2b());
                tasks.Add(checker3(4));
                tasks.Add(checker4());
                tasks.Add(checker5());
                tasks.Add(checker6(0));
                tasks.Add(checker6(1));
                tasks.Add(checker6(2));
                tasks.Add(checker6(3));
            }
            await Task.WhenAll(tasks);
        }
    }
}
