using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;
using Orleans.TestingHost;
using UnitTests.GrainInterfaces;
using Orleans.Runtime;
using Tests.GeoClusterTests;
using Xunit;
using Xunit.Abstractions;

namespace Tests.GeoClusterTests
{
    public class LogViewGrainTestsFourClusters : TestingClusterHost
    {
        private const string Cluster0 = "A";
        private const string Cluster1 = "B";
        private const string Cluster2 = "C";
        private const string Cluster3 = "D";

        private static ClientWrapper Client0;
        private static ClientWrapper Client1;
        private static ClientWrapper Client2;
        private static ClientWrapper Client3;

        public LogViewGrainTestsFourClusters(ITestOutputHelper output) : base()
        {
            TimeSpan waitTimeout = TimeSpan.FromSeconds(60);

            // use a random global service id for testing purposes
            var globalserviceid = Guid.NewGuid();

            // Create two clusters, each with 2 silos. 
            NewGeoCluster(globalserviceid, Cluster0, 1, ReplicationProviderConfiguration.ConfigureLogViewProvidersForTesting);
            NewGeoCluster(globalserviceid, Cluster1, 1, ReplicationProviderConfiguration.ConfigureLogViewProvidersForTesting);
            NewGeoCluster(globalserviceid, Cluster2, 1, ReplicationProviderConfiguration.ConfigureLogViewProvidersForTesting);
            NewGeoCluster(globalserviceid, Cluster3, 1, ReplicationProviderConfiguration.ConfigureLogViewProvidersForTesting);

            WaitForLivenessToStabilizeAsync().WaitWithThrow(waitTimeout);

            // Create clients.
            Client0 = NewClient<ClientWrapper>(Cluster0, 0);
            Client1 = NewClient<ClientWrapper>(Cluster1, 0);
            Client2 = NewClient<ClientWrapper>(Cluster2, 0);
            Client3 = NewClient<ClientWrapper>(Cluster3, 0);

            Client1.InjectClusterConfiguration(Cluster0, Cluster1, Cluster2, Cluster3);
            WaitForMultiClusterGossipToStabilizeAsync(false).WaitWithThrow(waitTimeout);
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
                return GrainClient.GrainFactory.GetGrain<ISimpleLogViewGrain>(i, grainclass).ToString();
            }

            public void SetALocal(string grainclass, int i, int a)
            {
                var grainRef = GrainClient.GrainFactory.GetGrain<ISimpleLogViewGrain>(i, grainclass);
                grainRef.SetALocal(a).Wait();
            }

            public void SetAGlobal(string grainclass, int i, int a)
            {
                var grainRef = GrainClient.GrainFactory.GetGrain<ISimpleLogViewGrain>(i, grainclass);
                grainRef.SetAGlobal(a).Wait();
            }

            public Tuple<int,bool> SetAConditional(string grainclass, int i, int a)
            {
                var grainRef = GrainClient.GrainFactory.GetGrain<ISimpleLogViewGrain>(i, grainclass);
                return grainRef.SetAConditional(a).Result;
            }

            public void IncrementAGlobal(string grainclass, int i)
            {
                var grainRef = GrainClient.GrainFactory.GetGrain<ISimpleLogViewGrain>(i, grainclass);
                grainRef.IncrementAGlobal().Wait();
            }

            public void IncrementALocal(string grainclass, int i)
            {
                var grainRef = GrainClient.GrainFactory.GetGrain<ISimpleLogViewGrain>(i, grainclass);
                grainRef.IncrementALocal().Wait();
            }

            public int GetAGlobal(string grainclass, int i)
            {
                var grainRef = GrainClient.GrainFactory.GetGrain<ISimpleLogViewGrain>(i, grainclass);
                return grainRef.GetAGlobal().Result;
            }

            public int GetALocal(string grainclass, int i)
            {
                var grainRef = GrainClient.GrainFactory.GetGrain<ISimpleLogViewGrain>(i, grainclass);
                return grainRef.GetALocal().Result;
            }
            public void SetBLocal(string grainclass, int i, int a)
            {
                var grainRef = GrainClient.GrainFactory.GetGrain<ISimpleLogViewGrain>(i, grainclass);
                grainRef.SetBLocal(a).Wait();
            }

            public void SetBGlobal(string grainclass, int i, int a)
            {
                var grainRef = GrainClient.GrainFactory.GetGrain<ISimpleLogViewGrain>(i, grainclass);
                grainRef.SetBGlobal(a).Wait();
            }

            public void AddReservationLocal(string grainclass, int i, int x)
            {
                var grainRef = GrainClient.GrainFactory.GetGrain<ISimpleLogViewGrain>(i, grainclass);
                grainRef.AddReservationLocal(x).Wait();
            }

            public void RemoveReservationLocal(string grainclass, int i, int x)
            {
                var grainRef = GrainClient.GrainFactory.GetGrain<ISimpleLogViewGrain>(i, grainclass);
                grainRef.RemoveReservationLocal(x).Wait();
            }

            public int[] GetReservationsGlobal(string grainclass, int i)
            {
                var grainRef = GrainClient.GrainFactory.GetGrain<ISimpleLogViewGrain>(i, grainclass);
                return grainRef.GetReservationsGlobal().Result;
            }

            public void Synchronize(string grainclass, int i)
            {
                var grainRef = GrainClient.GrainFactory.GetGrain<ISimpleLogViewGrain>(i, grainclass);
                grainRef.SynchronizeGlobalState().Wait();
            }

            public void InjectClusterConfiguration(params string[] clusters)
            {
                systemManagement.InjectMultiClusterConfiguration(clusters).Wait();
            }
            IManagementGrain systemManagement;

            public long GetConfirmedVersion(string grainclass, int i)
            {
                var grainRef = GrainClient.GrainFactory.GetGrain<ISimpleLogViewGrain>(i, grainclass);
                return grainRef.GetConfirmedVersion().Result;
            }

        }

        #endregion

  
        [Fact, TestCategory("GeoCluster")]
        public async Task TestBattery_SharedStorageProvider()
        {
            await DoReplicationTests("UnitTests.Grains.SimpleLogViewGrainSharedStorage");
        }

        [Fact, TestCategory("GeoCluster")]
        public async Task TestBattery_CustomStorageProvider()
        {
            await DoReplicationTests("UnitTests.Grains.SimpleLogViewGrainCustomStorage");
        }


        private async Task DoReplicationTests(string grainClass, int phases = 100)
        {
            await AllChecks(grainClass, phases);
        }

     

        private async Task AllChecks(string grainClass, int phases)
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
                var grainidentity = string.Format("grainref={0}", Client0.GetGrainRef(grainClass, x));
                // force creation of replicas
                AssertEqual(0, Client0.GetALocal(grainClass, x), grainidentity);
                AssertEqual(0, Client1.GetALocal(grainClass, x), grainidentity);
                AssertEqual(0, Client2.GetALocal(grainClass, x), grainidentity);
                AssertEqual(0, Client3.GetALocal(grainClass, x), grainidentity);
                // write global on client 0
                Client0.SetAGlobal(grainClass, x, 333);
                // read global on client 1
                int r1 = Client1.GetAGlobal(grainClass, x);
                AssertEqual(333, r1, grainidentity);
                // read global on client 2
                int r2 = Client2.GetAGlobal(grainClass, x);
                AssertEqual(333, r2, grainidentity);
                // read global on client 3
                int r3 = Client3.GetAGlobal(grainClass, x);
                AssertEqual(333, r3, grainidentity);
                // check local stability
                AssertEqual(333, Client0.GetALocal(grainClass, x), grainidentity);
                AssertEqual(333, Client1.GetALocal(grainClass, x), grainidentity);
                AssertEqual(333, Client2.GetALocal(grainClass, x), grainidentity);
                AssertEqual(333, Client3.GetALocal(grainClass, x), grainidentity);
                // check versions
                AssertEqual(1, Client0.GetConfirmedVersion(grainClass, x), grainidentity);
                AssertEqual(1, Client1.GetConfirmedVersion(grainClass, x), grainidentity);
                AssertEqual(1, Client2.GetConfirmedVersion(grainClass, x), grainidentity);
                AssertEqual(1, Client3.GetConfirmedVersion(grainClass, x), grainidentity);
            });

            Func<Task> checker2 = () => Task.Run(() =>
            {
                int x = GetRandom();
                var grainidentity = string.Format("grainref={0}", Client0.GetGrainRef(grainClass, x));
                // increment on replica 1
                Client1.IncrementAGlobal(grainClass, x);
                // expect on replica 0,2,3
                int r1 = Client0.GetAGlobal(grainClass, x);
                AssertEqual(1, r1, grainidentity);
                int r2 = Client2.GetAGlobal(grainClass, x);
                AssertEqual(1, r2, grainidentity);
                int r3 = Client3.GetAGlobal(grainClass, x);
                AssertEqual(1, r3, grainidentity);
                // check versions
                AssertEqual(1, Client0.GetConfirmedVersion(grainClass, x), grainidentity);
                AssertEqual(1, Client1.GetConfirmedVersion(grainClass, x), grainidentity);
                AssertEqual(1, Client2.GetConfirmedVersion(grainClass, x), grainidentity);
                AssertEqual(1, Client3.GetConfirmedVersion(grainClass, x), grainidentity);
            });

            Func<Task> checker2b = () => Task.Run(() =>
            {
                int x = GetRandom();
                var grainidentity = string.Format("grainref={0}", Client0.GetGrainRef(grainClass, x));
                // force creation on replica 0
                AssertEqual(0, Client0.GetAGlobal(grainClass, x), grainidentity);
                // increment on replica 1
                Client1.IncrementAGlobal(grainClass, x);
                // expect on replica 0,2,3
                int r1 = Client0.GetAGlobal(grainClass, x);
                AssertEqual(1, r1, grainidentity);
                int r2 = Client2.GetAGlobal(grainClass, x);
                AssertEqual(1, r2, grainidentity);
                int r3 = Client3.GetAGlobal(grainClass, x);
                AssertEqual(1, r3, grainidentity);
                // check versions
                AssertEqual(1, Client0.GetConfirmedVersion(grainClass, x), grainidentity);
                AssertEqual(1, Client1.GetConfirmedVersion(grainClass, x), grainidentity);
                AssertEqual(1, Client2.GetConfirmedVersion(grainClass, x), grainidentity);
                AssertEqual(1, Client3.GetConfirmedVersion(grainClass, x), grainidentity);
            });

            Func<int,Task> checker3 = (int numupdates) => Task.Run(() =>
            {
                int x = GetRandom();
                var grainidentity = string.Format("grainref={0}", Client0.GetGrainRef(grainClass, x));

                var clients = new ClientWrapper[] { Client0, Client1, Client2, Client3 };
                // concurrently chaotically increment (numupdates) times
                Parallel.For(0, numupdates, i => clients[i % 4].IncrementALocal(grainClass, x));

                Client0.Synchronize(grainClass, x); // push all changes
                Client2.Synchronize(grainClass, x); // push all changes
                Client3.Synchronize(grainClass, x); // push all changes
                AssertEqual(numupdates, Client1.GetAGlobal(grainClass, x), grainidentity); // push & get all
                AssertEqual(numupdates, Client0.GetAGlobal(grainClass, x), grainidentity); // get all
                AssertEqual(numupdates, Client2.GetAGlobal(grainClass, x), grainidentity); // get all
                AssertEqual(numupdates, Client3.GetAGlobal(grainClass, x), grainidentity); // get all

                // check versions
                AssertEqual(numupdates, Client0.GetConfirmedVersion(grainClass, x), grainidentity);
                AssertEqual(numupdates, Client1.GetConfirmedVersion(grainClass, x), grainidentity);
                AssertEqual(numupdates, Client2.GetConfirmedVersion(grainClass, x), grainidentity);
                AssertEqual(numupdates, Client3.GetConfirmedVersion(grainClass, x), grainidentity);
            });

            Func<Task> checker4 = () => Task.Run(() =>
            {
                int x = GetRandom();
                Task.WaitAll(
                  Task.Run(() => Assert.True(Client0.GetALocal(grainClass, x) == 0)),
                  Task.Run(() => Assert.True(Client1.GetALocal(grainClass, x) == 0)),
                  Task.Run(() => Assert.True(Client2.GetALocal(grainClass, x) == 0)),
                  Task.Run(() => Assert.True(Client3.GetALocal(grainClass, x) == 0)),
                  Task.Run(() => Assert.True(Client0.GetAGlobal(grainClass, x) == 0)),
                  Task.Run(() => Assert.True(Client1.GetAGlobal(grainClass, x) == 0)),
                  Task.Run(() => Assert.True(Client2.GetAGlobal(grainClass, x) == 0)),
                  Task.Run(() => Assert.True(Client3.GetAGlobal(grainClass, x) == 0))
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
                Assert.Equal(1, result.Length);
                Assert.Equal(2, result[0]);
            });

            Func<int, Task> checker6 = async (int preload) => 
            {
                var x = GetRandom();
               
                if (preload % 2 == 0)
                    Client1.GetAGlobal(grainClass, x);
                if ((preload / 2) % 2 == 0)
                    Client0.GetAGlobal(grainClass, x);

                bool done1 = false;
                bool done2 = false;
                bool done3 = false;

                await Task.WhenAny(
                    Task.Delay(20000),
                    Task.WhenAll(
                       Task.Run(() =>
                       {
                           while (Client1.GetALocal(grainClass, x) != 1)
                             System.Threading.Thread.Sleep(100);
                           done1 = true;
                       }),
                       Task.Run(() =>
                       {
                           while (Client2.GetALocal(grainClass, x) != 1)
                               System.Threading.Thread.Sleep(100);
                           done2 = true;
                       }),
                       Task.Run(() =>
                       {
                           while (Client3.GetALocal(grainClass, x) != 1)
                               System.Threading.Thread.Sleep(100);
                           done3 = true;
                       }),
                       Task.Run(() =>
                       {    
                           Client0.SetALocal(grainClass, x, 1);
                       }))
                );

                AssertEqual(true, done1 && done2 && done3, string.Format("checker6({0}): update did not propagate within 20 sec", preload));
            };

            Func<int,Task> checker7 = (int variation) => Task.Run(() =>
            {
                int x = GetRandom();

                if (variation % 2 == 0)
                    Client1.GetAGlobal(grainClass, x);
                if ((variation / 2) % 2 == 0)
                    Client0.GetAGlobal(grainClass, x);

                var grainidentity = string.Format("grainref={0}", Client0.GetGrainRef(grainClass, x));

                // write conditional on client 0, should always succeed
                {
                    var result = Client0.SetAConditional(grainClass, x, 333);
                    AssertEqual(0, result.Item1, grainidentity);
                    AssertEqual(true, result.Item2, grainidentity);
                    AssertEqual(1, Client0.GetConfirmedVersion(grainClass, x), grainidentity);
                }

                if ((variation / 4) % 2 == 1)
                    System.Threading.Thread.Sleep(100);

                // write conditional on client1, may or may not succeed based on timing
                {
                    var result = Client1.SetAConditional(grainClass, x, 444);
                    if (result.Item1 == 0) // was stale, thus failed
                    {
                        AssertEqual(false, result.Item2, grainidentity);
                        // must have updated as a result
                        AssertEqual(1, Client1.GetConfirmedVersion(grainClass, x), grainidentity);
                        // check stability
                        AssertEqual(333, Client0.GetALocal(grainClass, x), grainidentity);
                        AssertEqual(333, Client1.GetALocal(grainClass, x), grainidentity);
                        AssertEqual(333, Client0.GetAGlobal(grainClass, x), grainidentity);
                        AssertEqual(333, Client1.GetAGlobal(grainClass, x), grainidentity);
                    }
                    else // was up-to-date, thus succeeded
                    {
                        AssertEqual(true, result.Item2, grainidentity);
                        AssertEqual(1, result.Item1, grainidentity);
                        // version is now 2
                        AssertEqual(2, Client1.GetConfirmedVersion(grainClass, x), grainidentity);
                        // check stability
                        AssertEqual(444, Client1.GetALocal(grainClass, x), grainidentity);
                        AssertEqual(444, Client0.GetAGlobal(grainClass, x), grainidentity);
                        AssertEqual(444, Client1.GetAGlobal(grainClass, x), grainidentity);
                    }
                }
            });

 

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

            await checker7(0);
            await checker7(4);
            await checker7(7);

            // run tests under blocked notification to force race one way
            BlockNotificationMessages(Cluster0);
            await checker7(0);
            await checker7(1);
            await checker7(2);
            await checker7(3);
            UnblockNotificationMessages(Cluster0);


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
                tasks.Add(checker7(0));
                tasks.Add(checker7(1));
                tasks.Add(checker7(2));
                tasks.Add(checker7(3));
                tasks.Add(checker7(4));
                tasks.Add(checker7(5));
                tasks.Add(checker7(6));
                tasks.Add(checker7(7));
            }
            await Task.WhenAll(tasks);
        }

    }
}
