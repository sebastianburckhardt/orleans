﻿using Orleans;
using Orleans.Runtime;
using Orleans.Runtime.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TestGrainInterfaces;
using Tests.GeoClusterTests;
using Xunit;
using Xunit.Abstractions;

namespace UnitTests.GeoClusterTests
{
    public class MultiClusterRegistrationTests : TestingClusterHost
    {
        private string[] ClusterNames;
        private ClientWrapper[][] Clients;

        private IEnumerable<KeyValuePair<string, ClientWrapper>> EnumerateClients()
        {
            for (int i = 0; i < ClusterNames.Length; i++)
                foreach (var c in Clients[i])
                    yield return new KeyValuePair<string, ClientWrapper>(ClusterNames[i], c);
        }

        public MultiClusterRegistrationTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact, TestCategory("Functional"), TestCategory("GeoCluster")]
        public async Task TwoClusterBattery()
        {

            await RunWithTimeout("Start Clusters and Clients", 180 * 1000, () => StartClustersAndClients(2,2));

            var testtasks = new List<Task>();

            testtasks.Add(RunWithTimeout("Deact", 20000, Deact));
            testtasks.Add(RunWithTimeout("SequentialCalls", 10000, SequentialCalls));
            testtasks.Add(RunWithTimeout("ParallelCalls", 10000, ParallelCalls));
            testtasks.Add(RunWithTimeout("ManyParallelCalls", 10000, ManyParallelCalls));

            foreach (var t in testtasks)
                await t;
        }

        [Fact, TestCategory("GeoCluster")]
        public async Task ThreeClusterBattery()
        {

            await RunWithTimeout("Start Clusters and Clients", 180 * 1000, () => StartClustersAndClients(2,2,2));

            var testtasks = new List<Task>();

            testtasks.Add(RunWithTimeout("Deact", 20000, Deact));
            testtasks.Add(RunWithTimeout("SequentialCalls", 10000, SequentialCalls));
            testtasks.Add(RunWithTimeout("ParallelCalls", 10000, ParallelCalls));
            testtasks.Add(RunWithTimeout("ManyParallelCalls", 10000, ManyParallelCalls));

            foreach (var t in testtasks)
                await t;
        }

        [Fact, TestCategory("GeoCluster")]
        public async Task FourClusterBattery()
        {

            await RunWithTimeout("Start Clusters and Clients", 180 * 1000, () => StartClustersAndClients(2, 2, 1, 1));

            var testtasks = new List<Task>();

            for (int i = 0; i < 20; i++)
            {
                testtasks.Add(RunWithTimeout("Deact", 20000, Deact));
                testtasks.Add(RunWithTimeout("SequentialCalls", 10000, SequentialCalls));
                testtasks.Add(RunWithTimeout("ParallelCalls", 10000, ParallelCalls));
                testtasks.Add(RunWithTimeout("ManyParallelCalls", 10000, ManyParallelCalls));
            }

            foreach (var t in testtasks)
                await t;
        }

        public Task StartClustersAndClients(params int[] silos) 
        {
            return StartClustersAndClients(null, silos);
        }

        public Task StartClustersAndClients(Action<ClusterConfiguration> customizer, params int[] silos)
        {
            WriteLog("Creating clusters and clients...");
            var stopwatch = new System.Diagnostics.Stopwatch();
            stopwatch.Start();

            // use a random global service id for testing purposes
            var globalserviceid = Guid.NewGuid();
            random = new Random();

            System.Threading.ThreadPool.SetMaxThreads(8, 8);

            Action<ClusterConfiguration> addtracing = (ClusterConfiguration c) =>
            {
                // logging  
                foreach (var o in c.Overrides)
                {
                    o.Value.TraceLevelOverrides.Add(new Tuple<string, Severity>("Runtime.Catalog", Severity.Verbose));
                    o.Value.TraceLevelOverrides.Add(new Tuple<string, Severity>("Runtime.Dispatcher", Severity.Verbose2));
                    o.Value.TraceLevelOverrides.Add(new Tuple<string, Severity>("Orleans.GrainDirectory.LocalGrainDirectory", Severity.Verbose2));
                }

                customizer?.Invoke(c);
            };

            // Create clusters and clients
            ClusterNames = new string[silos.Length];
            Clients = new ClientWrapper[silos.Length][];
            for (int i = 0; i < silos.Length; i++)
            {
                var numsilos = silos[i];
                var clustername = ClusterNames[i] = ((char)('A' + i)).ToString();
                var c = Clients[i] = new ClientWrapper[numsilos];
                NewGeoCluster(globalserviceid, clustername, silos[i], addtracing);
                // create one client per silo
                Parallel.For(0, numsilos, paralleloptions, (j) => c[j] = NewClient<ClientWrapper>(clustername, j));
            }

            WriteLog("Clusters and clients are ready (elapsed = {0})", stopwatch.Elapsed);

            // wait for configuration to stabilize
            WaitForLivenessToStabilizeAsync().WaitWithThrow(TimeSpan.FromMinutes(1));
    
            Clients[0][0].InjectClusterConfiguration(ClusterNames);
            WaitForMultiClusterGossipToStabilizeAsync(false).WaitWithThrow(TimeSpan.FromMinutes(System.Diagnostics.Debugger.IsAttached ? 60 : 1));

            stopwatch.Stop();
            WriteLog("Multicluster is ready (elapsed = {0}).", stopwatch.Elapsed);

            return TaskDone.Done;
        }

  
        Random random;

        #region client wrappers
        public class ClientWrapper : ClientWrapperBase
        {
            public ClientWrapper(string name, int gatewayport) : base(name, gatewayport)
            {
                systemManagement = GrainClient.GrainFactory.GetGrain<IManagementGrain>(0);
            }
            public int CallGrain(int i)
            {
                var grainRef = GrainClient.GrainFactory.GetGrain<IClusterTestGrain>(i);
                GrainClient.Logger.Info("Call Grain {0}", grainRef);
                Task<int> toWait = grainRef.SayHelloAsync();
                toWait.Wait();
                return toWait.Result;
            }
            public string GetRuntimeId(int i)
            {
                var grainRef = GrainClient.GrainFactory.GetGrain<IClusterTestGrain>(i);
                GrainClient.Logger.Info("GetRuntimeId {0}", grainRef);
                Task<string> toWait = grainRef.GetRuntimeId();
                toWait.Wait();
                return toWait.Result;
            }
            public void Deactivate(int i)
            {
                var grainRef = GrainClient.GrainFactory.GetGrain<IClusterTestGrain>(i);
                GrainClient.Logger.Info("Deactivate {0}", grainRef);
                Task toWait = grainRef.Deactivate();
                toWait.Wait();
            }
            public void InjectClusterConfiguration(params string[] clusters)
            {
                systemManagement.InjectMultiClusterConfiguration(clusters).Wait();
            }
            IManagementGrain systemManagement;
            public string GetGrainRef(int i)
            {
                return GrainClient.GrainFactory.GetGrain<IClusterTestGrain>(i).ToString();
            }
        }
        #endregion



        private int Next()
        {
            lock (random)
                return random.Next();
        }

      
        public async Task SequentialCalls()
        {
            await Task.Yield();

            var x = Next();
            var gref = Clients[0][0].GetGrainRef(x);
            var list = EnumerateClients().ToList();

            // one call to each silo, in parallel
            foreach (var c in list) c.Value.CallGrain(x);
            
            // total number of increments should match
            AssertEqual(list.Count() + 1, Clients[0][0].CallGrain(x), gref);
        }

        public async Task ParallelCalls()
        {
            await Task.Yield();

            var x = Next();
            var gref = Clients[0][0].GetGrainRef(x);
            var list = EnumerateClients().ToList();

            // one call to each silo, in parallel
            Parallel.ForEach(list, paralleloptions, k => k.Value.CallGrain(x));

            // total number of increments should match
            AssertEqual(list.Count + 1, Clients[0][0].CallGrain(x), gref);
        }

        public async Task ManyParallelCalls()
        {
            await Task.Yield();

            var x = Next();
            var gref = Clients[0][0].GetGrainRef(x);

            // pick just one client per cluster, use it multiple times
            var clients = Clients.Select(a => a[0]).ToList();

            // concurrently increment (numupdates) times, distributed over the clients
            Parallel.For(0, 20, paralleloptions, i => clients[i % clients.Count].CallGrain(x));

            // total number of increments should match
            AssertEqual(21, Clients[0][0].CallGrain(x), gref);
        }

        public async Task Deact()
        {            
            var x = Next();
            var gref = Clients[0][0].GetGrainRef(x);

            // put into cluster A
            var id = Clients[0][0].GetRuntimeId(x);

            WriteLog("Grain {0} at {1}", gref, id);
            Assert.True(Clusters[ClusterNames[0]].Silos.Any(silo => silo.Silo.SiloAddress.ToString() == id));

            // ensure presence in all caches
            var list = EnumerateClients().ToList();
            Parallel.ForEach(list, paralleloptions, k => k.Value.CallGrain(x));
            Parallel.ForEach(list, paralleloptions, k => k.Value.CallGrain(x));

            AssertEqual(2* list.Count() + 1, Clients[0][0].CallGrain(x), gref);

            WriteLog("Grain {0} deactivating.", gref);

            //deactivate
            Clients[0][0].Deactivate(x);

            // wait for deactivation to complete
            await Task.Delay(5000);

            WriteLog("Grain {0} reactivating.", gref);

            // activate anew in cluster B
            var val = Clients[1][0].CallGrain(x);
            AssertEqual(1, val, gref);
            var newid = Clients[1][0].GetRuntimeId(x);
            WriteLog("{2} sees Grain {0} at {1}", gref, newid, ClusterNames[1]);
            Assert.True(Clusters[ClusterNames[1]].Silos.Any(silo => silo.Silo.SiloAddress.ToString() == newid));

            WriteLog("Grain {0} Check that other clusters find new activation.", gref);
            for (int i = 2; i < Clusters.Count; i++)
            {
                var idd = Clients[i][0].GetRuntimeId(x);
                WriteLog("{2} sees Grain {0} at {1}", gref, idd, ClusterNames[i]);
                AssertEqual(newid, idd, gref);
            }
        }



        [Fact, TestCategory("GeoCluster")]
        public async Task BlockedDeact()
        {
            await RunWithTimeout("Start Clusters and Clients", 180 * 1000, () =>
            {
                Action<ClusterConfiguration> c =
                  (cc) => { cc.Globals.DirectoryLazyDeregistrationDelay = TimeSpan.FromSeconds(5); };
                return StartClustersAndClients(c, 1, 1);
            });

            await RunWithTimeout("BlockedDeact", 10 * 1000, async () =>
            {
                var x = Next();
                var gref = Clients[0][0].GetGrainRef(x);

                // put into cluster A and access from cluster B
                var id = Clients[0][0].GetRuntimeId(x);

                WriteLog("Grain {0} at {1}", gref, id);
                Assert.True(Clusters[ClusterNames[0]].Silos.Any(silo => silo.Silo.SiloAddress.ToString() == id));

                var id2 = Clients[1][0].GetRuntimeId(x);
                AssertEqual(id2, id, gref);

                // deactivate grain in cluster A, but block deactivation message to cluster B

                WriteLog("Grain {0} deactivating.", gref);
                BlockAllClusterCommunication(ClusterNames[0], ClusterNames[1]);
                Clients[0][0].Deactivate(x);
                await Task.Delay(5000);
                UnblockAllClusterCommunication(ClusterNames[0]);

                // reactivate in cluster B. This should cause unregistration to be sent
                WriteLog("Grain {0} reactivating.", gref);

                // activate anew in cluster B
                var val = Clients[1][0].CallGrain(x);
                AssertEqual(1, val, gref);
                var newid = Clients[1][0].GetRuntimeId(x);
                WriteLog("{2} sees Grain {0} at {1}", gref, newid, ClusterNames[1]);
                Assert.True(Clusters[ClusterNames[1]].Silos[0].Silo.SiloAddress.ToString() == newid);
            });
        }
    }
}
