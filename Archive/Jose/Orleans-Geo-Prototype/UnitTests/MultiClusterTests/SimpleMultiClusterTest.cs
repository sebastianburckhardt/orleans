using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using Orleans.Runtime;
using TestGrainInterface;

namespace UnitTests.MultiClusterTests
{
    // We need use ClientWraper to load a client object in a new app domain. This allows us to create multiple clients that are connected to 
    // different silos.
    public class ClientWrapper : MarshalByRefObject
    {
        public ClientWrapper(string configFile)
        {
            OrleansClient.Initialize(configFile);
        }

        public int CallGrain(int grainId)
        {
            ITestGrainInterface grainRef = TestGrainInterfaceFactory.GetGrain(grainId);
            Task<int> toWait = grainRef.SayHelloAsync();
            toWait.Wait();
            return toWait.Result;
        }
    }

    [TestClass]
    [DeploymentItem("TestGrain.dll")]
    [DeploymentItem("Config_Cluster0.xml")]
    [DeploymentItem("Config_Cluster1.xml")]
    [DeploymentItem("Config_Client0.xml")]
    [DeploymentItem("Config_Client1.xml")]
    public class SimpleMultiClusterTest : UnitTestBase
    {
        private static readonly string ConfigPrefix = 
            @"E:\Depot\CCF\Orleans\Code\Partners\Jose\OrleansV4\UnitTests\"; 
            //Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
        private readonly List<SiloHandle> activeSilos;
        private readonly List<AppDomain> activeClients;
        private volatile int threadsDone;

        public SimpleMultiClusterTest()
            : base(new Options
            {
                SiloConfigFile = new FileInfo(Path.Combine(ConfigPrefix, "Config_Cluster0.xml")),
                BasePort = -1,
                StartClient = false,
                OverrideConfig = false,
                StartPrimary = false,
                StartSecondary = false,
            })
        {
            activeSilos = new List<SiloHandle>();
            activeClients = new List<AppDomain>();
        }

        // This is a helper function which is used to run the race condition tests. This function waits for all client threads trying to create the
        // same activation to finish. The last client thread to finish will wakeup the coordinator thread. 
        private void WaitForCoordinator(int numThreads, object coordWakeup, object toWait)
        {
            Monitor.Enter(coordWakeup);
            Monitor.Enter(toWait);

            threadsDone -= 1;
            if (threadsDone == 0)
            {
                Monitor.Pulse(coordWakeup);
            }
            
            Monitor.Exit(coordWakeup);
            Monitor.Wait(toWait);
            Monitor.Exit(toWait);
        }

        // This is a helper function which is used to signal the worker client threads to run another iteration of our concurrent experiment.
        private void WaitForWorkers(int numThreads, object coordWakeup, object toWait)
        {
            Monitor.Enter(coordWakeup);

            while (threadsDone != 0)
            {
                Monitor.Wait(coordWakeup);
            }

            threadsDone = numThreads;
            Monitor.Exit(coordWakeup);

            Monitor.Enter(toWait);
            Monitor.PulseAll(toWait);
            Monitor.Exit(toWait);
        }

        // ClientThreadArgs is a set of arguments which is used by a client thread which is concurrently running with other client threads. We
        // use client threads in order to simulate race conditions.
        private class ClientThreadArgs
        {
            public ClientWrapper client;
            public IEnumerable<Tuple<int, int>> args;
            public int resultIndex;
            public int numThreads;
            public object coordWakeup;
            public object toWait;
            public List<Tuple<int, int>>[] results;
        }

        // Each client thread which is concurrently trying to create a sequence of grains with other clients runs this function.
        private void ThreadFunc(object obj)
        {
            var threadArg = (ClientThreadArgs) obj;
            var resultList = new List<Tuple<int, int>>();

            // Go through the sequence of arguments one by one.
            foreach (var arg in threadArg.args)
            {
                // Call the appropriate grain.
                var grainId = arg.Item2;
                int ret = threadArg.client.CallGrain(grainId);

                // Keep the result in resultList.
                resultList.Add(Tuple.Create(grainId, ret));

                // Finally, wait for the coordinator to kick-off another round of the test.
                WaitForCoordinator(threadArg.numThreads, threadArg.coordWakeup, threadArg.toWait);
            }

            // Track the results for validation.
            lock (threadArg.results)
            {
                threadArg.results[threadArg.resultIndex] = resultList;
            }
        }

        // This function takes a configfile corresponding to a particular cluster, and the number of silos to create in the cluster. It returns
        // a list of SiloHandles coresponding to the created silos.
        private List<SiloHandle> CreateCluster(string configFile, int numSilos)
        {
            var handleList = new List<SiloHandle>();
            var primaryOption = new Options
            {
                SiloConfigFile = new FileInfo(configFile),
                StartClient = false,
                BasePort = -1,
                OverrideConfig = false,
                DomainName = "Primary",
            };
            handleList.Add(StartAdditionalOrleans(primaryOption, Silo.SiloType.Primary));

            for (int i = 1; i < numSilos; ++i)
            {
                var options = new Options
                {
                    SiloConfigFile = new FileInfo(configFile),
                    StartClient = false,
                    BasePort = -1,
                    OverrideConfig = false,
                    DomainName = "Secondary_" + i,
                };
                handleList.Add(StartAdditionalOrleans(options, Silo.SiloType.Secondary));
            }
            activeSilos.AddRange(handleList);
            return handleList;
        }

        // This function is used to create an individual silo.
        private SiloHandle CreateSilo(string configFile, int siloIndex)
        {
            var options = new Options
            {
                SiloConfigFile = new FileInfo(configFile),
                StartClient = false,
                BasePort = -1,
                OverrideConfig = false,
                DomainName = "Secondary_" + siloIndex,
            };

            var ret = StartAdditionalOrleans(options, Silo.SiloType.Secondary);
            activeSilos.Add(ret);
            return ret;
        }

        // Create a client, loaded in a new app domain.
        private ClientWrapper CreateClient(string configFile)
        {
            var clientArgs = new object[] { configFile };
            var setup = new AppDomainSetup { ApplicationBase = Environment.CurrentDirectory };
            var clientDomain = AppDomain.CreateDomain("client", null, setup);
            activeClients.Add(clientDomain);
            return (ClientWrapper)clientDomain.CreateInstanceFromAndUnwrap(
                    Assembly.GetExecutingAssembly().Location, typeof(ClientWrapper).FullName, false,
                    BindingFlags.Default, null, clientArgs, CultureInfo.CurrentCulture,
                    new object[] { });
        }

        // Kill all clients and silos.
        [TestCleanup]
        public void TestCleanup()
        {
            foreach (var handle in activeSilos)
            {
                KillRuntime(handle);
            }
            activeSilos.Clear();

            foreach (var client in activeClients)
            {
                try
                {
                    AppDomain.Unload(client);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e); 
                    throw;
                }   
            }
            activeClients.Clear();
        }

        #region Creation

        // This function is used to test the activation creation protocol. It runs with two clusters, with 1 silo each. 
        [TestMethod, TestCategory("GeoCluster")]
        public void TestSingleSingleClusterCreation()
        {
            // Create two clusters, each with a single silo.
            var configCluster0 = Path.Combine(ConfigPrefix, "Config_Cluster0.xml");
            var configCluster1 = Path.Combine(ConfigPrefix, "Config_Cluster1.xml");
            var cluster0 = CreateCluster(configCluster0, 1);
            var cluster1 = CreateCluster(configCluster1, 1);

            WaitForLivenessToStabilize();

            int numGrains = 2000;

            // Create clients.
            var client0 = CreateClient(Path.Combine(ConfigPrefix, "Config_Client0.xml"));
            var client1 = CreateClient(Path.Combine(ConfigPrefix, "Config_Client1.xml"));

            // Create grains on both clusters. Alternating between the two.
            for (int i = 0; i < numGrains; ++i)
            {
                int val = -1;
                
                // Make calls to even numbered grains using client0. Client0 is connected to cluster 0.
                if (i%2 == 0)
                {
                    val = client0.CallGrain(i);
                }
                else
                {
                    // Make calls to odd numbered grains using client1. Client1 is connected to cluster1.
                    val = client1.CallGrain(i);
                }

                Assert.AreEqual(1, val);
            }
            
            // Grains are activated in a cluster immediately, while the creation algorithm runs in the background. Wait for all in-flight instances
            // of activation creation to complete by sleeping. Since we expect that all in-flight instances of the algorithm will complete before
            // the thread wakes up, we expect that all activations are in state OWNED. Both clients create non-conflicting activations, therefore,
            // we expect that all created activations are in state OWNED.
            Thread.Sleep(60000);

            // Ensure that we have the correct number of OWNED grains. We have created 2000 grains, 1000 grains are created on cluster 0,
            // and 1000 grains are created on cluster1.
            // Get the grain directory associated with each of the clusters.
            var dir1 = cluster0[0].Silo.TestHookup.GetDirectory();
            var dir2 = cluster1[0].Silo.TestHookup.GetDirectory();
            int ownCount = 0;
            foreach (var keyvalue in dir1)
            {
                var activation = keyvalue.Value.Instances.First();
                var actId = activation.Key;
                var actInfo = activation.Value;
                ownCount += actInfo.Status == ActivationStatus.OWNED ? 1 : 0;
            }
            foreach (var keyvalue in dir2)
            {
                var activation = keyvalue.Value.Instances.First();
                var actId = activation.Key;
                var actInfo = activation.Value;
                ownCount += actInfo.Status == ActivationStatus.OWNED ? 1 : 0;
            }

            // Assert that the number of OWNED grains is equal to the number of grains that we invoked.
            Assert.AreEqual(numGrains, ownCount);
        }

        // This function is used to test the activation creation algorithm when two clusters create non-conflicting activations.
        [TestMethod, TestCategory("GeoCluster")]
        public void TestMultiMultiClusterCreation()
        {
            // Create two clusters, each with 5 silos.
            var cluster0 = CreateCluster(Path.Combine(ConfigPrefix, "Config_Cluster0.xml"), 5);
            var cluster1 = CreateCluster(Path.Combine(ConfigPrefix, "Config_Cluster1.xml"), 5);

            WaitForLivenessToStabilize();

            // Create 4 clients, 2 clients will connect to two gateway silos on cluster 0, 2 clients connect to cluster1.
            var client0 = CreateClient(Path.Combine(ConfigPrefix, "Config_Client0.xml"));
            var client1 = CreateClient(Path.Combine(ConfigPrefix, "Config_Client2.xml"));
            var client2 = CreateClient(Path.Combine(ConfigPrefix, "Config_Client1.xml"));
            var client3 = CreateClient(Path.Combine(ConfigPrefix, "Config_Client3.xml"));

            // Create 2000 grains, 1000 grains in each cluster. We alternate the calls among two clients connected to the same cluster. This allows
            // us to ensure that two clients within the same cluster never end up with two separate activations of the same grain.
            int numGrains = 2000;
            for (int i = 0; i < numGrains; ++i)
            {
                int first, second;
                if (i%4 == 0)
                {
                    first = client0.CallGrain(i);
                    second = client1.CallGrain(i);
                }
                else if (i%4 == 1)
                {
                    first = client1.CallGrain(i);
                    second = client0.CallGrain(i);
                }
                else if (i%4 == 2)
                {
                    first = client2.CallGrain(i);
                    second = client3.CallGrain(i);
                }
                else
                {
                    first = client3.CallGrain(i);
                    second = client2.CallGrain(i);
                }

                // Make sure that the values we see are 1 and 2. This means that two clients connected to silos in the same cluster both called the
                // same activation of the grain.
                Assert.AreEqual(1, first);
                Assert.AreEqual(2, second);
            }

            // Wait for 60 seconds in order to allow the asynchronously invoked activation creation algorithm to complete. 
            Thread.Sleep(60000);

            // Count the total number of OWNED activations in cluster0.
            var countsCluster0 = cluster0.Select(silo =>
                    silo.Silo.TestHookup.GetDirectory()
                        .Where(g => g.Value.Instances.First().Value.Status == ActivationStatus.OWNED)
                        .Count()).Sum();

            // Count the total number of OWNED activations in cluster1. 
            var countsCluster1 = cluster1.Select(silo =>
                    silo.Silo.TestHookup.GetDirectory()
                        .Where(g => g.Value.Instances.First().Value.Status == ActivationStatus.OWNED)
                        .Count()).Sum();

            // Ensure that we're running this test with an even number of grains :).
            Assert.AreEqual(0, numGrains%2);

            // Check that total number of OWNED grains that we counted is equal to the number of grains that were activated.
            Assert.AreEqual(numGrains, countsCluster0+countsCluster1);

            // The grains are divided evenly among clusters0 and 1. Verify this.
            Assert.AreEqual(numGrains/2, countsCluster0);
            Assert.AreEqual(numGrains/2, countsCluster1);
        }

        #endregion Creation

        #region Race Conditions

        // This function takes two arguments, a list of client configurations, and an integer. The list of client configurations is used to
        // create multiple clients that concurrently call the grains in range [0, numGrains). We run the experiment in a series of barriers.
        // The clients all invoke grain "g", in parallel, and then wait on a signal by the main thread (this function). The main thread, then 
        // wakes up the clients, after which they invoke "g+1", and so on.
        private List<Tuple<int, int>>[] DoConcurrentExperiment(List<string> configList, int numGrains)
        {
            // We use two objects to coordinate client threads and the main thread. coordWakeup is an object that is used to signal the coordinator
            // thread. toWait is used to signla client threads.
            var coordWakeup = new object();
            var toWait = new object();

            // We keep a list of client threads.
            var clientThreads = new List<Tuple<Thread, ClientThreadArgs>>();
            var rand = new Random();
            var results = new List<Tuple<int, int>>[configList.Count];
            threadsDone = results.Count();

            int index = 0;

            // Go through each of the config files, and create a client corresponding to each configuration file.
            foreach (var configFile in configList)
            {
                // A client thread takes a list of tupes<int, int> as argument. The list is an ordered sequence of grains to invoke. tuple.item1
                // is the grainId. tuple.item2 is never used (this should probably be cleaned up, but I don't want to break anything :).
                var args = new List<Tuple<int, int>>();
                for (int j = 0; j < numGrains; ++j)
                {
                    var waitTime = rand.Next(16, 100);
                    args.Add(Tuple.Create(waitTime, j));
                }
                
                // Given a config file, create client starts a client in a new appdomain. We also create a thread on which the client will run.
                // The thread takes a "ClientThreadArgs" as argument.
                var client = CreateClient(configFile);
                var thread = new Thread(new ParameterizedThreadStart(ThreadFunc));
                var threadFuncArgs = new ClientThreadArgs
                {
                    client = client,
                    args = args,
                    resultIndex = index,
                    numThreads = configList.Count,
                    coordWakeup = coordWakeup,
                    toWait = toWait,
                    results = results,
                };
                clientThreads.Add(Tuple.Create(thread, threadFuncArgs));
                index += 1;
            }

            // Go through the list of client threads, and start each of the threads with the appropriate arguments.
            foreach (var threadNArg in clientThreads)
            {
                var thread = threadNArg.Item1;
                var arg = threadNArg.Item2;

                thread.Start(arg);
            }

            // We run numGrains iterations of the experiment. The coordinator thread calls the function "WaitForWorkers" in order to wait
            // for the client threads to finish concurrent calls to a single grain. 
            for (int i = 0; i < numGrains; ++i)
            {
                WaitForWorkers(configList.Count, coordWakeup, toWait);
            }

            // Once the clients threads have finished calling the grain the appropriate number of times, we wait for them to write out their results.
            foreach (var threadNArg in clientThreads)
            {
                var thread = threadNArg.Item1;
                thread.Join();
            }

            // Finally, we return an array of results.
            return results;
        }

        // This function is used to test the case where two different clusters are racing, trying to activate the same grain.
        [TestMethod, TestCategory("GeoCluster")]
        public void TestSingleSingleClusterRace()
        {
            // Create two clusters, each with 1 silo. 
            var cluster0 = CreateCluster(Path.Combine(ConfigPrefix, "Config_Cluster0.xml"), 1);
            var cluster1 = CreateCluster(Path.Combine(ConfigPrefix, "Config_Cluster1.xml"), 1);

            WaitForLivenessToStabilize();

            // Create two clients, connect each client to the appropriate cluster.
            int numGrains = 2000;
            List<string> clientConfigs = new List<string>
            {
                Path.Combine(ConfigPrefix, "Config_Client0.xml"),
                Path.Combine(ConfigPrefix, "Config_Client1.xml")
            };
                
            // DoConcurrentExperiment will run each client specified in clientConfigs in a separate thread. Each of these clients will race for
            // the activation of a sequence of grains. The grains are in the range [0, numGrains).
            var results = DoConcurrentExperiment(clientConfigs, numGrains);

            // maxDictionary keeps track of the maximum value of the grain that we have seen after the first round of the experiment. 
            Dictionary<int, int> maxDictionary = new Dictionary<int, int>();
            for (int i = 0; i < numGrains; ++i)
            {
                var result0 = results[0][i];
                var result1 = results[1][i];

                int max = result0.Item2 > result1.Item2 ? result0.Item2 : result1.Item2;
                maxDictionary.Add(i, max);

                // If maxDictionary[i] is 1, then it means that the clients have created separate optimistic activations. If maxDictionary[i] == 2,
                // then it means that clients raced, but before the second one could call the duplicate optimistic activation, it finds out
                // about the remote activation, and kills its local activation. In this case, both calls happen to the same activation.
                Assert.IsTrue(max == 1 || max == 2);
            }

            // We sleep in order to allow for the asynchronous activation creation algorithm to run. The activation creation algorithm runs, and
            // eventually kills off one of the optimistically created activations of a grain. Once the optimistically created activation is killed,
            // we store a CACHED reference to the winner activation. Therefore, all calls that follow must call the _same grain_.
            Thread.Sleep(60000);

            // We perform another run of concurrent experiments. This time, since the activation creation algorithm has already run, we expect
            // that all calls from concurrent clients will reference the same activation of a grain.
            var newResults = DoConcurrentExperiment(clientConfigs, numGrains);
            for (int i = 0; i < numGrains; ++i)
            {
                var result0 = newResults[0][i].Item2;
                var result1 = newResults[1][i].Item2;
 
                // Since we're now calling duplicate activations of the same grain, expect to see a sequence of values.
                if (maxDictionary[i] == 1)
                {
                    Assert.IsTrue((result0 == 2 && result1 == 3) || (result0 == 3 && result1 == 2));
                }
                else
                {
                    Assert.IsTrue((result0 == 3 && result1 == 4) || (result0 == 4 && result1 == 3));
                }
            }

            var clusterCached0 = new HashSet<GrainId>();
            var clusterCached1 = new HashSet<GrainId>();
            var clusterOwned0 = new HashSet<GrainId>();
            var clusterOwned1 = new HashSet<GrainId>();

            // We now get the content in each of the grain directory partitions of the clusters.
            Action<HashSet<GrainId>, HashSet<GrainId>, Dictionary<GrainId, IGrainInfo>> func = (cached, owned, dict) =>
            {
                foreach (var grainKeyValue in dict)
                {
                    var activation = grainKeyValue.Value;
                    var actInfo = activation.Instances.First().Value;
                    if (actInfo.Status == ActivationStatus.CACHED)
                    {
                        cached.Add(grainKeyValue.Key);
                    }
                    else if (actInfo.Status == ActivationStatus.OWNED)
                    {
                        owned.Add(grainKeyValue.Key);
                    }
                }
            };

            cluster0.ForEach(handle => func(clusterCached0, clusterOwned0, handle.Silo.TestHookup.GetDirectory()));
            cluster1.ForEach(handle => func(clusterCached1, clusterOwned1, handle.Silo.TestHookup.GetDirectory()));

            // Since both clients raced to create the same grain, we expect one cluster to contain a CACHED activation of the grain, and the other
            // to contain an OWNED activation of the grain. 
            Assert.AreEqual(clusterCached0.Count, clusterOwned1.Count);
            Assert.AreEqual(clusterCached1.Count, clusterOwned0.Count);
            foreach (var grain in clusterCached0)
            {
                Assert.IsTrue(clusterOwned1.Contains(grain));
            }
            foreach (var grain in clusterCached1)
            {
                Assert.IsTrue(clusterOwned0.Contains(grain));
            }
        }

        // This test is exactly the same as TestSingleSingleClusterRace. The only difference is that we run each cluster with more than one silo, 
        // and also use multiple clients connected to silos in the same cluster. The structure of the experiment itself is identical to
        // that of TestSingleSingleClusterRace. 
        [TestMethod, TestCategory("GeoCluster")]
        public void TestMultiMultiClusterRace()
        {
            // Create two clusters, each with 5 silos.
            var cluster0 = CreateCluster(Path.Combine(ConfigPrefix, "Config_Cluster0.xml"), 5);
            var cluster1 = CreateCluster(Path.Combine(ConfigPrefix, "Config_Cluster1.xml"), 5);

            WaitForLivenessToStabilize();

            int numGrains = 2000;

            // Create multiple clients. Two clients connect to each cluster.
            List<string> clientConfigs = new List<string>
            {
                Path.Combine(ConfigPrefix, "Config_Client0.xml"),
                Path.Combine(ConfigPrefix, "Config_Client1.xml"),
                Path.Combine(ConfigPrefix, "Config_Client2.xml"),
                Path.Combine(ConfigPrefix, "Config_Client3.xml"),
            };

            // DoConcurrentExperiment will run each client specified in clientConfigs in a separate thread. Each of these clients will race for
            // the activation of a sequence of grains. The grains are in the range [0, numGrains).
            var results = DoConcurrentExperiment(clientConfigs, numGrains);

            //
            Dictionary<int, int> maxDictionary = new Dictionary<int, int>();
            var temp = new int[clientConfigs.Count];
            for (int i = 0; i < numGrains; ++i)
            {
                temp[0] = results[0][i].Item2;
                temp[1] = results[1][i].Item2;
                temp[2] = results[2][i].Item2;
                temp[3] = results[3][i].Item2;

                maxDictionary.Add(i, temp.Max());
            }

            // Wait for the asynchronously invoked activation creation protocol to run. Once the protocol finishes, we can be sure that the grain
            // directories contain only OWNED and CACHED activations.
            Thread.Sleep(60000);

            // Do another run of our concurrent experiment. However, this time, we expect to see a sequence of values because all clients must use
            // a single activation. There are no duplicate, optimistically created instances of a grain in two clusters.
            var newResults = DoConcurrentExperiment(clientConfigs, numGrains);
            
            // For each of the results that get, ensure that we see a sequence of values.
            for (int i = 0; i < numGrains; ++i)
            {
                var result0 = newResults[0][i];
                var result1 = newResults[1][i];
                var result2 = newResults[2][i];
                var result3 = newResults[3][i];

                var expectedCount = maxDictionary[i]*4 + 10;

                if (!(result0.Item2 + result1.Item2 + result2.Item2 + result3.Item2 == expectedCount) &&
                    result0.Item2 > 0 && result1.Item2 > 0 && result2.Item2 > 0 &&
                    result0.Item2 != result1.Item2 && result0.Item2 != result2.Item2 && result0.Item2 != result3.Item2 &&
                    result1.Item2 != result2.Item2 && result1.Item2 != result3.Item2 &&
                    result2.Item2 != result3.Item2)
                {
                    Assert.Fail();
                }
                Assert.IsTrue((result0.Item2 + result1.Item2 + result2.Item2 + result3.Item2 == expectedCount) &&
                    result0.Item2 > 0 && result1.Item2 > 0 && result2.Item2 > 0 &&
                    result0.Item2 != result1.Item2 && result0.Item2 != result2.Item2 && result0.Item2 != result3.Item2 &&
                    result1.Item2 != result2.Item2 && result1.Item2 != result3.Item2 &&
                    result2.Item2 != result3.Item2);
            }

            // We now verify the invariant that all activations that are in state OWNED in one cluster, _must_ be in state CACHED in the other
            // cluster. 
            var clusterCached0 = new HashSet<GrainId>();
            var clusterCached1 = new HashSet<GrainId>();
            var clusterOwned0 = new HashSet<GrainId>();
            var clusterOwned1 = new HashSet<GrainId>();

            Action<HashSet<GrainId>, HashSet<GrainId>, Dictionary<GrainId, IGrainInfo>> func = (cached, owned, dict) =>
            {
                foreach (var grainKeyValue in dict)
                {
                    var activation = grainKeyValue.Value;
                    var actInfo = activation.Instances.First().Value;
                    if (actInfo.Status == ActivationStatus.CACHED)
                    {
                        cached.Add(grainKeyValue.Key);
                    }
                    else if (actInfo.Status == ActivationStatus.OWNED)
                    {
                        owned.Add(grainKeyValue.Key);
                    }
                }
            };

            // Since both clients raced to create the same grain, we expect one cluster to contain a CACHED activation of the grain, and the other
            // to contain an OWNED activation of the grain. 
            cluster0.ForEach(handle => func(clusterCached0, clusterOwned0, handle.Silo.TestHookup.GetDirectory()));
            cluster1.ForEach(handle => func(clusterCached1, clusterOwned1, handle.Silo.TestHookup.GetDirectory()));
            Assert.AreEqual(clusterCached0.Count, clusterOwned1.Count);
            Assert.AreEqual(clusterCached1.Count, clusterOwned0.Count);
            foreach (var grain in clusterCached0)
            {
                Assert.IsTrue(clusterOwned1.Contains(grain));
            }
            foreach (var grain in clusterCached1)
            {
                Assert.IsTrue(clusterOwned0.Contains(grain));
            }
        }

#endregion Race Conditions

        #region Conflict Resolution
     
        private void ValidateDirectory(Dictionary<GrainId, IGrainInfo> directory, Func<IActivationInfo, bool> test)
        {
            // Check that the grain directory contains only doubtful activations.
            foreach (var grainKeyValue in directory)
            {
                var grainId = grainKeyValue.Key;
                var grainInfo = grainKeyValue.Value;

                foreach (var actKeyValue in grainInfo.Instances)
                {
                    var actId = actKeyValue.Key;
                    var actInfo = actKeyValue.Value;
                    if (!test(actInfo))
                    {
                        Assert.Fail();
                    }
                    Assert.IsTrue(test(actInfo));
                }
            }
        }

        // Beware: This test is highly timing dependent. I found it very hard to get the timing right.
        // This test is exactly the same as TestConflictResolution. The only difference is that we use more silos per cluster.
        [TestMethod, TestCategory("GeoCluster")]
        public void TestMultiConflictResolution()
        {
            var cluster0 = CreateCluster(Path.Combine(ConfigPrefix, "Config_Cluster0.xml"), 3);
            var cluster1 = CreateCluster(Path.Combine(ConfigPrefix, "Config_Cluster1.xml"), 3);

            // Turn off intercluster messaging.
            cluster0.ForEach(handle => handle.Silo.TestHookup.SwitchClusterMessaging(false));
            cluster1.ForEach(handle => handle.Silo.TestHookup.SwitchClusterMessaging(false));

            WaitForLivenessToStabilize();

            var client0 = CreateClient(Path.Combine(ConfigPrefix, "Config_Client0.xml"));
            var client1 = CreateClient(Path.Combine(ConfigPrefix, "Config_Client1.xml"));
            var client2 = CreateClient(Path.Combine(ConfigPrefix, "Config_Client2.xml"));
            var client3 = CreateClient(Path.Combine(ConfigPrefix, "Config_Client3.xml"));

            var clients = new ClientWrapper[] {client0, client1, client2, client3};

            cluster0.ForEach(handle => handle.Silo.TestHookup.SetLookupTimeout(TimeSpan.FromMilliseconds(10.0)));
            cluster1.ForEach(handle => handle.Silo.TestHookup.SetLookupTimeout(TimeSpan.FromMilliseconds(10.0)));

            for (int i = 0; i < 40; ++i)
            {
                int res0, res1, res2, res3;
                if (i%2 == 1)
                {
                    res0 = clients[0].CallGrain(i);
                    res1 = clients[1].CallGrain(i);
                    res2 = clients[2].CallGrain(i);
                    res3 = clients[3].CallGrain(i);
                }
                else
                {
                    res0 = clients[1].CallGrain(i);
                    res1 = clients[0].CallGrain(i);
                    res2 = clients[0].CallGrain(i);
                    res3 = clients[1].CallGrain(i);
                }

                Assert.AreEqual(1, res0);
                Assert.AreEqual(1, res1);
                Assert.AreEqual(2, res2);
                Assert.AreEqual(2, res3);
            }

            // We put this thread to sleep to allow the activation creation protocol to kick in. The protocol will timeout on trying to create
            // activations because we have turned off messaging across clusters.
            Thread.Sleep(60000);

            Func<IActivationInfo, bool> validateDoubtful =
                (info =>
                    (info.Status == ActivationStatus.DOUBTFUL || info.Status == ActivationStatus.SYSTEM));
            cluster0.ForEach(handle => ValidateDirectory(handle.Silo.TestHookup.GetDirectory(), validateDoubtful));
            cluster1.ForEach(handle => ValidateDirectory(handle.Silo.TestHookup.GetDirectory(), validateDoubtful));

            cluster0.ForEach(handle => handle.Silo.TestHookup.SetLookupTimeout(TimeSpan.FromSeconds(2.0)));
            cluster1.ForEach(handle => handle.Silo.TestHookup.SetLookupTimeout(TimeSpan.FromSeconds(2.0)));

            // Turn on intercluster messaging and wait for the resolution to kick in.
            cluster0.ForEach(handle => handle.Silo.TestHookup.SwitchClusterMessaging(true));
            cluster1.ForEach(handle => handle.Silo.TestHookup.SwitchClusterMessaging(true));

            // Wait for anti-entropy to kick in. One of the DOUBTFUL activations must be killed, and the other must be converted to OWNED.
            Thread.Sleep(60000);

            Func<IActivationInfo, bool> validateOwned =
                (info =>
                    (info.Status != ActivationStatus.DOUBTFUL));
            cluster0.ForEach(handle => ValidateDirectory(handle.Silo.TestHookup.GetDirectory(), validateOwned));
            cluster1.ForEach(handle => ValidateDirectory(handle.Silo.TestHookup.GetDirectory(), validateOwned));

            // The checks in this loop are timing dependent. We need to ensure that the grain whose DOUBTFUL activation was killed now refers to
            // the remote OWNED activation.
            for (int i = 0; i < 40; ++i)
            {
                int res0, res1, res2, res3;
                if (i % 2 == 1)
                {
                    res0 = clients[0].CallGrain(i);
                    res1 = clients[1].CallGrain(i);
                    res2 = clients[2].CallGrain(i);
                    res3 = clients[3].CallGrain(i);
                }
                else
                {
                    res0 = clients[1].CallGrain(i);
                    res1 = clients[0].CallGrain(i);
                    res2 = clients[0].CallGrain(i);
                    res3 = clients[1].CallGrain(i);
                }

                Assert.AreEqual(3, res0);
                Assert.AreEqual(4, res1);
                Assert.AreEqual(5, res2);
                Assert.AreEqual(6, res3);
            }
        }

        // Thsis function is used to test the anti-entropy protocol.
        [TestMethod, TestCategory("GeoCluster")]
        public void TestConflictResolution()
        {
            var cluster0 = CreateCluster(Path.Combine(ConfigPrefix, "Config_Cluster0.xml"), 1);
            var cluster1 = CreateCluster(Path.Combine(ConfigPrefix, "Config_Cluster1.xml"), 1);

            // Turn off intercluster messaging to simulate a partition.
            cluster0[0].Silo.TestHookup.SwitchClusterMessaging(false);
            cluster1[0].Silo.TestHookup.SwitchClusterMessaging(false);

            WaitForLivenessToStabilize();

            var client0 = CreateClient(Path.Combine(ConfigPrefix, "Config_Client0.xml"));
            var client1 = CreateClient(Path.Combine(ConfigPrefix, "Config_Client1.xml"));

            cluster0[0].Silo.TestHookup.SetLookupTimeout(TimeSpan.FromMilliseconds(10.0));
            cluster1[0].Silo.TestHookup.SetLookupTimeout(TimeSpan.FromMilliseconds(10.0));

            // This should create two activations of the grain on each cluster.
            for (int i = 0; i < 1000; ++i)
            {
                var res0 = client0.CallGrain(i);
                var res1 = client1.CallGrain(i);

                Assert.AreEqual(1, res0);
                Assert.AreEqual(1, res1);
            }

            // We put this thread to sleep to allow the activation creation protocol to kick in. The protocol will timeout on trying to create
            // activations because we have turned off messaging across clusters.
            Thread.Sleep(10000);

            // Validate that all the created grains are in state DOUBTFUL.
            Func<IActivationInfo, bool> validateDoubtful =
                (info =>
                    (info.Status == ActivationStatus.DOUBTFUL || info.Status == ActivationStatus.SYSTEM));
            ValidateDirectory(cluster0[0].Silo.TestHookup.GetDirectory(), validateDoubtful);
            ValidateDirectory(cluster1[0].Silo.TestHookup.GetDirectory(), validateDoubtful);

            // Adjust the lookup timeout so that it doesn't fail.
            cluster0[0].Silo.TestHookup.SetLookupTimeout(TimeSpan.FromMilliseconds(2000.0));
            cluster1[0].Silo.TestHookup.SetLookupTimeout(TimeSpan.FromMilliseconds(2000.0));

            // Turn on intercluster messaging.
            cluster0[0].Silo.TestHookup.SwitchClusterMessaging(true);
            cluster1[0].Silo.TestHookup.SwitchClusterMessaging(true);

            // Wait for anti-entropy to kick in. One of the DOUBTFUL activations must be killed, and the other must be converted to OWNED.
            Thread.Sleep(30000);

            var dict0 = cluster0[0].Silo.TestHookup.GetDirectory();
            var dict1 = cluster1[0].Silo.TestHookup.GetDirectory();

            // Validate that all the duplicates have been resolved.
            Func<IActivationInfo, bool> validateOwned =
                (info =>
                    info.Status != ActivationStatus.DOUBTFUL);
            ValidateDirectory(dict0, validateOwned);
            ValidateDirectory(dict1, validateOwned);

            cluster0[0].Silo.TestHookup.SetLookupTimeout(TimeSpan.FromSeconds(1.0));
            cluster1[0].Silo.TestHookup.SetLookupTimeout(TimeSpan.FromSeconds(1.0));

            // The checks in this loop are timing dependent. We need to ensure that the grain whose DOUBTFUL activation was killed now refers to
            // the remote OWNED activation.
            for (int i = 0; i < 1000; ++i)
            {
                var res0 = client0.CallGrain(i);
                var res1 = client1.CallGrain(i);
                
                Assert.IsTrue(res0 == 2 && res1 == 3);
            }
        }

        // This function is used to test the anti-entropy phase of the protocol.
        [TestMethod, TestCategory("GeoCluster")]
        public void TestResolution()
        {
            // Create two clusters, each with one silo each.
            var cluster0 = CreateCluster(Path.Combine(ConfigPrefix, "Config_Cluster0.xml"), 1);
            var cluster1 = CreateCluster(Path.Combine(ConfigPrefix, "Config_Cluster1.xml"), 1);

            // Two new grains.
            var grain0 = GrainId.NewId();
            var grain1 = GrainId.NewId();

            var silo0 = cluster0[0].Silo.SiloAddress;
            var silo1 = cluster1[0].Silo.SiloAddress;

            // Insert two different activations into the grain directory.
            var actId0 = ActivationId.NewId();
            var actId1 = ActivationId.NewId();
            var addr0 = cluster0[0].Silo.TestHookup.ForceAddActivation(grain0, actId0, silo0, ActivationStatus.DOUBTFUL);
            var addr1 = cluster1[0].Silo.TestHookup.ForceAddActivation(grain1, actId1, silo1, ActivationStatus.DOUBTFUL);

            // Ensure that the activations were correctly added.
            Assert.AreEqual(ActivationAddress.GetAddress(silo0, grain0, actId0), addr0);
            Assert.AreEqual(ActivationAddress.GetAddress(silo1, grain1, actId1), addr1);

            // Wait for the activations to resolve.
            Thread.Sleep(20000);

            // We're going to inspect grain directories.
            var dict0 = cluster0[0].Silo.TestHookup.GetDirectory();
            var dict1 = cluster1[0].Silo.TestHookup.GetDirectory();

            var stateAct0 = dict0[grain0].Instances[actId0].Status;
            var stateAct1 = dict1[grain1].Instances[actId1].Status;

            // We expect that since the activations did not conflict with each other. Both are upgraded from DOUBTFUL to OWNED.
            Assert.AreEqual(ActivationStatus.OWNED, stateAct0);
            Assert.AreEqual(ActivationStatus.OWNED, stateAct1);
        }
      
        #endregion Conflict Resolution
    }
}
