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

using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Orleans.TestingHost;
using System.Reflection;
using System.Globalization;
using UnitTests.Tester;
using Orleans.Runtime.Configuration;

namespace Tests.GeoClusterTests
{
    public class TestingClusterHost   
    {
        protected readonly Dictionary<string, ClusterInfo> clusters;

        public TestingClusterHost() : base()
        {
            clusters = new Dictionary<string, ClusterInfo>();

            UnitTestSiloHost.CheckForAzureStorage();
        }

        protected struct ClusterInfo
        {
            public List<SiloHandle> silos;  // currently active silos
            public string config;                 // the configuration file TODO: keep this as a structure
        }

        private static readonly string ConfigPrefix =
              Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);

        public static string GetConfigFile(string fileName)
        {
            return Path.Combine(ConfigPrefix, fileName);
        }
        public static void WriteLog(string format, params object[] args)
        {
            Console.WriteLine(format, args);
        }

        #region Cluster Creation

        /// <summary>
        /// This function takes a Config file corresponding to a
        /// ClusterConfiguration object and the number of silos to
        /// create in the cluster. It returns the ID of the newly
        /// created cluster. Silos in a cluster are created in the following
        /// order: The first one is the Primary node, then Secondary_1,
        /// Secondary_2, and so on. The configuration file can specify
        /// overrides for each of these silos.
        /// 
        /// At least the primary is always created.
        /// 
        /// TODO: maintain the config object instead of reloading from file everytime?
        /// </summary>
        /// <param name="configFile"></param>
        /// <param name="numSilos"></param>
        /// <returns></returns>
        public string NewCluster(string configFile, int numSilos, Action<ClusterConfiguration> customizer = null)
        {
            WriteLog("Starting New Cluster...");
            var silohandles = new SiloHandle[numSilos];

            var primaryOption = new TestingSiloOptions
            {
                SiloConfigFile = new FileInfo(configFile),
                StartClient = false,
                AutoConfigNodeSettings = false,
                SiloName = "Primary",
                ConfigurationCustomizer = customizer,
            };
            silohandles[0] = TestingSiloHost.StartOrleansSilo(null, Silo.SiloType.Primary, primaryOption, 0);
           
            Parallel.For(1, numSilos, i =>
            {
                var options = new TestingSiloOptions
                {
                    SiloConfigFile = new FileInfo(configFile),
                    StartClient = false,
                    AutoConfigNodeSettings = false,
                    SiloName = "Secondary_" + i,
                    ConfigurationCustomizer = customizer
               };

                silohandles[i] = TestingSiloHost.StartOrleansSilo(null, Silo.SiloType.Secondary, options, i);
            });
            
            string clusterId = silohandles[0].Silo.GlobalConfig.ClusterId;

            clusters[clusterId] = new ClusterInfo { config = configFile,
                                                    silos = silohandles.ToList() };

            WriteLog("Cluster {0} started.", clusterId);
            return clusterId;
        }

        public void AddSiloToCluster(string clusterId, string siloName, Action<ClusterConfiguration> customizer = null)
        {
            var clusterinfo = clusters[clusterId];

            var options = new TestingSiloOptions
            {
                SiloConfigFile = new FileInfo(clusterinfo.config),
                StartClient = false,
                AutoConfigNodeSettings = false,
                SiloName = siloName, 
                ConfigurationCustomizer = customizer
            };
            var silo = TestingSiloHost.StartOrleansSilo(null, Silo.SiloType.Secondary, options, clusterinfo.silos.Count);
        }

        public void StopCluster(string cluster)
        {
            var info = clusters[cluster];
            Parallel.For(1, info.silos.Count, i => TestingSiloHost.StopSilo(info.silos[i]));
            TestingSiloHost.StopSilo(info.silos[0]);
        }

        public void StopAllClientsAndClusters()
        {
            WriteLog("Stopping All Clients and Clusters...");
            StopAllClients();
            StopAllClusters();
            WriteLog("All Clients and Clusters Are Stopped.");
        }

        public void StopAllClusters()
        {
            Parallel.ForEach(clusters.Keys, key => StopCluster(key));
        }

        public List<SiloHandle> GetSilosInCluster(string clusterId)
        {
            return clusters[clusterId].silos;
        }

        #endregion

        #region client wrappers

        private readonly List<AppDomain> activeClients = new List<AppDomain>();

        // Create a client, loaded in a new app domain.
        public T CreateClient<T>(string name, string configFile)
        {
            var clientArgs = new object[] { configFile };
            var setup = new AppDomainSetup { ApplicationBase = Environment.CurrentDirectory };
            var clientDomain = AppDomain.CreateDomain(name, null, setup);

            T client = (T)clientDomain.CreateInstanceFromAndUnwrap(
                    Assembly.GetExecutingAssembly().Location, typeof(T).FullName, false,
                    BindingFlags.Default, null, clientArgs, CultureInfo.CurrentCulture,
                    new object[] { });

            lock (activeClients)
            {
                activeClients.Add(clientDomain);
            }
            return client;
        }

        public void StopAllClients()
        {
            // Take a working copy
            List<AppDomain> clients = new List<AppDomain>();
            lock (activeClients)
            {
                clients.AddRange(activeClients);
                activeClients.Clear();
            }

            foreach (var client in clients)
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
        }

        #endregion

        #region Cluster Config


        public void BlockAllClusterCommunication(string from, string to)
        {
            foreach (var silo in clusters[from].silos)
                foreach (var dest in clusters[to].silos)
                    silo.Silo.TestHookup.BlockSiloCommunication(dest.Endpoint, 100);
        }

        public void UnblockAllClusterCommunication(string from)
        {
            foreach (var silo in clusters[from].silos)
                    silo.Silo.TestHookup.UnblockSiloCommunication();
        }

  
        private SiloHandle GetActiveSiloInClusterByName(string clusterId, string siloName)
        {
            if (clusters[clusterId].silos == null) return null;
            return clusters[clusterId].silos.Find(s => s.Name == siloName);
        }
        #endregion
    }
}