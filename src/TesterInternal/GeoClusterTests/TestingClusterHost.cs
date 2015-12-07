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
using System.Net;
using System.Net.Sockets;
using Orleans;
using Microsoft.VisualStudio.TestTools.UnitTesting;

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
            public List<SiloHandle> Silos;  // currently active silos
            public int SequenceNumber; // we number created clusters in order of creation
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




        #region Default Cluster and Client Configuration

        private void NodeOverride(ClusterConfiguration config, string siloName, int port, int proxyGatewayEndpoint = 0)
        {
            NodeConfiguration nodeConfig = config.GetConfigurationForNode(siloName);
            nodeConfig.HostNameOrIPAddress = "loopback";
            nodeConfig.Port = port;
            nodeConfig.DefaultTraceLevel = config.Defaults.DefaultTraceLevel;
            nodeConfig.PropagateActivityId = config.Defaults.PropagateActivityId;
            nodeConfig.BulkMessageLimit = config.Defaults.BulkMessageLimit;
            if (proxyGatewayEndpoint != 0)
                nodeConfig.ProxyGatewayEndpoint = new IPEndPoint(IPAddress.Loopback, proxyGatewayEndpoint);
            config.Overrides[siloName] = nodeConfig;
        }
     
        
        // A default multi-cluster configuration setup 
        // that works for up to two clusters with up to five silos each

        public void AdjustClusterConfigurationDefaults(ClusterConfiguration c, int clusternumber)
        {
            if ((clusternumber < 0) || (clusternumber > 1))
                throw new ArgumentException("currently only support two clusters");

            c.Globals.GlobalServiceId = "mctesting";
            c.Globals.ClusterId = "cluster" + clusternumber.ToString();
            c.Globals.NumMultiClusterGateways = 2;
            c.Globals.DefaultMultiCluster = null;

            c.Globals.GossipChannels = new List<Orleans.Runtime.Configuration.GlobalConfiguration.GossipChannelConfiguration>(1) { 
                new Orleans.Runtime.Configuration.GlobalConfiguration.GossipChannelConfiguration()
                {
                   ChannelType = Orleans.Runtime.Configuration.GlobalConfiguration.GossipChannelType.AzureTable,
                   ConnectionString = StorageTestConstants.DataConnectionString
                }
            };

            if (clusternumber == 0)
            {
                c.Globals.SeedNodes.Clear();
                c.Globals.SeedNodes.Add(new IPEndPoint(IPAddress.Loopback, 21111));
                NodeOverride(c,"Primary",  21111, 22221);
                NodeOverride(c, "Secondary_1", 21112, 22222);
                NodeOverride(c, "Secondary_2", 21113, 22223);                 
                NodeOverride(c, "Secondary_3", 21114);
                NodeOverride(c, "Secondary_4", 21115);
            }
            else if (clusternumber == 1)
            {
                c.Globals.SeedNodes.Clear();
                c.Globals.SeedNodes.Add(new IPEndPoint(IPAddress.Loopback, 21116));
                NodeOverride(c, "Primary", 21116,  22226);
                NodeOverride(c, "Secondary_1", 21117, 22227);
                NodeOverride(c, "Secondary_2", 21118);
                NodeOverride(c, "Secondary_3", 21119);
                NodeOverride(c, "Secondary_4", 21120);
            }
        }

        // The corresponding client configuration defaults
        public int DetermineGatewayPort(int clusternumber, int clientnumber)
        {
            if (clusternumber == 0)
            {
               return clientnumber % 2 == 0 ? 22221 : 22222;     
            }
            else if (clusternumber == 1)
            {
               return clientnumber % 2 == 0 ? 22226 : 22227;
            }

            else
                throw new ArgumentException("currently only support two clusters");
        }

        #endregion

        #region Cluster Creation

        /// <summary>
        /// This function takes the number of silos to
        /// create in the cluster, and a delegate that can override configurations.
        /// It returns the ID of the newly
        /// created cluster.  
        /// 
        /// </summary>
        /// <param name="configFile"></param>
        /// <param name="numSilos"></param>
        /// <returns></returns>
        public string NewCluster(int numSilos, Action<ClusterConfiguration> customizer = null)
        {
            lock (clusters)
            {
                WriteLog("Starting New Cluster...");
                var mycount = clusters.Count;

                Action<ClusterConfiguration> configurationcustomizer = (config) =>
                    {
                        // use default configuration for this cluster
                        AdjustClusterConfigurationDefaults(config, mycount);
                        // add custom configurations
                        if (customizer != null)
                            customizer(config);
                    };

                var silohandles = new SiloHandle[numSilos];

                var primaryOption = new TestingSiloOptions
                {
                    StartClient = false,
                    AutoConfigNodeSettings = false,
                    SiloName = "Primary",
                    ConfigurationCustomizer = configurationcustomizer
                };
                silohandles[0] = TestingSiloHost.StartOrleansSilo(null, Silo.SiloType.Primary, primaryOption, 0);

                Parallel.For(1, numSilos, i =>
                {
                    var options = new TestingSiloOptions
                    {
                        StartClient = false,
                        AutoConfigNodeSettings = false,
                        SiloName = "Secondary_" + i,
                        ConfigurationCustomizer = configurationcustomizer
                    };

                    silohandles[i] = TestingSiloHost.StartOrleansSilo(null, Silo.SiloType.Secondary, options, i);
                });

                string clusterId = silohandles[0].Silo.GlobalConfig.ClusterId;

                clusters[clusterId] = new ClusterInfo
                {
                    Silos = silohandles.ToList(),
                    SequenceNumber = mycount
                };

                WriteLog("Cluster {0} started.", clusterId);
                return clusterId;
            }
        }

        public void AddSiloToCluster(string clusterId, string siloName, Action<ClusterConfiguration> customizer = null)
        {
            var clusterinfo = clusters[clusterId];

            var options = new TestingSiloOptions
            {
                StartClient = false,
                AutoConfigNodeSettings = false,
                SiloName = siloName, 
                ConfigurationCustomizer = customizer
            };
            var silo = TestingSiloHost.StartOrleansSilo(null, Silo.SiloType.Secondary, options, clusterinfo.Silos.Count);
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
            lock (clusters)
            {
                Parallel.ForEach(clusters.Keys, key =>
                {
                    var info = clusters[key];
                    Parallel.For(1, info.Silos.Count, i => TestingSiloHost.StopSilo(info.Silos[i]));
                    TestingSiloHost.StopSilo(info.Silos[0]);
                });
                clusters.Clear();
            }
        }

        public List<SiloHandle> GetSilosInCluster(string clusterId)
        {
            return clusters[clusterId].Silos;
        }

        #endregion

        #region client wrappers

        private readonly List<AppDomain> activeClients = new List<AppDomain>();

        public class ClientWrapperBase : MarshalByRefObject {

            public string Name { get; private set; }

            public ClientWrapperBase(string name, int gatewayport)
            {
                this.Name = name;

                Console.WriteLine("Initializing client {0} in AppDomain {1}", name, AppDomain.CurrentDomain.FriendlyName);

                ClientConfiguration config = null;
                try
                {
                    config = ClientConfiguration.LoadFromFile("ClientConfigurationForTesting.xml");
                }
                catch (Exception) { }

                if (config == null)
                {
                    Assert.Fail("Error loading client configuration file");
                }
                config.GatewayProvider = ClientConfiguration.GatewayProviderType.Config;
                config.Gateways.Clear();
                config.Gateways.Add(new IPEndPoint(IPAddress.Loopback, gatewayport));

                GrainClient.Initialize(config);
            }
            
        }

        // Create a client, loaded in a new app domain.
        public T NewClient<T>(string ClusterId, int ClientNumber) where T: ClientWrapperBase
        {
            var ci = clusters[ClusterId];
            var name = string.Format("Client-{0}-{1}", ClusterId, ClientNumber);
            var gatewayport = DetermineGatewayPort(ci.SequenceNumber, ClientNumber);
       
            var clientArgs = new object[] { name, gatewayport };
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
            lock (activeClients)
            {
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
        }

        #endregion

        #region Cluster Config


        public void BlockAllClusterCommunication(string from, string to)
        {
            foreach (var silo in clusters[from].Silos)
                foreach (var dest in clusters[to].Silos)
                    silo.Silo.TestHookup.BlockSiloCommunication(dest.Endpoint, 100);
        }

        public void UnblockAllClusterCommunication(string from)
        {
            foreach (var silo in clusters[from].Silos)
                    silo.Silo.TestHookup.UnblockSiloCommunication();
        }

  
        private SiloHandle GetActiveSiloInClusterByName(string clusterId, string siloName)
        {
            if (clusters[clusterId].Silos == null) return null;
            return clusters[clusterId].Silos.Find(s => s.Name == siloName);
        }
        #endregion
    }
}