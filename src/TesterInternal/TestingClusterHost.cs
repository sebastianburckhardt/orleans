using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Orleans.TestingHost;
using Orleans.MultiCluster;
using System.Reflection;
using System.Globalization;
using UnitTests.Tester;
using Orleans.Runtime.Configuration;

namespace Tests.GeoClusterTests
{
    //TODO: start a cluster by default based on the default config file?
    public class TestingClusterHost: TestingSiloHost
    {
        protected readonly Dictionary<string, ClusterInfo> clusters;

        public TestingClusterHost()
            : base(new TestingSiloOptions
            {
                StartClient = false,
                StartPrimary = false,
                StartSecondary = false
            })
        {
            clusters = new Dictionary<string, ClusterInfo>();

            UnitTestSiloHost.CheckForAzureStorage();
        }

        protected struct ClusterInfo
        {
            public List<SiloHandle> activeSilos;  // currently active silos
            public List<SiloHandle> stoppedSilos; // gracefully stopped silos
            public List<SiloHandle> killedSilos;  // killed silos
            public string config;                 // the configuration file TODO: keep this as a structure
        }

        private static readonly string ConfigPrefix =
              Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);

        public static string GetConfigFile(string fileName)
        {
            return Path.Combine(ConfigPrefix, fileName);
        }

        #region Silos and Cluster Creation

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
            var handleList = new List<SiloHandle>();
            SiloHandle silo;
            var primaryOption = new TestingSiloOptions
            {
                SiloConfigFile = new FileInfo(configFile),
                StartClient = false,
                AutoConfigNodeSettings = false,
                SiloName = "Primary",
                ConfigurationCustomizer = customizer
            };
            silo = StartAdditionalSilo(Silo.SiloType.Primary, primaryOption);
            handleList.Add(silo);

            string clusterId = silo.Silo.GlobalConfig.ClusterId;

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
                silo = StartAdditionalSilo(Silo.SiloType.Secondary, options);
                lock(handleList)
                   handleList.Add(silo);
            });
            
            clusters[clusterId] = new ClusterInfo { config = configFile,
                                                    activeSilos = handleList,
                                                    stoppedSilos = new List<SiloHandle>(),
                                                    killedSilos = new List<SiloHandle>()};
            return clusterId;
        }

        public bool AddSiloToCluster(string clusterId, string siloName, Silo.SiloType type)
        {
            var configFile = clusters[clusterId].config;

            // if cluster was not created
            if (String.IsNullOrEmpty(configFile)) return false;

            var options = new TestingSiloOptions
            {
                SiloConfigFile = new FileInfo(configFile),
                StartClient = false,
                AutoConfigNodeSettings = false,
                SiloName = siloName
            };
            var silo = StartAdditionalSilo(type, options);

            if (clusters[clusterId].activeSilos == null) return false; // should not be the case

            clusters[clusterId].activeSilos.Add(silo);
            return true;
        }

        public bool StopSiloInCluster(string clusterId, string siloName)
        {
            var silo = GetActiveSiloInClusterByName(clusterId, siloName);
            if (silo == null) return false;

            StopSilo(silo);

            clusters[clusterId].activeSilos.Remove(silo);
            clusters[clusterId].stoppedSilos.Add(silo);
           
            return true;
        }

        public bool KillSiloInClusterByName(string clusterId, string siloName)
        {
            var silo = GetActiveSiloInClusterByName(clusterId, siloName);
            if (silo == null) return false;

            KillSilo(silo);

            clusters[clusterId].activeSilos.Remove(silo);
            clusters[clusterId].stoppedSilos.Add(silo);

            return true;
        }

        public bool KillGatewaySiloInCluster(string clusterId)
        {
            //TODO: wait for stabilization to make sure we kill an actual GW?
            var silos = clusters[clusterId].activeSilos;
            if (silos == null || silos.Count == 0)
                return false;

            // get a random active silo and use it to interrogate the gossip network
            SiloHandle silo = silos[0];
            SiloAddress sa = silo.Silo.LocalMultiClusterOracle.GetRandomClusterGateway(silo.Silo.GlobalConfig.ClusterId);

            // find the cluster gateway
            silo = silos.Find(s => s.Silo.SiloAddress.Equals(sa));

            KillSiloInClusterByName(silo.Name, clusterId);
            return true;
        }

        public void StopAllClusters()
        {
            StopAllSilos();
            // TODO: clear structures
        }

        public List<SiloHandle> GetActiveSilosInCluster(string clusterId)
        {
            return clusters[clusterId].activeSilos;
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

        public async Task<bool> InjectClusterConfiguration(string cluster, string Conf)
        {
            // pick an active silo of the specified cluster and gossip the new configuration

            var activeSilo = clusters[cluster].activeSilos.FirstOrDefault();
           
            if (activeSilo == null) return false;

            var clusterlist = Conf.Split(',').ToList();

            activeSilo.Silo.TestHookup.InjectMultiClusterConfiguration(new MultiClusterConfiguration(DateTime.UtcNow, clusterlist));

            await WaitForMultiClusterGossipToStabilizeAsync();

            return true;
        }


  
        private SiloHandle GetActiveSiloInClusterByName(string clusterId, string siloName)
        {
            if (clusters[clusterId].activeSilos == null) return null;
            return clusters[clusterId].activeSilos.Find(s => s.Name == siloName);
        }
        #endregion
    }
}