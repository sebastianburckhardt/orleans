using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.CompilerServices;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Pileus.Utils;
using System.Diagnostics;
using System.Threading;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.RetryPolicies;

namespace Microsoft.WindowsAzure.Storage.Pileus.Configuration
{
    /* Deprecated: Do not Use */
    
    /// <summary>
    /// This is a singleton used for maintaining the list of <see cref="ContainerConfiguration"/>.
    /// 
    /// TODO: provide methods for modifying CloudBlobClients during runtime as well. Currently, they are only provided at start time. 
    /// </summary>
    public static class ConfigurationLookup
    {
        //Maps CapCloudContainer's name to its configuration
        private static Dictionary<string, ContainerConfiguration> ContainerConfigurations = new Dictionary<string, ContainerConfiguration>();

        //maps name of the CloudStorageAccount credential (A.K.A server name) to its corresponding CloudBlobClient that is used for performing read operations.
        //These client obejcts are shared between threads. Hence, they will be used only for performing fast reads. 
        public static Dictionary<string, CloudBlobClient> sharedClients;

        private static Dictionary<string, CloudStorageAccount> accounts;

        //Maps serverName to its corresponding ServerState
        //This is only required to keep track of latencies of servers which are not accessed by the clients. 
        //Hence, during reconfiguration, a client upload these latencies as well along with latencies in SessionState.
        public static Dictionary<string, ServerState> nonReplicaServerStates;

        public static List<string> AllServers
        {
            get
            {
                return sharedClients.Keys.ToList();
            }
        }

        /// <summary>
        /// Client used for reading/writing the configuration form the Azure storage.
        /// </summary>
        public static CloudBlobClient ConfigurationCloudBlobClient { get; private set; }

        /// <summary>
        /// CloudTableClient used for writing SLA and SessionStates by client, and reaing them by the configurator.
        /// </summary>
        public static CloudTableClient ConfigurationCloudTableClient { get; private set; }

        private static Task periodicPingTask;

        public static void Init(Dictionary<string, CloudStorageAccount> replicaAccounts, CloudBlobClient configurationCloudBlobClient, CloudTableClient configurationCloudTableClient, bool periodicPing)
        {
            accounts = replicaAccounts;
            sharedClients = new Dictionary<string, CloudBlobClient>();
            foreach (string key in accounts.Keys)
            {
                CloudBlobClient client;
                client = accounts[key].CreateCloudBlobClient();
                client.LocationMode = LocationMode.PrimaryOnly;
                sharedClients[key] = client;

                // create another client for geo-replicated secondary
                client = accounts[key].CreateCloudBlobClient();
                client.LocationMode = LocationMode.SecondaryOnly;
                sharedClients[key + "-secondary"] = client;
            }
            
            ConfigurationCloudBlobClient = configurationCloudBlobClient;
            ConfigurationCloudTableClient = configurationCloudTableClient;
            nonReplicaServerStates = new Dictionary<string, ServerState>();

            if (periodicPing)
            {
                periodicPingTask = Task.Factory.StartNew(() => PeriodPing());
            }
        }

        /// <summary>
        /// Returns the corresponding <see cref="ContainerConfiguration"/>.
        /// If the container does not exist, it tries to read it from the Azure's configuration container by calling ReadConfiguration.
        /// </summary>
        /// <param name="containerName">Name of the container configuration</param>
        /// <param name="autoRefresh">If true, the configuration will automatically refresh by reading its state from the azure storage. Hence, all new configurations will be reflected here automatically.</param>
        /// <returns></returns>
        public static ContainerConfiguration GetConfiguration(string containerName, bool autoRefresh)
        {
            if (ContainerConfigurations.ContainsKey(containerName))
                return ContainerConfigurations[containerName];
            else
            {
                ContainerConfiguration container = ContainerConfiguration.ReadConfiguration(containerName, /*SessionState.GetInstance()*/ null, autoRefresh);
                if (container != null)
                {
                    ContainerConfigurations[containerName] = container;
                    ICloudBlob blob = container.GetConfigurationContainer().GetBlockBlobReference(ConstPool.CURRENT_CONFIGURATION_BLOB_NAME);
                    if (blob.Exists())
                    {
                        //we break possible leases that are left from last run
                        //TODO: Remove me
                        try
                        {
                            blob.BreakLease(new TimeSpan(1));
                        }
                        catch  
                        {
                        }
                    }
                }
                return container;
            }
        }

        [MethodImplAttribute(MethodImplOptions.Synchronized)]
        public static void AddConfiguration(ContainerConfiguration configuration)
        {
            //if (ContainerConfigurations.ContainsKey(configuration.Name))
            //{
            //    return;
            //}
            //else
            {
                ContainerConfigurations[configuration.Name] = configuration;
            }
        }

        public static void RemoveConfiguration(ContainerConfiguration configuration)
        {
            ContainerConfigurations.Remove(configuration.Name);
        }


        public static bool IsValidServer(string serverName)
        {
            if (serverName == null)
                return false;

            if (sharedClients.ContainsKey(serverName))
                return true;
            else
                return false;
        }

        /// <summary>
        /// Returns a CloudBlobContainer for the given containerName at the given server.
        /// </summary>
        /// <param name="serverName">Name of the Server (I.e., name of the storage account)</param>
        /// <param name="containerName">Name of the container</param>
        /// <returns></returns>
        public static CloudBlobContainer GetCloudBlobContainer(string serverName, string containerName)
        {
            CloudBlobContainer result = sharedClients[serverName].GetContainerReference(containerName);
            return result;
        }

        /// <summary>
        /// Returns <see cref="CloudPageBlob"/> from shared clients if the request is for a read operation.
        /// Otherwise, it creates a new account, and return a new client. 
        /// 
        /// Note that if the same CloudStorageAccount object is shared between reads and writes, it will have a HUGE impact on read performance.
        /// Hence, it was decided to separate accounts. 
        /// 
        /// </summary>
        /// <param name="serverName"></param>
        /// <param name="containerName"></param>
        /// <param name="blobName"></param>
        /// <param name="isRead"></param>
        /// <returns></returns>
        public static ICloudBlob GetCloudBlob(string serverName, string containerName, string blobName, bool isRead=true)
        {
            ICloudBlob result ;
            if (isRead)
            {
                result = sharedClients[serverName].GetContainerReference(containerName).GetPageBlobReference(blobName);
            }
            else
            {
                CloudStorageAccount httpAcc = new CloudStorageAccount(accounts[serverName].Credentials, false);                
                result = httpAcc.CreateCloudBlobClient().GetContainerReference(containerName).GetPageBlobReference(blobName);
            }
            return result;
        }

        /// <summary>
        /// Periodically pings servers not existing in <see cref="SessionState"/>. 
        /// This is fundamental for having a correct reconfiguration since each client needs to send its latency view of the whole system to the configurator.
        /// Hence, periodically, we ping all servers that do not exist in SessionState. 
        /// </summary>
        public static void PeriodPing()
        {
            /*
              Stopwatch watch = new Stopwatch();
               while (true)
               {
                   // if (SessionState.GetInstance().replicas.Count > 0)
                   if (SessionState.replicas.Count > 0)
                   {
                       foreach (string server in sharedClients.Keys.ToList())
                       {
                           // if (!SessionState.GetInstance().replicas.Keys.Contains(server))
                           if (!SessionState.replicas.Keys.Contains(server))
                           {
                               ServerState serverState;
                               if (nonReplicaServerStates.ContainsKey(server))
                                   serverState = nonReplicaServerStates[server];
                               else
                                   serverState = new ServerState(server, false, -1);

                               CloudBlobContainer tmpContainer = sharedClients[server].GetRootContainerReference();
                               watch.Restart();
                               //we perform a dummy operation to get the rtt latency!
                               sharedClients[server].GetServiceProperties();
                               long el = watch.ElapsedMilliseconds;
                               serverState.AddRtt(el);
                               nonReplicaServerStates[server] = serverState;
                           }
                           else
                           {
                               //server is either primary or secondary. But, we still need to ping it if it is not contacted yet. 
                               nonReplicaServerStates.Remove(server);

                               //if the server is not reached yet, we also perform a dummy operation for it.
                               //                            ServerState serverState = SessionState.GetInstance().replicas[server];
                               ServerState serverState = SessionState.replicas[server];
                               if (!serverState.IsContacted())
                               {
                                   CloudBlobContainer tmpContainer = sharedClients[server].GetRootContainerReference();
                                   watch.Restart();
                                   //we perform a dummy operation to get the rtt latency!
                                   sharedClients[server].GetServiceProperties();
                                   serverState.AddRtt(watch.ElapsedMilliseconds);
                               }
                           }
                       }
                   }

                   Thread.Sleep(ConstPool.LOOKUP_PING_INTERVAL);
               }
            */
       }

        public static Dictionary<string, ServerState> GetNonReplicaServerState()
        {
            /*
                        foreach (string server in nonReplicaServerStates.Keys.ToList())
                        {
                            // if (SessionState.GetInstance().replicas.Keys.Contains(server))
                            if (SessionState.replicas.Keys.Contains(server))
                                nonReplicaServerStates.Remove(server);
                        }
            */
            return nonReplicaServerStates;
        }
    }
}
