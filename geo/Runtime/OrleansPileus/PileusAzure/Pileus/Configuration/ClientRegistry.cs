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
    /// <summary>
    /// This is a singleton used for maintaining a local cache of <see cref="Configuration"/>, <see cref="CloudStorageAccount"/>,
    /// and <see cref="CloudBlobClient"/>
    /// and also for obtaining a <see cref="CloudBlobContainer"/> and <see cref="ICloudBlob"/> for a given container and server.
    /// </summary>
    public static class ClientRegistry
    {
        // Maps CapCloudContainer's name to its configuration
        private static Dictionary<string, ReplicaConfiguration> configurations = new Dictionary<string, ReplicaConfiguration>();

        // Maps name of a CapCloudContainer to its corresponding CloudBlobClient that is used for performing read operations
        // These client objects are shared between threads. Hence, they will be used only for performing fast reads. 
        private static Dictionary<string, CloudBlobClient> sharedClients;

        // The set of accounts that can be used to store replicated data; maps a server name to the account info
        private static Dictionary<string, CloudStorageAccount> accounts;

        // The account for storing configuration and usage data
        private static CloudStorageAccount configurationAccount;

        public static void Init(Dictionary<string, CloudStorageAccount> replicaAccounts, CloudStorageAccount configAccount)
        {
            configurations = new Dictionary<string, ReplicaConfiguration>();
            accounts = replicaAccounts;
            sharedClients = new Dictionary<string, CloudBlobClient>();
            configurationAccount = configAccount;
        }

        /// <summary>
        /// Returns the <see cref="ReplicaConfiguration"/> for a named container.
        /// If the configuration has not yet been cached locally, it is read from Azure's configuration container.
        /// </summary>
        /// <param name="containerName">Name of the container</param>
        /// <param name="cloudBacked">Whether the configuration should be fetched from Azure</param>
        /// <param name="autoRefresh">If true, the configuration will automatically refresh by reading its state from the azure storage. Hence, all new configurations will be reflected here automatically.</param>
        /// <returns></returns>
        public static ReplicaConfiguration GetConfiguration(string containerName, bool cloudBacked = true, bool autoRefresh = true)
        {
            if (!configurations.ContainsKey(containerName))
            {
                configurations[containerName] = new ReplicaConfiguration(containerName);
                if (cloudBacked)
                {
                    configurations[containerName].SyncWithCloud(configurationAccount, autoRefresh);
                }
            }
            return configurations[containerName];
        }

        [MethodImplAttribute(MethodImplOptions.Synchronized)]
        public static void AddConfiguration(ReplicaConfiguration Configuration)
        {
            configurations[Configuration.Name] = Configuration;
        }

        public static void RemoveConfiguration(ReplicaConfiguration Configuration)
        {
            configurations.Remove(Configuration.Name);
        }

        public static CloudStorageAccount GetConfigurationAccount()
        {
            return configurationAccount;
        }

        public static CloudStorageAccount GetAccount(string serverName)
        {
            CloudStorageAccount account = null;
            string accountName = serverName;
            if (serverName.EndsWith("-secondary"))
            {
                // use primary account for Azure geo-replicated secondary
                accountName = serverName.Replace("-secondary", "");
            }

            if (accounts == null)
            {
                accounts = new Dictionary<string, CloudStorageAccount>();
            }
            if (accounts.ContainsKey(accountName))
            {
                account = accounts[accountName];
            }
            return account;
        }

        public static CloudBlobClient GetCloudBlobClient(string serverName)
        {
            CloudBlobClient client = null;
            if (sharedClients == null)
            {
                sharedClients = new Dictionary<string, CloudBlobClient>();
            }
            if (sharedClients.ContainsKey(serverName))
            {
                client = sharedClients[serverName];
            }
            else  // need to create client
            {
                CloudStorageAccount account = GetAccount(serverName);
                if (account != null)
                {
                    client = account.CreateCloudBlobClient();
                    if (serverName.EndsWith("-secondary"))
                    {
                        client.DefaultRequestOptions.LocationMode = LocationMode.SecondaryOnly;
                    }
                    else
                    {
                        client.DefaultRequestOptions.LocationMode = LocationMode.PrimaryOnly;
                    }
                    sharedClients[serverName] = client;
                }
            }
            return client;
        }

        /// <summary>
        /// Returns a CloudBlobContainer for the given containerName at the given server.
        /// </summary>
        /// <param name="serverName">Name of the Server (I.e., name of the storage account)</param>
        /// <param name="containerName">Name of the container</param>
        /// <returns></returns>
        public static CloudBlobContainer GetCloudBlobContainer(string serverName, string containerName)
        {
            CloudBlobClient client = GetCloudBlobClient(serverName);
            CloudBlobContainer result = client.GetContainerReference(containerName);
            IEnumerable<IListBlobItem> blobs = client.ListBlobs("");
            IEnumerable<CloudBlobContainer> containers = client.ListContainers();
            return result;
        }

        /// <summary>
        /// Returns a CloudBlobContainer for the given containerName at the main primary server.
        /// </summary>
        /// <param name="containerName">Name of the container</param>
        /// <returns></returns>
        public static CloudBlobContainer GetMainPrimaryContainer(string containerName)
        {
            string serverName = GetConfiguration(containerName).PrimaryServers.First();
            CloudBlobContainer result = GetCloudBlobContainer(serverName, containerName);
            return result;
        }

        /// <summary>
        /// Returns the CloudBlobContainer used for storing configuration blobs of the given containerName. 
        /// </summary>
        /// <param name="containerName"></param>
        /// <returns></returns>
        public static CloudBlobContainer GetConfigurationContainer(string containerName)
        {
            CloudBlobClient configClient = configurationAccount.CreateCloudBlobClient();
            CloudBlobContainer result = configClient.GetContainerReference(ConstPool.CONFIGURATION_CONTAINER_PREFIX + containerName);
            return result;
        }

        /// <summary>
        /// Returns <see cref="CloudBlockBlob"/> from shared clients if the request is for a re ad operation.
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
        public static ICloudBlob GetCloudBlob(string serverName, string containerName, string blobName, bool isRead = true)
        {
            try
            {
                ICloudBlob result;
                if (isRead)
                {
                    result = GetCloudBlobContainer(serverName, containerName).GetBlockBlobReference(blobName);
                }
                else
                {


                    CloudStorageAccount httpAcc = GetAccount(serverName); // new CloudStorageAccount(GetAccount(serverName).Credentials, false);

                    result = httpAcc.CreateCloudBlobClient().GetContainerReference(containerName).GetBlockBlobReference(blobName);


                }
                return result;
            }
            catch (Exception e)
            {
                Trace.TraceError(e.ToString());
            }
            return null;    
        }
    }
}
