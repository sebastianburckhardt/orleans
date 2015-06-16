using Microsoft.WindowsAzure.Storage.Pileus.Utils;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.Storage.Pileus.Configuration
{
    // TODO: Upload client-server RTTS to the cloud as well.  These moved out of SessionState and into ServerMonitor.
    // So we need a new TableEntity, I suppose.

    // TODO: Keep the TableEntity classes hidden in this class.
    
    /// <summary>
    /// Stores information in the cloud (i.e. Azure) about the read/write patterns and SLAs being used by clients.
    /// This data is used by the Reconfiguration Service (i.e. Configurator).
    /// </summary>
    public class ClientUsageCloudStore
    {
        private string clientName;

        /// <summary>
        /// CloudTableClient used for writing SLA and SessionStates by client, and reading them by the configurator.
        /// </summary>
        public CloudTableClient ConfigurationCloudTableClient { get; private set; }

        /// <summary>
        /// Constructor for ClientUsageCloudStore
        /// </summary>
        /// <param name="account">the Azure account where client data is stored</param>
        public ClientUsageCloudStore(CloudStorageAccount account, string clientName)
        {
            this.clientName = clientName;
            this.ConfigurationCloudTableClient = account.CreateCloudTableClient();
        }

        public void UploadClientData(string containerName, int configurationEpoch, List<ConsistencySLAEngine> engines, SessionState sessionState, ServerMonitor monitor)
        {
            CloudTableClient client = ConfigurationCloudTableClient;

            //First we send SLAs, then we try sending session
            CloudTable slaTable = client.GetTableReference(ConstPool.SLA_CONFIGURATION_TABLE_NAME);
            if (!slaTable.Exists())
            {
                slaTable.CreateIfNotExists();
            }
            //List<ConsistencySLAEngine> engines = CapCloudBlobClient.slaEngines[containerName];
            foreach (ConsistencySLAEngine engine in engines)
            {
                try
                {
                    TableOperation retrieveOperation = TableOperation.Retrieve<ServiceLevelAgreementTableEntity>(containerName + configurationEpoch, clientName + engine.Sla.Id);
                    TableResult result = slaTable.Execute(retrieveOperation);

                    ServiceLevelAgreementTableEntity entity = (ServiceLevelAgreementTableEntity)result.Result;

                    // TODO: move this code elsewhere or remove it
                    /*
                    if (CapCloudBlobClient.numberOfClients != null)
                    {
                        engine.Sla.AdjustHitsAndMisses(CapCloudBlobClient.numberOfClients.Invoke());
                    }
                    */

                    if (entity != null)
                    {
                        entity.SetSLA(engine.Sla);
                        TableOperation updateSLA = TableOperation.Replace(entity);
                        slaTable.Execute(updateSLA);
                    }
                    else
                    {
                        //The key of the entity is the concatenation of client name plus sla id. 
                        entity = new ServiceLevelAgreementTableEntity(engine.Sla, containerName, "" + configurationEpoch, clientName);
                        TableOperation insertSLA = TableOperation.Insert(entity);
                        slaTable.Execute(insertSLA);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.StackTrace);
                    throw ex;
                }
            }

            //Now, it's time to send sessionState

            CloudTable stateTable = client.GetTableReference(ConstPool.SESSION_STATE_CONFIGURATION_TABLE_NAME);
            if (!stateTable.Exists())
            {
                stateTable.CreateIfNotExists();
            }
            try
            {
                // TODO: Why does this only need a single session state?
                //SessionStateTableEntity entity = new SessionStateTableEntity(engines.First().SessionState, ClientRegistry.GetNonReplicaServerState(), containerName, "" + configurationEpoch, clientName);
                // TODO: This is totally broken; need to revise SessionStateTableEntity
                SessionStateTableEntity entity = new SessionStateTableEntity(sessionState, null, null, containerName, "" + configurationEpoch, clientName);
                TableOperation insertSession = TableOperation.InsertOrReplace(entity);
                stateTable.Execute(insertSession);
                //SessionState.configurationEpoch = configurationEpoch;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.StackTrace);
                throw ex;
            }

        }

        /// <summary>
        /// Read the client data (i.e., SLAs and SessionStates) from the Azure's storage. (OBSOLETE)
        /// </summary>
        /// <param name="containerName">The name of the container</param>
        /// <param name="configurationEpoch">The configuration epoch number</param>
        /// <param name="SLAs"></param>
        /// <param name="sessionState"></param>
        public void ReadClientData(string containerName, int configurationEpoch, SortedSet<ServiceLevelAgreement> SLAs, Dictionary<string, SessionStateTableEntity> sessionStates)
        {
            CloudTableClient client = ConfigurationCloudTableClient;

            try
            {
                CloudTable slaTable = client.GetTableReference(ConstPool.SLA_CONFIGURATION_TABLE_NAME);

                TableQuery<ServiceLevelAgreementTableEntity> query = new TableQuery<ServiceLevelAgreementTableEntity>().
                    Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, containerName + configurationEpoch));


                List<ServiceLevelAgreementTableEntity> results = slaTable.ExecuteQuery(query).ToList();
                foreach (ServiceLevelAgreementTableEntity entity in results)
                {
                    SLAs.Add(entity.GetSLA());
                }
            }
            catch (StorageException)
            {
                // no stored SLAs; ignore
            }

            try
            {
                CloudTable sessionTable = client.GetTableReference(ConstPool.SESSION_STATE_CONFIGURATION_TABLE_NAME);

                TableQuery<SessionStateTableEntity> query2 = new TableQuery<SessionStateTableEntity>().
                    Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, containerName + configurationEpoch));

                List<SessionStateTableEntity> results2 = null;
                results2 = sessionTable.ExecuteQuery(query2).ToList();
                foreach (SessionStateTableEntity entity in results2)
                {
                    sessionStates[entity.ClientName] = entity;
                    entity.GetNonReplicaServers();
                    entity.GetPrimaryReplicaServers();
                    entity.GetSecondaryReplicaServers();
                }
            }
            catch (StorageException)
            {
                // no stored session state; ignore
            }
        }

        /// <summary>
        /// Read the client data (i.e., SLAs and SessionStates) from the Azure's storage. (NEW VERSION)
        /// </summary>
        /// <param name="containerName">The name of the container</param>
        /// <param name="configurationEpoch">The configuration epoch number</param>
        /// <param name="SLAs">Set into which retreived SLAs are placed</param>
        /// <param name="clientData">Hashtable into which retrieved client data is placed</param>
        public Dictionary<string, ClientUsageData> ReadClientData(string containerName, int configurationEpoch /*, SortedSet<ServiceLevelAgreement> SLAs, Dictionary<string, ClientUsageData> clientData*/)
        {
            Dictionary<string, ClientUsageData> clientData = new Dictionary<string, ClientUsageData>();
            
            // read SLAs from cloud
            CloudTableClient cloudClient = ConfigurationCloudTableClient;
            try
            {
                CloudTable slaTable = cloudClient.GetTableReference(ConstPool.SLA_CONFIGURATION_TABLE_NAME);

                TableQuery<ServiceLevelAgreementTableEntity> query = new TableQuery<ServiceLevelAgreementTableEntity>().
                    Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, containerName + configurationEpoch));


                List<ServiceLevelAgreementTableEntity> results = slaTable.ExecuteQuery(query).ToList();
                foreach (ServiceLevelAgreementTableEntity entity in results)
                {
                    if (!clientData.ContainsKey(entity.ClientName))
                    {
                        clientData[entity.ClientName] = new ClientUsageData(entity.ClientName);
                    }
                    clientData[entity.ClientName].SLAs.Add(entity.GetSLA());
                }
            }
            catch (StorageException)
            {
                // no stored SLAs; ignore
            }

            // read other client data from cloud
            try
            {
                CloudTable sessionTable = cloudClient.GetTableReference(ConstPool.SESSION_STATE_CONFIGURATION_TABLE_NAME);

                TableQuery<SessionStateTableEntity> query2 = new TableQuery<SessionStateTableEntity>().
                    Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, containerName + configurationEpoch));

                List<SessionStateTableEntity> results2 = null;
                results2 = sessionTable.ExecuteQuery(query2).ToList();
                foreach (SessionStateTableEntity entity in results2)
                {
                    // create client data object if necessary (but it should already exist)
                    if (!clientData.ContainsKey(entity.ClientName))
                    {
                        clientData[entity.ClientName] = new ClientUsageData(entity.ClientName);
                    }
                    
                    // record reads and writes
                    clientData[entity.ClientName].NumberOfReads = entity.NumberOfReads;
                    clientData[entity.ClientName].NumberOfWrites = entity.NumberOfWrites;
                    
                    // record round-trip times to servers
                    foreach (ServerState server in entity.GetPrimaryReplicaServers().Values)
                    {
                        clientData[entity.ClientName].ServerRTTs[server.Name] = server.RTTs;
                    }
                    foreach (ServerState server in entity.GetSecondaryReplicaServers().Values)
                    {
                        clientData[entity.ClientName].ServerRTTs[server.Name] = server.RTTs;
                    }
                    foreach (ServerState server in entity.GetNonReplicaServers().Values)
                    {
                        clientData[entity.ClientName].ServerRTTs[server.Name] = server.RTTs;
                    }
                }
            }
            catch (StorageException)
            {
                // no stored session state; ignore
            }

            return clientData;
        }

        private Dictionary<string, bool> activeUploaders = new Dictionary<string, bool>();

        /// <summary>
        /// Periodically uploads SLAs and SessionStates.
        /// SLAs and SessionStates are stored in two Azure tables called.
        /// </summary>
        public void StartUploadConfigurationTask(string containerName, List<ConsistencySLAEngine> engines, SessionState sessionState, ServerMonitor monitor)
        {
            activeUploaders[containerName] = true;
            int sleepDuration = 0;
            int epoch;
            DateTimeOffset lastModified;
            ReplicaConfiguration configuration = ClientRegistry.GetConfiguration(containerName, false);
            if (configuration == null)
                activeUploaders[containerName] = false;

            while (activeUploaders[containerName])
            {
                try
                {
                    configuration.SyncWithCloud(ClientRegistry.GetConfigurationAccount());

                    while (!configuration.IsInFastMode())
                    {
                        lock (configuration)
                            Monitor.Wait(configuration, 1000);
                    }

                    epoch = configuration.Epoch;
                    Console.WriteLine("Uploading SLA and SessionState with Epoch: " + epoch);
                    lastModified = configuration.EpochModifiedTime;

                    //    |====a====|-conf in progress-|====b====|
                    //Clients upload their configurations between the middle of a configuration period, and before a configuration epoch is finished.
                    //I.e., after for example a, and before the first next bar, or after b, and before the next upcomming bar.
                    if (DateTimeOffset.Now.Subtract(lastModified).TotalMilliseconds > ConstPool.CONFIGURATION_UPLOAD_INTERVAL / 2)
                    {
                        UploadClientData(containerName, epoch, engines, sessionState, monitor);
                        //Console.WriteLine("Uploaded the configuration to the azure storage. ...");
                        sleepDuration = ConstPool.CONFIGURATION_UPLOAD_INTERVAL;
                    }
                    else
                    {
                        //we try to push the upload to some time after the middle of an epoch duration.
                        sleepDuration = (ConstPool.CONFIGURATION_UPLOAD_INTERVAL / 2) - DateTimeOffset.Now.Subtract(lastModified).Milliseconds;
                    }
                    Thread.Sleep(sleepDuration);

                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.StackTrace);
                    throw ex;
                }
            }
        }

        public void EndUploadConfigurationTask(string containerName)
        {
            activeUploaders[containerName] = false;
        }
    }

    /// <summary>
    /// Data obtained from each client that is used for reconfiguration decisions and actions.
    /// </summary>
    public class ClientUsageData
    {
        public string ClientName { get; set; }

        public int NumberOfReads { get; set; }
        public int NumberOfWrites { get; set; }

        public Dictionary<string, LatencyDistribution> ServerRTTs { get; set; }

        public List<ServiceLevelAgreement> SLAs { get; set; }

        public ClientUsageData(string client) 
        {
            this.ClientName = client;
            this.NumberOfReads = 0;
            this.NumberOfWrites = 0;
            this.ServerRTTs = new Dictionary<string, LatencyDistribution>();
            this.SLAs = new List<ServiceLevelAgreement>();
        }
    }
}
