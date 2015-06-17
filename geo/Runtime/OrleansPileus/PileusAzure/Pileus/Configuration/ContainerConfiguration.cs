using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using System.Runtime.CompilerServices;
using System.Threading;
using Microsoft.WindowsAzure.Storage.Pileus.Utils;
using System.Diagnostics;

namespace Microsoft.WindowsAzure.Storage.Pileus.Configuration
{
    /* Deprecated: Do not Use */
    
    /// <summary>
    /// Maintains a list of primary and secondary servers.
    /// 
    /// NOTE: the notion of ServerName is exactly the name of a Cloud Storage Account Name. 
    /// The notion of servername and storage account name are used interchanbly in this project.
    /// So, a storage account name "dbteuropestorage" in Account.cs will lead to having a server name called "dbteuropestorage" in this class.
    /// 
    /// TODO Remove Console Writes.
    /// </summary>
    [Serializable()]
    public class ContainerConfiguration 
    {
        private static Task refreshConfigurationTask;

        /// <summary>
        /// Initializes a new instance of <see cref="ContainerConfiguration"/> by reading it from the Azure storage. 
        /// 
        /// This constructor is private. User must call the static <see cref="ReadConfiguration"/> to get an instance of this object.
        /// </summary>
        /// <param name="capCloudBlobContainerName">The name of the CapCloudBlobContainer. </param>
        /// <param name="sessionState">session state of the SLAEngine. This state is used for registering, and unregistering newly added/removed replicas</param>
        private ContainerConfiguration(string capCloudBlobContainerName, SessionState sessionState)
        {
            PrimaryServers = new List<string>();
            SecondaryServers = new List<string>();
            WriteOnlyPrimaryServers = new List<string>();
            NonReplicaServers = new List<string>();
            SyncPeriod = new Dictionary<string, int>();
            MainPrimaryIndex = 0;
            Epoch = 0;

            this.Name = capCloudBlobContainerName;
            this.State = sessionState;
        }

        /// <summary>
        /// Initializes an instance of <see cref="ContainerConfiguration"/>
        /// </summary>
        /// <param name="capCloudBlobContainerName">>The name of the CapCloudBlobContainer</param>
        /// <param name="primaryServer"></param>
        /// <param name="secondaryServer"></param>
        /// <param name="sessionState">session state of the SLAEngine. This state is used for registering, and unregistering newly added/removed replicas</param>
        private ContainerConfiguration(string capCloudBlobContainerName,
            string primaryServer, string secondaryServer, SessionState sessionState)
        {
            PrimaryServers = new List<string>();
            SecondaryServers = new List<string>();
            WriteOnlyPrimaryServers = new List<string>();
            NonReplicaServers = new List<string>();

            SyncPeriod = new Dictionary<string, int>();

            this.Name = capCloudBlobContainerName;

            if (ConfigurationLookup.IsValidServer(primaryServer))
                PrimaryServers.Add(primaryServer);

            if (ConfigurationLookup.IsValidServer(secondaryServer))
            {
                SecondaryServers.Add(secondaryServer);
                SyncPeriod[secondaryServer] = ConstPool.DEFAULT_SYNC_INTERVAL;
            }

            MainPrimaryIndex = 0;
            Epoch = 0;

            this.State = sessionState;
        }

        #region Properties

        private SessionState State { get; set; }

        //Name of the CapCloudBlobContainer (AKA tablet's name in the context of [SOSP'13])
        public string Name { get; private set; }

        //List of Primary severs
        public List<string> PrimaryServers { get; set; }

        //List of write only Primary servers (already existed in PrimaryServers) that should not be registered in the session state.
        //These servers are newly added servers and must be used only for put operations, but cannot be used for get_primary operations because they have not catch the whole container. 
        //Therefore, get_primary is not allowed to be received in them. 
        public List<string> WriteOnlyPrimaryServers { get; set; }

        //List of Secondary servers
        public List<string> SecondaryServers { get; set; }

        //List of Non-replicating servers, i.e. those that could possibly hold replicas but currently do not
        public List<string> NonReplicaServers { get; set; }

        //Maps secondary server name to corresponding syncperiod.
        public Dictionary<string, int> SyncPeriod;

        public int MainPrimaryIndex { get; private set; }

        public string MainPrimaryServer
        {
            get
            {
                return PrimaryServers[MainPrimaryIndex];
            }
        }

        /// <summary>
        /// Configuration's Epoch number.
        /// </summary>
        public int Epoch { get; private set; }

        /// <summary>
        /// The time when the epoch is modified. 
        /// </summary>
        public DateTimeOffset EpochModifiedTime { get; private set; }
        #endregion

        #region Static Methods

        /// <summary>
        /// Create a configuration of a CapCloudBlobContainer and save it in the Azure's configuration blob if there is not an instance there.
        /// </summary>
        /// <param name="capCloudBlobContainerName">The name of the CapCloudBlobContainer</param>
        /// <param name="primaryServer"></param>
        /// <param name="secondaryServer"></param>
        /// <param name="sessionState">session state of the SLAEngine. This state is used for registering, and unregistering newly added/removed replicas</param>
        /// <returns></returns>
        public static ContainerConfiguration CreateConfiguration(string capCloudBlobContainerName,
            string primaryServer, string secondaryServer, SessionState sessionState, bool autoRefresh = false)
        {
            ContainerConfiguration result = new ContainerConfiguration(capCloudBlobContainerName, primaryServer, secondaryServer,sessionState);
            // result.EndCurrentEpoch();  // is this needed?
            result.InitializeConfigurationBlob();
            result.StartNewEpoch(true);

            if (autoRefresh)
            {
                refreshConfigurationTask = Task.Factory.StartNew(() => result.RefreshConfigurationPeriodically());
            }

            return result;

        }

        /// <summary>
        /// Delete the container from the azure storage.
        /// </summary>
        /// <param name="capCloudBlobContainerName">The name of the CapCloudBlobContainer</param>
        /// <returns></returns>
        public static void DeleteConfiguration(string capCloudBlobContainerName)
        {
            //ConfigurationLookup.GetConfigurationContainer(capCloudBlobContainerName).DeleteIfExists();
        }

        /// <summary>
        /// Read an already created configuration of a CapCloudBlobContainer from the Azure's configuration blob.
        /// </summary>
        /// <param name="capCloudBlobContainerName">Name of the CapCloudBlobContainer</param>
        /// <param name="sessionState">session state of the SLAEngine. This state is used for registering, and unregistering newly added/removed replicas</param>
        /// <param name="autoRefresh">if true, it will automatically refresh the configuration by reading it periodically from the cloud storage.</param>
        /// <returns></returns>
        public static ContainerConfiguration ReadConfiguration(string capCloudBlobContainerName, SessionState sessionState, bool autoRefresh)
        {
            ContainerConfiguration result = new ContainerConfiguration(capCloudBlobContainerName,sessionState);

            //reads new configuration
            result.ReadConfiguration();
            if (autoRefresh)
                refreshConfigurationTask = Task.Factory.StartNew(() => result.RefreshConfigurationPeriodically());

            if (result.PrimaryServers != null && result.PrimaryServers.Count >0)
            {
                return result;
            }
            else
                return null;
        }

        /// <summary>
        /// Refresh the configuration right now.
        /// </summary>
        public void RefreshConfigurationNow()
        {
            this.ReadConfiguration();
        }

        #endregion

        #region private Methods
        
        /// <summary>
        /// Register the primary servers with the session state in order to be to perform read operations on them. 
        /// Therefore, those primary servers in <see cref="WriteOnlyPrimaryServers"/> will not be registered.
        /// This is because they are called write primaries, and only write operations will be performed on them. 
        /// No read operation is allowed to go to them. Hence, they are not registed with the session state. 
        /// </summary>
        private void SyncPrimaryServersWithSessionState()
        {
            int i = 0;
            foreach (string server in PrimaryServers){
                if (WriteOnlyPrimaryServers != null && WriteOnlyPrimaryServers.Contains(server))
                    continue;
                //State.RegisterServer(server,true,i++);
            }
            
        }

        /// <summary>
        /// Register secondary replicas with the session state in order to perform read operations on them. 
        /// </summary>
        private void SyncSecondaryServersWithSessionState()
        {
            int i = 0;
            foreach (string server in SecondaryServers)
            {
                //State.RegisterServer(server, false, i++);
            }

        }

        /// <summary>
        /// Removed already registered servers in session states if they don't exist anymore (because of some reconfiguration).
        /// </summary>
        private void garbageCollectOldServersFromSessionState()
        {
            List<string> allServers = new List<string>();
            PrimaryServers.ForEach(o => allServers.Add(o));
            SecondaryServers.ForEach(o => allServers.Add(o));

            //State.UnregisterOldServers(allServers);
        }

        /// <summary>
        /// Initializes a blob by writing an empty stream to the azure.
        /// </summary>
        private void InitializeConfigurationBlob()
        {
            try
            {
                CloudBlobContainer container = GetConfigurationContainer();
                if (!container.Exists())
                    container.Create();
                ICloudBlob blob = container.GetBlockBlobReference(ConstPool.CURRENT_CONFIGURATION_BLOB_NAME);
                
                // break lease if left from previous execution
                try
                {
                    blob.BreakLease(new TimeSpan(1));
                }
                catch
                {
                }

                using (MemoryStream stream = new MemoryStream())
                {
                    BinaryFormatter formatter = new BinaryFormatter();
                    formatter.Serialize(stream, "");
                    stream.Position = 0;
                    blob.UploadFromStream(stream);
                }
            }
            catch (StorageException ex)
            {
                Console.WriteLine(ex.ToString());
                throw ex;
            }
        }

        /// <summary>
        /// Periodically refresh the configuration.
        /// </summary>
        private void RefreshConfigurationPeriodically()
        {
            try
            {


                Stopwatch w = new Stopwatch();
                CloudBlobContainer configurationContainer = GetConfigurationContainer();
                ICloudBlob blob = configurationContainer.GetBlockBlobReference(ConstPool.CURRENT_CONFIGURATION_BLOB_NAME);


                while (true)
                {

                    while (!blob.Exists())
                    {
                        Console.WriteLine("Configuration blob is null for " + this.Name);
                        Thread.Sleep(ConstPool.CACHED_CONFIGURATION_VALIDITY_DURATION);
                    }

                    int oldEpoch = Epoch;
                    Epoch = -1;
                    w.Restart();

                    blob.FetchAttributes();
                    int newEpoch = Convert.ToInt32(blob.Metadata[ConstPool.EPOCH_NUMBER]);
                    if (newEpoch > 0)
                    {
                        if (newEpoch != oldEpoch)
                        {
                            //foreach (ConsistencySLAEngine engine in CapCloudBlobClient.slaEngines[this.Name])
                            //{
                            //    engine.Sla.ResetHitsAndMisses();
                            //}
                            ReadConfiguration();

                            Console.WriteLine("New configuration for Epoch " + newEpoch + " is primaries: " + String.Join(", ", PrimaryServers.ToList()) + " secondaries:" + String.Join(", ", SecondaryServers.ToList()));
                        }
                        Epoch = newEpoch;

                        w.Stop();
                        if (ConstPool.CACHED_CONFIGURATION_VALIDITY_DURATION - Convert.ToInt32(w.ElapsedMilliseconds) > 0)
                            Thread.Sleep(ConstPool.CACHED_CONFIGURATION_VALIDITY_DURATION - Convert.ToInt32(w.ElapsedMilliseconds));

                        lock (this)
                            Monitor.PulseAll(this);
                    }
                    else
                    {
                        //Cached Configuration is changing, we need to wait for the meantime. 
                        Thread.Sleep(ConstPool.CONFIGURATION_ACTION_DURATION);
                    }
                }
            }
            catch (StorageException ex)
            {
                Console.WriteLine(ex.ToString());
                throw ex;
            }
            catch (Exception exx)
            {
                Console.WriteLine(exx.ToString());
                throw exx;
            }
        }

        #endregion

        #region internal Methods
        
        /// <summary>
        /// Read the state of the container configuration from the Azure storage.
        /// </summary>
        internal void ReadConfiguration()
        {
            ICloudBlob blob;
            
            try
            {
                try
                {
                    blob = GetConfigurationContainer().GetBlockBlobReference(ConstPool.CURRENT_CONFIGURATION_BLOB_NAME);
                    blob.FetchAttributes();
                }
                catch (StorageException se)
                {
                    //304 is azure's not modified exception.
                    //it means that the configuration is not modified since last read.
                    if (StorageExceptionCode.NotModified(se))
                    {
                        Console.WriteLine("ReadConfiguration returned without reading...");
                        return;
                    }

                    //404 is Azure's not-found exception
                    if (StorageExceptionCode.NotFound(se))
                    {
                        Console.WriteLine("ReadConfiguration did not find a configuration blob; it needs to be created...");
                        PrimaryServers = null;
                        return;
                    }

                    Console.WriteLine(se.ToString());
                    Console.WriteLine(se.StackTrace);
                    throw se;
                }

                using (MemoryStream stream = new MemoryStream())
                {
                    blob.DownloadToStream(stream);
                    stream.Seek(0, 0);
                    BinaryFormatter formatter = new BinaryFormatter();
                    ContainerConfiguration tmp = (ContainerConfiguration)formatter.Deserialize(stream);

                    if (!blob.Metadata.ContainsKey(ConstPool.EPOCH_NUMBER))
                    {
                        Console.WriteLine("No Epoch in configuration metadata!");  // this should not happen
                        return;  // stay with current configuration for now
                    }
                    
                    int currentEpoch = Convert.ToInt32(blob.Metadata[ConstPool.EPOCH_NUMBER]);
                    if (currentEpoch > Epoch)
                    {
                        PrimaryServers = tmp.PrimaryServers;
                        PrimaryServers.Sort();
                        SecondaryServers = tmp.SecondaryServers ?? new List<string>();
                        MainPrimaryIndex = tmp.MainPrimaryIndex;
                        WriteOnlyPrimaryServers = tmp.WriteOnlyPrimaryServers ?? new List<string>();
                        SyncPeriod = tmp.SyncPeriod;

                        SyncPrimaryServersWithSessionState();
                        SyncSecondaryServersWithSessionState();
                        garbageCollectOldServersFromSessionState();

                        Epoch = currentEpoch;

                        if (blob.Metadata[ConstPool.EPOCH_MODIFIED_TIME] != null)
                            EpochModifiedTime = DateTimeOffset.Parse(blob.Metadata[ConstPool.EPOCH_MODIFIED_TIME]);
                        else
                            EpochModifiedTime = DateTimeOffset.MinValue;
                    }
                }
            }
            catch (StorageException se)
            {
                Console.WriteLine(se.ToString());
                Console.WriteLine(se.StackTrace);
                throw se;               
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                Console.WriteLine(ex.StackTrace);
                //throw ex;
            }

        }


        /// <summary>
        /// By writing an empty string in metadata[EPOCH_NUMBER] of the configuration blob, all clients will eventually read it, and enter the slow mode. 
        /// </summary>
        internal void EndCurrentEpoch()
        {
            try
            {
                using (CloudBlobLease lease = new CloudBlobLease(GetConfigurationContainer().GetBlockBlobReference(ConstPool.CURRENT_CONFIGURATION_BLOB_NAME), LeaseTakingPolicy.TryUntilSuccessful))
                {
                    ICloudBlob blob = GetConfigurationContainer().GetBlockBlobReference(ConstPool.CURRENT_CONFIGURATION_BLOB_NAME);
                    blob.Metadata[ConstPool.EPOCH_NUMBER] = ConstPool.RECONFIGURATION_IN_PROGRESS;
                    blob.SetMetadata(lease.getAccessConditionWithLeaseId());
                }
                Thread.Sleep(ConstPool.CACHED_CONFIGURATION_VALIDITY_DURATION);
            }
            catch (StorageException ex)
            {
                Console.WriteLine(ex.ToString());
                throw ex;
            }
        }

        /// <summary>
        /// Starts a new configuration epoch.
        /// </summary>
        /// <param name="updateConfiguration">If true, the configuration is also uploaded to the azure storage.</param>
        /// <returns></returns>
        internal void StartNewEpoch(bool uploadConfiguration)
        {
            using (CloudBlobLease lease = new CloudBlobLease(GetConfigurationContainer().GetBlockBlobReference(ConstPool.CURRENT_CONFIGURATION_BLOB_NAME),LeaseTakingPolicy.TryUntilSuccessful))
            {
                if (lease.HasLease)
                {
                    if (uploadConfiguration)
                        UploadConfiguration(lease.getAccessConditionWithLeaseId().LeaseId);

                    try
                    {
                        CloudBlobContainer configurationContainer = GetConfigurationContainer();

                        ICloudBlob blob = configurationContainer.GetBlockBlobReference(ConstPool.CURRENT_CONFIGURATION_BLOB_NAME);
                        int newEpoch = ++this.Epoch;
                        blob.Metadata[ConstPool.EPOCH_NUMBER] = "" + newEpoch;
                        blob.Metadata[ConstPool.EPOCH_MODIFIED_TIME] = DateTimeOffset.Now.ToString();
                        blob.SetMetadata(lease.getAccessConditionWithLeaseId());

                        Console.WriteLine("Epoch "  + Epoch + " written to the azure and started.");
                    }
                    catch (StorageException ex)
                    {
                        Console.WriteLine(ex.ToString());
                        throw ex;
                    }
                }
            }
            SyncPrimaryServersWithSessionState();
            SyncSecondaryServersWithSessionState();
        }

        /// <summary>
        /// Uploads the configuration to the cloud
        /// </summary>
        /// <returns></returns>
        internal void UploadConfiguration(string leaseId)
        {
            try
            {
                ICloudBlob blob = GetConfigurationContainer().GetBlockBlobReference(ConstPool.CURRENT_CONFIGURATION_BLOB_NAME);
                blob.FetchAttributes();

                using (MemoryStream stream = new MemoryStream())
                {
                    BinaryFormatter formatter = new BinaryFormatter();
                    formatter.Serialize(stream, this);
                    stream.Position = 0;
                    AccessCondition access=AccessCondition.GenerateIfMatchCondition(blob.Properties.ETag);
                    if (leaseId != "")
                        access.LeaseId = leaseId;
                    blob.UploadFromStream(stream, access);
                }
            }
            catch (StorageException ex)
            {
                Console.WriteLine(ex.ToString());
                throw ex;
            }
        }

        #endregion

        #region public Methods

        public bool IsInFastMode()
        {
            return Epoch >= 0;
        }

        //The container used for read/write of the configuration state.
        public CloudBlobContainer GetConfigurationContainer()
        {
            //return ConfigurationLookup.GetConfigurationContainer(Name);
            return null;
        }

        public CloudBlobContainer GetMainPrimaryContainer()
        {
            return ConfigurationLookup.GetCloudBlobContainer(MainPrimaryServer, this.Name);
        }

        public List<CloudBlobContainer> GetPrimaryContainers()
        {
            List<CloudBlobContainer> result = new List<CloudBlobContainer>();
            PrimaryServers.ForEach(s => result.Add(ConfigurationLookup.GetCloudBlobContainer(s, this.Name)));
            return result;
        }

        public List<CloudBlobContainer> GetSecondaryContainers()
        {
            List<CloudBlobContainer> result = new List<CloudBlobContainer>();
            SecondaryServers.ForEach(s => result.Add(ConfigurationLookup.GetCloudBlobContainer(s, this.Name)));
            return result;
        }

        public List<CloudBlobContainer> GetAllContainers()
        {
            List<CloudBlobContainer> result = new List<CloudBlobContainer>();
            PrimaryServers.ForEach(s => result.Add(ConfigurationLookup.GetCloudBlobContainer(s, this.Name)));
            SecondaryServers.ForEach(s => result.Add(ConfigurationLookup.GetCloudBlobContainer(s, this.Name)));
            return result;
        }

        public ICloudBlob GetCloudBlob(string serverName, string blobName, bool isRead = true)
        {
            return ConfigurationLookup.GetCloudBlob(serverName, this.Name, blobName, isRead);
        }

        /// <summary>
        /// Returns the list of servers that do not replicate this container. 
        /// </summary>
        /// <returns></returns>
        public List<string> GetNoReplicaServers()
        {
            List<string> result = new List<string>();
            foreach (string server in ConfigurationLookup.AllServers)
            {
                if ((!PrimaryServers.Contains(server))&& (!SecondaryServers.Contains(server)))
                {
                    result.Add(server);
                }
            }
            return result;
        }

        #endregion

        public override string ToString()
        {
            string result;

            result = "Container: " + this.Name;
            result += " Primaries: ";
            PrimaryServers.ForEach(e => result += e + ",");
            result += " Secondaries: ";
            SecondaryServers.ForEach(e => result += e + ",");

            return result;
        }
    }
}
