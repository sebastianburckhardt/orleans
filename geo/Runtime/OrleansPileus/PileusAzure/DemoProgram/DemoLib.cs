using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Pileus;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage.Pileus.Configuration;
using Microsoft.WindowsAzure.Storage.Pileus.Utils;
using Microsoft.WindowsAzure.Storage.Pileus.Configuration.Constraint;
using PileusApp;
using PileusApp.Utils;
using System.Diagnostics;
using System.IO;
using PileusApp.YCSB;
using Microsoft.WindowsAzure.Storage.Pileus.Configuration.Actions;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage;
using System.Threading;

namespace TechFestDemo
{
    public class DemoLib
    {

        #region Local variables
        
        static int numReadWrites = 10;
        static int opsBetweenSyncs = 6;
        static int numBlobsToUse = 10;
        static int numBlobs = 1000;
        static int boundInSeconds = 60;

        static string containerName = "democontainer";

   //     static string configStorageSite = "dbtsouthstorage";
        static string configStorageSite = "devstoreaccount1";

        // For test
        static List<string> PrimaryServers = new List<string>() { "devstoreaccount1" };
        static List<string> SecondaryServers = new List<string>() { "devstoreaccount1" };
        static List<string> NonReplicaServers = new List<string>() { "devstoreaccount1" };
        static List<string> ReadOnlySecondaryServers = new List<string>() { "devstoreaccount1" };

        // For when demo is run in California
        /*
        static List<string> PrimaryServers = new List<string>() { "dbteuropestorage" };
        static List<string> SecondaryServers = new List<string>() { "dbtwestusstorage", "dbteuropestorage-secondary", "dbtwestusstorage-secondary" };
        static List<string> NonReplicaServers = new List<string>() { "dbtsouthstorage", "dbtsouthstorage-secondary", "dbtbrazilstorage", "dbteastasiastorage", "dbtjapanweststorage" };
        static List<string> ReadOnlySecondaryServers = new List<string>() { "dbteuropestorage-secondary", "dbtwestusstorage-secondary", "dbtsouthstorage-secondary", "dbtjapanweststorage-secondary" };
        */

        // For Paris demo 
        /*
        static List<string> PrimaryServers = new List<string>() { "dbtwestusstorage" };
        static List<string> SecondaryServers = new List<string>() { "dbteuropestorage", "dbteuropestorage-secondary", "dbtwestusstorage-secondary" };
        static List<string> NonReplicaServers = new List<string>() { "dbtsouthstorage", "dbtsouthstorage-secondary", "dbtbrazilstorage", "dbteastasiastorage", "dbtjapanweststorage" };
        static List<string> ReadOnlySecondaryServers = new List<string>() { "dbteuropestorage-secondary", "dbtwestusstorage-secondary", "dbtsouthstorage-secondary", "dbtjapanweststorage-secondary" };
        */

        static CapCloudBlobContainer container;

        static Dictionary<string, CloudStorageAccount> accounts;
        static ReplicaConfiguration config;

        static Configurator configurator;
        static List<ConfigurationAction> proposedActions;

        static Dictionary<string, ServiceLevelAgreement> slas;
        static ServiceLevelAgreement currentSLA;

        static Sampler sampler;

        static int opsSinceSync = 0;
        static bool startExperiment = false;

        static bool reloadDatabase = true;  // should be false 
        static bool clearConfiguration = true;  // should be false unless the first time
        static bool cloudBackedConfiguration = false;  // could be either true or false for demo
        static bool stableConfiguration = true;  // should be set to true when running demo

        static bool readDataFromFile = false;  // should  be true, except when the initial config changes
        static bool saveDataToFile = true;  // should be true
        static string samplerFileName = "DemoData.txt";

        #endregion


        #region Initialization

        public static void Initialize()
        {
            // get list of Azure storage accounts
            accounts = PileusApp.Account.GetStorageAccounts(false);
            ClientRegistry.Init(accounts, accounts[configStorageSite]);

            // delete cloud configuration if desired
            if (clearConfiguration)
            {
                ReplicaConfiguration configToDelete = new ReplicaConfiguration(containerName);
                ConfigurationCloudStore backingStore = new ConfigurationCloudStore(accounts[configStorageSite], configToDelete);
                Log("Deleting configuration in Azure...");
                backingStore.DeleteConfiguration();
                Log("Done.  Now waiting for it to really take effect...");
                Thread.Sleep(40000);  // give system a chance to complete delete
            }

            // read/create configuration 
            Log("Creating/reading configuration...");
            config = new ReplicaConfiguration(containerName, PrimaryServers, SecondaryServers, NonReplicaServers, ReadOnlySecondaryServers, cloudBackedConfiguration, stableConfiguration);
            ClientRegistry.AddConfiguration(config);

            // upload data into containers if desired
            if (reloadDatabase)
            {
                CreateDatabase(numBlobs);
            }

            // create SLAs
            slas = new Dictionary<string, ServiceLevelAgreement>();
            slas["strong"] = CreateConsistencySla(Consistency.Strong);
            slas["causal"] = CreateConsistencySla(Consistency.Causal);
            slas["bounded"] = CreateBoundedConsistencySla(boundInSeconds);
            slas["readmywrites"] = CreateConsistencySla(Consistency.ReadMyWrites);
            slas["monotonic"] = CreateConsistencySla(Consistency.MonotonicReads);
            slas["eventual"] = CreateConsistencySla(Consistency.Eventual);
            currentSLA = CreateFastOrStrongSla();
            slas["sla"] = currentSLA;

            // get/create replicated container
            Log("Creating replicated container...");
            Dictionary<string, CloudBlobContainer> containers = new Dictionary<string, CloudBlobContainer>();
            foreach (string site in accounts.Keys)
            {
                CloudBlobClient blobClient = accounts[site].CreateCloudBlobClient();
                CloudBlobContainer blobContainer = blobClient.GetContainerReference(containerName);
                blobContainer.CreateIfNotExists();
                containers.Add(site, blobContainer);
            }
            container = new CapCloudBlobContainer(containers, PrimaryServers.First());
            container.Configuration = config;
            container.Monitor = new ServerMonitor(config);

            configurator  = new Configurator(containerName);
        }

        public static void CreateDatabase(int numBlobs)
        {
            foreach (string site in PrimaryServers)
            {
                YCSBClientLoader.UploadDataToSite(numBlobs, 1024, site, containerName);
            }
            foreach (string site in SecondaryServers)
            {
                if (!ReadOnlySecondaryServers.Contains(site))
                {
                    YCSBClientLoader.UploadDataToSite(numBlobs, 1024, site, containerName);
                }
            }
            foreach (string site in NonReplicaServers)
            {
                if (!ReadOnlySecondaryServers.Contains(site))
                {
                    YCSBClientLoader.UploadDataToSite(numBlobs, 1024, site, containerName);
                }
            }
        }

        #endregion

        #region Configuration

        public static ReplicaConfiguration GetCurrentConfiguration()
        {
            return config;
        }

        public static void SetCurrentConfiguration(ReplicaConfiguration newConfig)
        {
            config = newConfig;
            slas["sla"].ResetHitsAndMisses();
        }

        public static void SetInitialConfiguration()
        {
            // Note: This is not the correct way to change the configuration.  We should actually use the Configurator.

            // end current epoch
            config.EndCurrentEpoch();
            
            // restore to original primary and secondary
            config.PrimaryServers = PrimaryServers.ToList();
            config.SecondaryServers = SecondaryServers.ToList();
            config.ReadOnlySecondaryServers = ReadOnlySecondaryServers.ToList();
            config.NonReplicaServers = NonReplicaServers.ToList();

            // start new epoch and upload to cloud
            config.StartNewEpoch();

            slas["sla"].ResetHitsAndMisses();
        }

        public static void ProposeNewConfiguration() 
        {
            List<ConfigurationConstraint> constraints = new List<ConfigurationConstraint>();
            // for now, we have no constraints...
            // constraints.Add(new LocationConstraint(containerName, "dbtwestusstorage", LocationConstraintType.Replicate));
            // constraints.Add(new ReplicationFactorConstraint(containerName, 1, 2));

            proposedActions = configurator.PickNewConfiguration(containerName, slas["sla"], container.Sessions["sla"], container.Monitor, config, constraints);
        }

        public static void InstallNewConfiguration()
        {
            if (proposedActions == null || proposedActions.Count == 0)
            {
                return;  // nothing to do
            }
            configurator.InstallNewConfiguration(proposedActions);
            slas["sla"].ResetHitsAndMisses();
        }

        #endregion


        #region Reads, Writes, Pings, and Syncs

        public static Sampler NewSampler()
        {
            Sampler sampler = YCSBClientExecutor.NewSampler();
            foreach (string cons in slas.Keys)
            {
                sampler.AddSampleName(cons + "Latency", Sampler.OutputType.Average);
                sampler.AddSampleName(cons + "PrimaryAccesses", Sampler.OutputType.Total);
                sampler.AddSampleName(cons + "TotalAccesses", Sampler.OutputType.Total);
            }
            return sampler;
        }

        public static Sampler PerformReadsWritesSyncs (Sampler reuseSampler = null, bool recon = false)
        {
            YCSBWorkload workload = new YCSBWorkload(YCSBWorkloadType.Workload_a, numBlobsToUse);

            sampler = reuseSampler;
            if (sampler == null)
            {
                sampler = NewSampler();
            }

            byte[] BlobDataBuffer = new byte[1024];
            Random random = new Random();
            random.NextBytes(BlobDataBuffer);
           
            // Synchronize before we start the experiment
            if (startExperiment == true)
            {
                Log("Syncing data to secondary replicas...");
                SyncSecondaryServers();
                startExperiment = false;
            }

            for (int ops = 0; ops < numReadWrites; ops++)
            {
                YCSBOperation op = workload.GetNextOperation();
                if (op == null)
                {
                    // exhasted trace so get new one
                    workload = new YCSBWorkload(YCSBWorkloadType.Workload_a, numBlobsToUse);
                    op = workload.GetNextOperation();
                }

                long duration = 0;
                if (op.Type == YCSBOperationType.READ)
                {
                    // HACK: warm up the primary server cache the first time we access it
                    // Reason: Strong pays higher latency compared to later consistencies in "containers" because of cold misses.
                    //duration = YCSBClientExecutor.GetBlob(op.KeyName, containers["eventual"]);
                    //duration = YCSBClientExecutor.GetBlob(op.KeyName, containers["strong"]);

                    List<string> consInRandomOrder = slas.Keys.OrderBy(el => random.Next()).ToList();
                    foreach (string cons in consInRandomOrder)
                    {
                        CapCloudBlob blob = (CapCloudBlob) container.GetBlobReference(op.KeyName, cons);
                        blob.Sla = slas[cons];
                        // executing GetBlob twice substantially reduces the latency variance
                        //duration = YCSBClientExecutor.GetBlob(blob);
                        duration = YCSBClientExecutor.GetBlob(blob);
                        sampler.AddSample(cons + "Latency", duration);
                        sampler.AddSample(cons + "TotalAccesses", 1);
                        if (config.PrimaryServers.Contains(blob.engine.chosenServer.Name))
                        {
                            sampler.AddSample(cons + "PrimaryAccesses", 1);
                        }
                        Log("Performed " + cons + " read for " + op.KeyName + " in " + duration + " from " + SiteName(blob.engine.chosenServer.Name));
                        //AppendDataFile(duration);
                        if (cons == "sla")
                        {
                            foreach (SubSLA sub in slas[cons]) {
                                Log("SLA: " + sub.Consistency + " hits=" + sub.NumberOfHits + " misses=" + sub.NumberOfMisses);
                            }
                        }
                    }
                    sampler.AddSample("ReadCount", 1);
                }
                else if (op.Type == YCSBOperationType.UPDATE)
                {
                    random.NextBytes(BlobDataBuffer);
                    
                    // use write with multiple sessions to avoid duplicate writes to primary
                    List<SessionState> sessions = new List<SessionState>();
                    foreach (string cons in slas.Keys)
                    {
                        sessions.Add(container.GetSession(cons));
                    }
                    ReadWriteFramework protocol = new ReadWriteFramework(op.KeyName, config, null);
                    Stopwatch watch = new Stopwatch();
                    using (var ms = new MemoryStream(BlobDataBuffer))
                    {
                        AccessCondition ac = AccessCondition.GenerateEmptyCondition();
                        watch.Start();
                        protocol.Write(blob => blob.UploadFromStream(ms, ac), ac, sessions, container.Monitor);
                    }
                    duration = watch.ElapsedMilliseconds;

                    /*  
                    foreach (string cons in slas.Keys)
                    {
                        ICloudBlob blob = container.GetBlobReference(op.KeyName, cons);
                        duration = YCSBClientExecutor.PutBlob(blob, BlobDataBuffer);
                        Log("Performed " + cons + " write for " + op.KeyName + " in " + duration); 
                    }
                    */

                    Log("Performed " + " write for " + op.KeyName + " in " + duration);
                    sampler.AddSample("WriteCount", 1);
                    sampler.AddSample("WriteLatency", duration);
                }

                if (++opsSinceSync >= opsBetweenSyncs)
                {
                    Log("Syncing data to secondary replicas...");
                    SyncSecondaryServers();
                    opsSinceSync = 0;
                }
            }
            return sampler;
        }

        public static void PingAllServers()
        {
            List<string> allServers = config.GetServers();
            Stopwatch watch = new Stopwatch();
            for (int pingCount = 0; pingCount < 5; pingCount++)
            {
                foreach (string server in allServers)
                {
                    CloudBlobClient blobClient = ClientRegistry.GetCloudBlobClient(server);
                    if (blobClient != null)
                    {
                        CloudBlobContainer blobContainer = blobClient.GetContainerReference(containerName);
                        //Log("Pinging " + SiteName(server) + " aka " + server + "...");
                        watch.Restart();
                        //we perform a dummy operation to get the rtt latency!
                        try
                        {
                            bool ok = blobContainer.Exists();
                        }
                        catch (StorageException se)
                        {
                            Log("Storage exception when pinging " + SiteName(server) + ": " + se.Message);
                        }
                        long el = watch.ElapsedMilliseconds;
                        ServerState ss = container.Monitor.GetServerState(server);
                        ss.AddRtt(el);
                        //Log("Pinged " + SiteName(server) + " in " + el + " milliseconds");
                    }
                    else
                    {
                        Log("Failed to ping " + SiteName(server));
                    }
                }
            }
        }

        public static ServerMonitor GetServerMonitor()
        {
            return container.Monitor;
        }

        private static void SyncSecondaryServers()
        {
            string primarySite = config.PrimaryServers.First();
            foreach (string secondarySite in config.SecondaryServers)
            {
                if (!config.ReadOnlySecondaryServers.Contains(secondarySite))
                {
                    SynchronizeContainer synchronizer = new SynchronizeContainer(ClientRegistry.GetCloudBlobContainer(primarySite, containerName),
                        ClientRegistry.GetCloudBlobContainer(secondarySite, containerName));
                    synchronizer.SyncContainers();
                    Log("Synchronization completed to " + SiteName(secondarySite) + ".");
                }
                else
                {
                    Log("Could not sync to " + SiteName(secondarySite) + " since secondary is read-only.");
                }
            }
        }

        #endregion

        
        #region SLAs

        public static ServiceLevelAgreement CreateShoppingCartSla()
        {
            ServiceLevelAgreement sla = new ServiceLevelAgreement("Shopping Cart");
            SubSLA subSla1 = new SubSLA(300, Consistency.Strong, 0, 1);
            SubSLA subSla2 = new SubSLA(300, Consistency.ReadMyWrites, 0, 0.75f);
            SubSLA subSla3 = new SubSLA(300, Consistency.Eventual, 0, 0.5f);
            SubSLA subSla4 = new SubSLA(800, Consistency.Eventual, 0, 0.25f);
            sla.Add(subSla1);
            sla.Add(subSla2);
            sla.Add(subSla3);
            sla.Add(subSla4);
            return sla;
        }

        public static ServiceLevelAgreement CreateFastOrStrongSla()
        {
            ServiceLevelAgreement sla = new ServiceLevelAgreement("Fast or Strong");
            SubSLA subSla1 = new SubSLA(100, Consistency.Strong, 0, 1);
            SubSLA subSla2 = new SubSLA(100, Consistency.Bounded, boundInSeconds, 0.8f);
            SubSLA subSla3 = new SubSLA(100, Consistency.Eventual, 0, 0.5f);
            SubSLA subSla4 = new SubSLA(2000, Consistency.Strong, 0, 0.25f);
            sla.Add(subSla1);
            sla.Add(subSla2);
            sla.Add(subSla3);
            sla.Add(subSla4);
            return sla;
        }

        /// <summary>
        /// Creates a simple SLA with a single desired consistency and a large latency.
        /// This forces reads to be performed at the closest replica with that consistency.
        /// </summary>
        /// <param name="cons"></param>
        /// <returns></returns>
        public static ServiceLevelAgreement CreateConsistencySla(Consistency cons)
        {
            ServiceLevelAgreement sla = new ServiceLevelAgreement(cons.ToString("g"));
            SubSLA subSla1 = new SubSLA(2000, cons, 0, 1);
            sla.Add(subSla1);
            return sla;
        }

        public static ServiceLevelAgreement CreateConsistencySla(Consistency cons, int latency, string Name)
        {
            ServiceLevelAgreement sla = new ServiceLevelAgreement(Name);
            SubSLA subSla1 = new SubSLA(latency, cons, 0, 1);
            sla.Add(subSla1);
            return sla;
        }

        public static ServiceLevelAgreement CreateBoundedConsistencySla(int bound)
        {
            ServiceLevelAgreement sla = new ServiceLevelAgreement("Bounded");
            SubSLA subSla1 = new SubSLA(2000, Consistency.Bounded, bound, 1);
            sla.Add(subSla1);
            return sla;
        }

        public static ServiceLevelAgreement GetCurrentSLA()
        {
            return currentSLA;
        }

        public static void SetCurrentSLA(string name)
        {
            switch (name)
            {
                case "Fast or Strong":
                    currentSLA = CreateFastOrStrongSla();
                    slas["sla"] = currentSLA;
                    break;
                case "Shopping Cart":
                    currentSLA = CreateShoppingCartSla();
                    slas["sla"] = currentSLA;
                    break;
                default:
                    // do nothing
                    break;
            }
        }

        public static float GetCurrentSLAUtility()
        {
            return currentSLA.GetAverageDeliveredUtility();
        }

        #endregion


        #region Logging Routines

        public delegate void Logger(string s);

        // private static Logger logger = null;
        private static Logger logger = null;

        // private static void Log(string s)
        public static void Log(string s)
        {
            if (logger != null)
            {
                logger(s);
            }
        }

        public static void RegisterLogger(Logger _logger)
        {
            logger = _logger;
        }
        
        public static string PrintCurrentConfiguration()
        {
            string result = null;
            ReplicaConfiguration config = ClientRegistry.GetConfiguration(containerName, false);
            result += "Current configuration for " + config.Name + ":" + "\r\n";
            result += "Primary: ";
            bool first = true;
            foreach (string name in config.PrimaryServers)
            {
                if (first)
                {
                    first = false;
                }
                else
                {
                    result += ", ";
                }
                result += SiteName(name);
            };
            result += "\r\n";
            result += "Secondaries: ";
            first = true;
            foreach (string name in config.SecondaryServers)
            {
                if (first)
                {
                    first = false;
                }
                else
                {
                    result += ", ";
                }
                result += SiteName(name);
            };
            result += "\r\n";
            return result;
        }

        public static string PrintReadWriteTimes(Sampler sampler)
        {
            string result = "";
            if (sampler.GetAllSampleValues("strongLatency") != null)
            {
                result += "Number of reads = " + sampler.GetAllSampleValues("strongLatency").Count + "\r\n";
                foreach (string cons in slas.Keys)
                {
                    result += cons + " read: " + sampler.GetSampleValue(cons + "Latency") + "\r\n";
                }
            }
            if (sampler.GetAllSampleValues("WriteLatency") != null)
            {
                result += "Number of writes = " + sampler.GetAllSampleValues("WriteLatency").Count + "\r\n";
                result += "write: " + sampler.GetSampleValue("WriteLatency");
            }
            return result;
        }

        public static string PrintServerRTTs()
        {
            string result = null;
            List<string> allServers = config.GetServers();
            foreach (string server in allServers)
            {
                ServerState ss = container.Monitor.GetServerState(server);
                result += SiteName(server) + " avg latency: " + ss.RTTs.FindAverage() + "\r\n";
            }
            return result;
        }

        public static string PrintReconfigurationActions()
        {
            string result = null;
            if (proposedActions == null || proposedActions.Count == 0)
            {
                result = "No new configuration is suggested at this time.";
            }
            else
            {
                foreach (ConfigurationAction act in proposedActions)
                {
                    result += "Chosen action for " + act.ModifyingContainer.Name.ToString() + ": \r\n";
                    result += ReconfigActionText(act) + "\r\n";
                }
                result += "\r\n";
                float gain = configurator.ComputeUtilityGainFromNewConfiguration(containerName, slas["sla"], container.Sessions["sla"], container.Monitor, config, proposedActions);
                result += "expected utility gain is " + gain.ToString("F2") + "\r\n";
                result += "expected cost is " + proposedActions.First().Cost.ToString("F2") + "\r\n";
            }
            return result;
        }

        private static string ReconfigActionText(ConfigurationAction act)
        {
            string text;
            switch (act.GetType().Name)
            {
                case "MakeSoloPrimaryServer": text = "Move the primary site to " + SiteName(act.ServerName); break;
                case "AddPrimaryServer": text = "Add a primary replica in " + SiteName(act.ServerName); break;
                case "AddSecondaryServer": text = "Add a secondary replica in " + SiteName(act.ServerName); break;
                case "AdjustSyncPeriod": text = "Reduce the sync period for site " + SiteName(act.ServerName); break;
                case "DowngradePrimary": text = "Downgrade from primary to secondary site " + SiteName(act.ServerName); break;
                case "RemoveSecondaryServer": text = "Remove the secondary replica in " + SiteName(act.ServerName); break;
                default: text = act.GetType().Name + " " + SiteName(act.ServerName); break;
            }
            return text;

        }

        public static string SiteName(string server)
        {
            string site;
            switch (server)
            {
                case "dbteuropestorage": site = "West Europe"; break;
                case "dbtwestusstorage": site = "West US"; break;
                case "dbtsouthstorage": site = "South US"; break;
                case "dbteuropestorage-secondary": site = "North Europe"; break;
                case "dbtwestusstorage-secondary": site = "East US"; break;
                case "dbtsouthstorage-secondary": site = "North US"; break;
                case "dbteastasiastorage": site = "East Asia"; break;
                case "dbtbrazilstorage": site = "Brazil"; break;
                case "dbtjapanweststorage": site = "West Japan"; break;
                case "dbtjapanweststorage-secondary": site = "East Japan"; break;
                default: site = server; break;
            }
           return site;
        }

        public static string ServerName(string site)
        {
            string name;
            switch (site)
            {
                case "West Europe": name = "dbteuropestorage"; break;
                case "West US": name = "dbtwestusstorage"; break;
                case "South US": name = "dbtsouthstorage"; break;
                case "North Europe": name = "dbteuropestorage-secondary"; break;
                case "East US": name = "dbtwestusstorage-secondary"; break;
                case "North US": name = "dbtsouthstorage-secondary"; break;
                case "East Asia": name = "dbteastasiastorage"; break;
                case "Brazil": name = "dbtbrazilstorage"; break;
                case "West Japan": name = "dbtjapanweststorage"; break;
                case "East Japan": name = "dbtjapanweststorage-secondary"; break;
                default: name = site; break;
            }
            return name;
        }

        #endregion


        #region Reading and Writing Files

        public static void ReadDataFile(Sampler sampler) 
        {
            int lines = 0;
            bool endOfFile = false;
            if (readDataFromFile && File.Exists(samplerFileName))
            {
                using (TextReader file = File.OpenText(samplerFileName))
                {
                    do
                    {
                        foreach (string cons in slas.Keys)
                        {
                            string line = file.ReadLine();
                            if (line == null)
                            {
                                endOfFile = true;
                            }
                            else
                            {
                                sampler.AddSample(cons + "Latency", float.Parse(line));
                                lines++;
                            }
                        }
                    }
                    while (!endOfFile);
                }
            }
            Log("Read " + lines + " samples");
        }

        public static void WriteDataFile(Sampler sampler)
        {
            using (TextWriter file = File.CreateText(samplerFileName))
            {
                Dictionary<string, float[]> samples = new Dictionary<string, float[]>();
                int numSamples = sampler.GetAllSampleValues("strongLatency").Count;
                foreach (string cons in slas.Keys)
                {
                    float[] values = sampler.GetAllSampleValues(cons + "Latency").ToArray();
                    samples.Add(cons, values);
                }
                for (int i = 0; i < numSamples; i++)
                {
                    foreach (string cons in slas.Keys)
                    {
                        file.WriteLine(samples[cons][i].ToString());
                    }
                }
            }
        }
        
        public static void AppendDataFile(float latency)
        {
            if (saveDataToFile)
            {
                using (TextWriter file = File.AppendText(samplerFileName))
                {
                    file.WriteLine(latency.ToString());
                }
            }
        }

        #endregion

    }
}
