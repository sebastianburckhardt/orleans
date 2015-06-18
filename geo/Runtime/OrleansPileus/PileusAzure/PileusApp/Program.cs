using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Threading;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Core;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using Microsoft.WindowsAzure.Storage.Pileus;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage.Pileus.Configuration;
using Microsoft.WindowsAzure.Storage.Pileus.Utils;
using Microsoft.WindowsAzure.Storage.Pileus.Configuration.Constraint;
using PileusApp.Utils;

namespace PileusApp
{
    public class Program
    {
        private static bool firstClient = true;

        #region Locals
        private static string containerName = null;
        private static CloudBlobClient blobClient;
        private static CloudBlobContainer blobContainer;
        private static CapCloudBlobContainer capContainer;

        private static double blobs = 1;
        private static int iterations = 1;
        private static int concurrency = 1;
        public static int parallelRequestsPerBlob = 1;
        public static bool useHttps = false;
        public static string resultFileFolderName;
        public static long blobSizeInKB = 1;

        private static string resultFile;
        private static List<long> insertTimes = new List<long>();
        private static List<long> getTimes = new List<long>();
        private static List<long> deleteTimes = new List<long>();
        private static int concurrentWorkers = 0;
        public static byte[] BlobDataBuffer;

        public static Dictionary<string, CloudStorageAccount> accounts;
        private static string configAccountName = "devstoreaccount1";  // "devstoreaccount1"

        private static ReplicaConfiguration configuration;        
        private static ConsistencySLAEngine slaEngine;
        #endregion

        public static void Main(string[] args)
        {
            Process currProc = Process.GetCurrentProcess();

            containerName = "testcontainer"; //Guid.NewGuid().ToString("N").ToLower();

            if (args.Length == 0)
            {
                args = new string[7];

                // the number of test iterations
                args[0] = "1";

                // the number of blobs
                args[1] = "10";

                // the number of concurrent test workers.
                args[2] = "2"; 

                // the blob size in KB
                args[3] = "1024";

                // the number of parallel requests per blob
                args[4] = "1";

                // use https or not
                args[5] = "false";

                // the result folder name
                args[6] = "folder1";
            }

            iterations = Int32.Parse(args[0]);
            blobs = Int32.Parse(args[1]);
            concurrency = Int32.Parse(args[2]);
            blobSizeInKB = Int32.Parse(args[3]);
            parallelRequestsPerBlob = Int32.Parse(args[4]);
            useHttps = bool.Parse(args[5]);
            resultFileFolderName = args[6];

            if (!Directory.Exists(resultFileFolderName))
            {
                Directory.CreateDirectory(resultFileFolderName);
            }

            resultFile = string.Format(@"{6}\{0}_{1}_{2}_{3}_{4}_{5}.csv", blobSizeInKB, parallelRequestsPerBlob, concurrency, useHttps, iterations, blobs, resultFileFolderName);

            ThreadPool.SetMinThreads(concurrency * parallelRequestsPerBlob, concurrency * parallelRequestsPerBlob);

            accounts = Account.GetStorageAccounts(useHttps);
            ClientRegistry.Init(accounts, accounts[configAccountName]);

            configuration = new ReplicaConfiguration(containerName);
            ClientRegistry.AddConfiguration(configuration);

            if (firstClient)
            {
                // delete configuration blob and tables
                // ReplicaConfiguration.DeleteConfiguration(containerName);
                ConfigurationCloudStore backingStore = new ConfigurationCloudStore(accounts[configAccountName], configuration);
                backingStore.DeleteConfiguration();

                CloudTableClient ConfigurationCloudTableClient = accounts[configAccountName].CreateCloudTableClient();
                CloudTable slaTable = ConfigurationCloudTableClient.GetTableReference(ConstPool.SLA_CONFIGURATION_TABLE_NAME);
                slaTable.DeleteIfExists();
                slaTable = ConfigurationCloudTableClient.GetTableReference(ConstPool.SESSION_STATE_CONFIGURATION_TABLE_NAME);
                slaTable.DeleteIfExists();

                Console.WriteLine("removed everything, wait 40 seconds ...");
                Thread.Sleep(40000);
                
                // recreate configuration
                configuration.PrimaryServers.Add("devstoreaccount1");
                configuration.SyncWithCloud(accounts[configAccountName], false);
                Console.WriteLine("recreated configuration, wait 10 seconds ...");
                Thread.Sleep(10000);
            }
            else
            {
                // retrieve configuration from cloud
                configuration.SyncWithCloud(accounts[configAccountName]);
            }

            if (firstClient)
                slaEngine = new ConsistencySLAEngine(CreateShoppingCartSla1(), configuration);
            else
                slaEngine = new ConsistencySLAEngine(CreateShoppingCartSla2(), configuration);

            blobClient = accounts["devstoreaccount1"].CreateCloudBlobClient();
            blobContainer = blobClient.GetContainerReference(containerName);
            blobContainer.CreateIfNotExists();
            capContainer = new CapCloudBlobContainer(blobContainer);

            // Generate random data
            BlobDataBuffer = new byte[1024 * blobSizeInKB];
            Random random = new Random();
            random.NextBytes(BlobDataBuffer);

            //ServiceLevelAgreement sla = CreateShoppingCartSla();

            Stopwatch totalWatch = new Stopwatch();
            totalWatch.Start();
            for (int m = 0; m < concurrency; m++)
            {

                ThreadPool.QueueUserWorkItem((o) =>
                {                    
                    Interlocked.Increment(ref concurrentWorkers);
                    for (int i = 0; i < iterations; i++)
                    {
                        Console.WriteLine("Running thread " + m + "." + i);
                        Console.WriteLine("concurrent workers: " + concurrentWorkers);
                        try
                        {
                            // do upload blob test.
                            var blobsList = UploadBlob();
                            Console.WriteLine("Upload Finished ...\n");

                            // GET and DELETE.
                            DoGetAndDelete(blobsList);
                            Console.WriteLine("DoGetAndDelete Finished ...\n");
                            
                            configure("client", containerName);
                            Console.WriteLine("Configure Finished ...\n");
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e.ToString());
                        }
                    }


                    Interlocked.Decrement(ref concurrentWorkers);
                });
            }
            Console.WriteLine("Program: Started to sleep");
            Thread.Sleep(5000);
            while (Interlocked.CompareExchange(ref concurrentWorkers, -1, 0) != -1)
            {
                if (concurrentWorkers < 0) break;
                Console.WriteLine("Waiting for a thread because there are " + concurrentWorkers + " threads.");
                Thread.Sleep(5000);
            }

            Console.WriteLine("Finished execution. ");
            ClientRegistry.GetConfigurationContainer(containerName).Delete();
            blobContainer.DeleteIfExists();

            totalWatch.Stop();
            long totalTimeTaken = totalWatch.ElapsedMilliseconds;
            using (StreamWriter sw = new StreamWriter(resultFile))
            {
                sw.Write(String.Format("Total time taken to run the test in ms : {0}\n\n", totalTimeTaken));
                sw.Write(String.Format("Args:Concurrency: {0} BlobSizeInKB:{1} ParallelRequestsPerBlob:{2} UsingHttps:{3} \n", concurrency, blobSizeInKB, parallelRequestsPerBlob, useHttps));

                // display result
                DisplayResults(sw, "Insert", insertTimes);
                DisplayResults(sw, "Get", getTimes);
                DisplayResults(sw, "Delete", deleteTimes);

                float tmp = 0;
                foreach (SubSLA s in capContainer.SLA)
                {
                    tmp += s.Utility * s.NumberOfHits;
                }

                sw.Write(String.Format("Current utility ", tmp));


                // Display perf results
                PerfMetrics metrics = new PerfMetrics();
                metrics.Update(currProc);
                metrics.Print(sw);
            }

            Console.Read();
        }

        public static ServiceLevelAgreement CreateShoppingCartSla1()
        {
            ServiceLevelAgreement sla = new ServiceLevelAgreement(""+1);
            SubSLA subSla1 = new SubSLA(400, Consistency.Strong, 0, 1);
            SubSLA subSla2 = new SubSLA(420, Consistency.ReadMyWrites, 0, 1f);
            SubSLA subSla3 = new SubSLA(470, Consistency.Eventual, 0, 1f);
            SubSLA subSla4 = new SubSLA(800, Consistency.Eventual, 0, 0.9f);
            sla.Add(subSla1);
            sla.Add(subSla2);
            sla.Add(subSla3);
            sla.Add(subSla4);
            return sla;
        }

        public static ServiceLevelAgreement CreateShoppingCartSla2()
        {
            ServiceLevelAgreement sla = new ServiceLevelAgreement("" + 2);
            SubSLA subSla1 = new SubSLA(800, Consistency.Strong, 0, 1);
            SubSLA subSla2 = new SubSLA(920, Consistency.ReadMyWrites, 0, 1f);
            SubSLA subSla3 = new SubSLA(1070, Consistency.Eventual, 0, 1f);
            sla.Add(subSla1);
            sla.Add(subSla2);
            sla.Add(subSla3);
            return sla;
        }



        #region Get, put, delete blobs

        public static long GetBlob(string blobName, CapCloudBlobContainer cont)
        {
            Stopwatch watch = new Stopwatch();
            watch.Start();
            ICloudBlob blob = cont.GetBlobReference(blobName);
            using (MemoryStream ms = new MemoryStream())
            {
                blob.DownloadToStream(ms);
            }

            watch.Stop();
            return watch.ElapsedMilliseconds;
        }


        public static long PutBlob(string blobName, byte[] data, CapCloudBlobContainer cont)
        {
            Stopwatch watch = new Stopwatch();
            watch.Start();
            ICloudBlob blob = cont.GetBlobReference(blobName);

            using (var ms = new MemoryStream(data))
            {
                blob.UploadFromStream(ms);
            }
            
            watch.Stop();
            return watch.ElapsedMilliseconds;
        }


        public static long DeleteBlob(string blobName, CapCloudBlobContainer cont)
        {
            Stopwatch watch = new Stopwatch();
            watch.Start();

            ICloudBlob blob = cont.GetBlobReference(blobName);
            blob.Delete();

            watch.Stop();
            return watch.ElapsedMilliseconds;
        }


        #endregion

        #region Configurator
        public static void configure(string clientName, string containerName)
        {

            //UploadConfigurationTask task = new UploadConfigurationTask(clientName);
            //ReplicaConfiguration configuration = ClientRegistry.GetConfiguration(containerName, false);
            //task.sendConfigurationData(containerName, configuration.Epoch);


            //if (firstClient)
            //{
            //    Console.WriteLine("went to sleep");
            //    Thread.Sleep(90000000);
            //}


            Configurator conf = new Configurator(containerName);

            List<ConfigurationConstraint> constraints = new List<ConfigurationConstraint>();
            constraints.Add(new LocationConstraint(containerName, configuration, "dbtsouthstorage", LocationConstraintType.Replicate));
            constraints.Add(new ReplicationFactorConstraint(containerName, configuration, 1, 2));

            conf.Configure(accounts[configAccountName], configuration.Epoch, configuration, constraints);
        }
        #endregion

        #region Test methods

        public static List<string> UploadBlob()
        {
            var result = new List<string>();
            for (int i = 0; i < blobs; i++)
            {
                string blobName = Guid.NewGuid().ToString();
                long time = PutBlob(blobName, BlobDataBuffer, capContainer);
                result.Add(blobName);
                insertTimes.Add(time);
            }

           return result;
        }

        public static void DoGetAndDelete(List<string> blobList)
        {
            for (int i = 0; i < blobList.Count; i++)
            {
                long time = GetBlob(blobList[i], capContainer);
                getTimes.Add(time);

                time = DeleteBlob(blobList[i], capContainer);
                deleteTimes.Add(time);
            }
        }

        #endregion

        #region Utils

        public static Stopwatch StartTest(string testType)
        {
            // Start the test.
            Stopwatch watch = new Stopwatch();
            Console.WriteLine(testType + " starts... " + DateTime.Now.ToString());
            watch.Start();
            return watch;
        }

        public static long StopTest(Stopwatch watch)
        {
            // The test completes.
            Console.WriteLine("Test ends...   " + DateTime.Now.ToString());
            watch.Stop();
            return watch.ElapsedTicks;
        }

        public static void DisplayResults(StreamWriter sw, string name, List<long> data)
        {
            // Is there a better way to get the times in microseconds ?
            double microsecondspertick = 1000000 / (double)Stopwatch.Frequency;
            if (data.Count > 0)
            {
                long tVal = 0;
                long minVal = Int64.MaxValue;
                long maxVal = 0;
                
                foreach (long val in data)
                {
                    tVal += val;
                    if (val > maxVal)
                    {
                        maxVal = val;
                    }

                    if (val < minVal)
                    {
                        minVal = val;
                    }
                }

                tVal /= data.Count;

                sw.Write(String.Format("{0},,{1},{2},{3},\n", name, minVal * microsecondspertick, tVal * microsecondspertick, maxVal * microsecondspertick));

                Console.WriteLine(String.Format(
                        "Results for {0}: (MIN/AVG/MAX) in Microseconds: ({1}/{2}/{3})", name, minVal * microsecondspertick, tVal * microsecondspertick, maxVal * microsecondspertick));

            }
        }
        #endregion
    }

}