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
using PileusApp.YCSB;
using System.Threading.Tasks;

namespace PileusApp.YCSB
{
    public class YCSBClientExecutor
    {
        #region Locals
        private static Sampler sampler;

        private static string containerName = null;
        private static string testSite;
        private static CloudBlobClient blobClient;
        private static CloudBlobContainer container;

        private static int numberOfClientAtPickTime;
        public static int parallelRequestsPerBlob = 1;
        public static bool useHttps = false;
        public static string resultFileFolderName;
        public static long blobSizeInB = 1;

        private static string resultFile;
        private static int concurrentWorkers = 0;

        private static YCSBWorkloadType workloadType;

        private static int sleepTimeBetweenTicks;
        private static int intervalLength;
        private static int totalExperimentLength;


        private static int currentTick;
        private static int concurrentClientsPerTick;
        private static int additionalCreatedClientsPerTick;


        private static int currentEmulationHour = 1;
        #endregion

        

        public static void Main(string[] args)
        {

            Console.CancelKeyPress += new ConsoleCancelEventHandler(StoreLog);

            Dictionary<string, Sampler.OutputType> sampleNames = new Dictionary<string, Sampler.OutputType>();
            sampleNames["Utility"] = Sampler.OutputType.Average;
            sampleNames["concurrentWorkers"] = Sampler.OutputType.Average;
            sampleNames["ReadLatency"] = Sampler.OutputType.Average;
            sampleNames["WriteLatency"] = Sampler.OutputType.Average;
            sampleNames["ReadCount"] = Sampler.OutputType.Total;
            sampleNames["WriteCount"] = Sampler.OutputType.Total;

            sampler = new Sampler(true, sampleNames, GetEmulationTime);

            if (args.Length == 0)
            {
                args = new string[11];

                // the result folder name
                args[0] = "folder1";

                // workload type
                args[1] = "b";

                // number of clients @ pick time
                args[2] = "15"; 

                // the blob size in Byte
                args[3] = "1024";

                // use https or not
                args[4] = "false";

                //sleep time between ticks in milliseconds. 
                args[5] = "90000";

                //interval length in ticks
                args[6] = "24";

                //duration of experiment in ticks
                args[7] = "24";

                //storage account locating configuration of given container
                args[8] = "dbtsouthstorage"; // "devstoreaccount1";

                //container  name
                args[9] = "testcontainer";

                //emulation start time at UTC
                args[10] = "9";
            }

            resultFileFolderName = args[0];
            if (args[1].Equals("a"))
            {
                workloadType = YCSBWorkloadType.Workload_a;
            }
            else if (args[1].Equals("b"))
            {
                workloadType = YCSBWorkloadType.Workload_b;
            }
            else if (args[1].Equals("c"))
            {
                workloadType = YCSBWorkloadType.Workload_c;
            }
            else
            {
                throw new Exception("Unkown workload");
            }
            numberOfClientAtPickTime = Int32.Parse(args[2]);
            blobSizeInB = Int32.Parse(args[3]);
            useHttps = bool.Parse(args[4]);

            sleepTimeBetweenTicks = Int32.Parse(args[5]);
            intervalLength = Int32.Parse(args[6]);
            totalExperimentLength = Int32.Parse(args[7]);

            testSite = args[8];
            containerName = args[9];

            currentEmulationHour = Int32.Parse(args[10]);

            if (!Directory.Exists(resultFileFolderName))
            {
                Directory.CreateDirectory(resultFileFolderName);
            }

            resultFile = string.Format(@"{5}\{6}_{0}_{1}_{2}_{3}_{4}.csv", blobSizeInB, parallelRequestsPerBlob, numberOfClientAtPickTime, useHttps,  workloadType, resultFileFolderName, Dns.GetHostName());

            ClientDistribution distribution = new ClientDistribution(intervalLength, intervalLength / 2, 4);
            double probabilityMean = distribution.GetNextProbability(intervalLength / 2);


            #region Adjust times to local times

            //VM times are all in UTC, so we need to hardcode timing.
            int localTime;

            if (Dns.GetHostName().Contains("westus"))
            {
                localTime = currentEmulationHour - 7;
            }
            else if (Dns.GetHostName().Contains("westeurope"))
            {
                localTime = currentEmulationHour + 1;
            }
            else if (Dns.GetHostName().Contains("eastasia"))
            {
                localTime = currentEmulationHour + 9;
            }
            else
            {
                localTime = currentEmulationHour - 7;
            }
            #endregion

            currentTick = localTime % intervalLength;
            totalExperimentLength += currentTick;

            //decreament by one. Increament inside while again.
            //Note that you cannot simply decreament at the end of while loop because samples will be saved with a wrong hour.
            currentEmulationHour--;
            Console.WriteLine("Starting Tick : " + currentTick  +  "   @  " + Dns.GetHostName());
            while (currentTick < totalExperimentLength)
            {
                currentEmulationHour++;
                double prob = (distribution.GetNextProbability(currentTick) * numberOfClientAtPickTime) / probabilityMean;
                concurrentClientsPerTick = Convert.ToInt32(prob);
                additionalCreatedClientsPerTick = 0;
                if (concurrentClientsPerTick == 0) concurrentClientsPerTick = 1;
                Console.WriteLine(">>>>>>>>>>>>>>>>TICK IS : " + currentTick + " <<<<<<<<<<<<<<<<" + " REQUIRED CLIENTS: " + concurrentClientsPerTick);
                for (int m = 0; m < (concurrentClientsPerTick - concurrentWorkers); m++)
                {
                    Task.Factory.StartNew(() => { RunClient(currentTick); });
                }
                currentTick++;
                Thread.Sleep(sleepTimeBetweenTicks);
            }

            //Execution is done. Wait until all client return.
            while (Interlocked.CompareExchange(ref concurrentWorkers, -1, 0) != -1)
            {
                Console.WriteLine("Waiting for a thread because thread are " + concurrentWorkers + " threads.");
                Thread.Sleep(5000);
            }

            StoreLog(null,null);

            return ;
        }

        
        /*
        public static ServiceLevelAgreement CreateShoppingCartSla1()
        {
            ServiceLevelAgreement sla = new ServiceLevelAgreement(""+1);
            SubSLA subSla1 = new SubSLA(130, Consistency.Strong, 0, 1);
            SubSLA subSla2 = new SubSLA(130, Consistency.ReadMyWrites, 0, .70f);
            SubSLA subSla3 = new SubSLA(250, Consistency.Eventual, 0, 0.5f);
            SubSLA subSla4 = new SubSLA(500, Consistency.Eventual, 0, 0.05f);
            sla.Add(subSla1);
            sla.Add(subSla2);
            sla.Add(subSla3);
            sla.Add(subSla4);
            return sla;
        }
        */


        #region Client

        public static void RunClient(int tick)
        {
            Interlocked.Increment(ref concurrentWorkers);
            byte[] BlobDataBuffer = new byte[blobSizeInB];
            Random random = new Random();
            random.NextBytes(BlobDataBuffer);

            Dictionary<string, CloudStorageAccount> accounts = Account.GetStorageAccounts(false);
            blobClient = accounts[testSite].CreateCloudBlobClient();
            container = blobClient.GetContainerReference(containerName);
            container.CreateIfNotExists();

            try
            {
                YCSBWorkload workload = new YCSBWorkload(workloadType, 100);

                YCSBOperation op = workload.GetNextOperation();
                while (op != null)
                {
                    long duration = 0;
                    ICloudBlob blob = container.GetBlobReferenceFromServer(op.KeyName);
                    
                    if (op.Type == YCSBOperationType.READ)
                    {
                        duration = GetBlob(blob);
                        if (PileusAppConstPool.ENABLE_CLIENT_READWRITE_OUTPUT)
                            Console.WriteLine("Performed Read for " + op.KeyName + " in " + duration);
                        sampler.AddSample("ReadCount", 1);
                        sampler.AddSample("ReadLatency", duration);
                        sampler.AddSample("concurrentWorkers", concurrentWorkers);
                    }
                    else if (op.Type == YCSBOperationType.UPDATE)
                    {
                        random.NextBytes(BlobDataBuffer);
                        duration = PutBlob(blob, BlobDataBuffer);
                        if (PileusAppConstPool.ENABLE_CLIENT_READWRITE_OUTPUT)
                            Console.WriteLine("Performed Write for " + op.KeyName + " in " + duration);
                        sampler.AddSample("WriteCount", 1);
                        sampler.AddSample("WriteLatency", duration);
                        sampler.AddSample("concurrentWorkers", concurrentWorkers);
                    }

                    op = workload.GetNextOperation();
                }
                if (PileusAppConstPool.ENABLE_CLIENT_READWRITE_OUTPUT)
                    Console.WriteLine("Client Finished ...\n");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.StackTrace);
            }


            Interlocked.Decrement(ref concurrentWorkers);

            if (((concurrentClientsPerTick - 1) >= concurrentWorkers) && (currentTick < totalExperimentLength))
            {
                //We want to keep total number of concurrent workers (i.e., concurrentWorkers) equals to "concurrentClientsPerTick".
                additionalCreatedClientsPerTick++;
                RunClient(currentTick);
            }

        }

        public static long GetBlob(ICloudBlob blob)
        {
            Stopwatch watch = new Stopwatch();
            watch.Start();
            using (MemoryStream ms = new MemoryStream())
            {
                blob.DownloadToStream(ms);
            }
            watch.Stop();
            return watch.ElapsedMilliseconds;
        }

        public static long PutBlob(ICloudBlob blob, byte[] data)
        {
            Stopwatch watch = new Stopwatch();
            watch.Start();
            using (var ms = new MemoryStream(data))
            {
                blob.UploadFromStream(ms);
            }
            return watch.ElapsedMilliseconds;
        }

        public static long DeleteBlob(ICloudBlob blob)
        {
            Stopwatch watch = new Stopwatch();
            watch.Start();
            blob.Delete();
            watch.Stop();
            return watch.ElapsedMilliseconds;
        }

        #endregion

        public static void ChosenUtility(float utility)
        {
            sampler.AddSample("Utility", utility);
        }

        /// <summary>
        /// This function is for the emulation purposes. 
        /// </summary>
        /// <returns></returns>
        public static int ActualNumberOfClients()
        {
            if (concurrentClientsPerTick == 0)
                return 1;
            return concurrentClientsPerTick;
        }

        protected static void StoreLog(object sender, ConsoleCancelEventArgs args)
        {
            Process currProc = Process.GetCurrentProcess();

            Console.WriteLine("All clients finished execution.");

            using (StreamWriter sw = new StreamWriter(resultFile))
            {
                sw.Write(String.Format("Args:Concurrency: {0} BlobSizeInKB:{1} ParallelRequestsPerBlob:{2} UsingHttps:{3} \n", numberOfClientAtPickTime, blobSizeInB, parallelRequestsPerBlob, useHttps));

                /*
                float tmp = 0;
                foreach (ConsistencySLAEngine engine in CapCloudBlobClient.slaEngines[containerName])
                {
                    foreach (SubSLA s in engine.Sla)
                    {
                        tmp += s.Utility * s.NumberOfHits;
                    }

                }
                sw.Write(String.Format("Current utility ", tmp));
                */

                // Display perf results
                PerfMetrics metrics = new PerfMetrics();
                metrics.Update(currProc);
                metrics.Print(sw);

                /*
                foreach (ConsistencySLAEngine engine in CapCloudBlobClient.slaEngines[containerName])
                {
                    Console.WriteLine("Average utility/seconds for SLA " + engine.Sla.Id);
                    sw.Write("Average utility/seconds for SLA " + engine.Sla.Id + "\n \n");
                    string samples = sampler.ToString();
                    Console.WriteLine(samples);
                    sw.Write(samples);
                }
                */
            }
        }


        /// <summary>
        /// The granularity that samples are stored in the Sampler. 
        /// 
        /// Example: if it returns the current second, the Sampler will store samples with the granularity of seconds. 
        /// Now, it is storing samples with the emulation hour granularity.
        /// </summary>
        /// <returns></returns>
        public static int GetEmulationTime()
        {
            return currentEmulationHour;
        }

        public static Sampler NewSampler()
        {
            Dictionary<string, Sampler.OutputType> sampleNames = new Dictionary<string, Sampler.OutputType>();
            sampleNames["Utility"] = Sampler.OutputType.Average;
            sampleNames["concurrentWorkers"] = Sampler.OutputType.Average;
            sampleNames["ReadLatency"] = Sampler.OutputType.Average;
            sampleNames["WriteLatency"] = Sampler.OutputType.Average;
            sampleNames["ReadCount"] = Sampler.OutputType.Total;
            sampleNames["WriteCount"] = Sampler.OutputType.Total;

            sampler = new Sampler(false, sampleNames, GetEmulationTime);
            return sampler;
        }
    }

}