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

using Microsoft.WindowsAzure.Storage.Pileus.Configuration.Actions;

namespace PileusApp
{
    /// <summary>
    /// Executes an instance of Pileus Configurator
    /// Also execute a replicator.
    /// 
    /// An instance of <see cref=" Replicator.cs"/> is also initialized and started here.
    /// </summary>
    public class ConfiguratorExecutor
    {
        #region Locals
        private static string containerName = null;
        private static string configurationSite;
        public static string resultFileFolderName;

        private static int sleepTimeBetweenTicks;
        private static int ticksBetweenConfigurations;
        private static int experimentDurationInTicks;
        private static int startTickOfConfiguration;
        #endregion

        public static void Main(string[] args)
        {

            Console.CancelKeyPress += new ConsoleCancelEventHandler(Cancelled);

            if (args.Length == 0)
            {
                args = new string[7];

                //storage account locating configuration of given container
                args[0] = "dbtsouthstorage"; // "devstoreaccount1";

                // container name
                args[1] = "testcontainer";

                // the result folder name
                args[2] = "folder1";

                //sleep time between ticks in milliseconds. 
                args[3] = "90000";

                //interval between configuration
                args[4] = "2";

                //duration of experiment in ticks
                args[5] = "24";

                //start tick for configuration
                args[6] = "1";
            }

            configurationSite = args[0];
            containerName = args[1];
            resultFileFolderName = args[2];

            sleepTimeBetweenTicks = Int32.Parse(args[3]);
            ticksBetweenConfigurations = Int32.Parse(args[4]);
            experimentDurationInTicks = Int32.Parse(args[5]);
            startTickOfConfiguration = Int32.Parse(args[6]);

            Dictionary<string, CloudStorageAccount> acounts = Account.GetStorageAccounts(true);
            acounts.Remove("devstoreaccount1");
            ClientRegistry.Init(acounts, Account.GetStorageAccounts(true)[configurationSite]);
            ReplicaConfiguration configuration = ClientRegistry.GetConfiguration(containerName, false);

            Configurator conf = new Configurator(containerName);


            #region replicator
            Replicator replicator = new Replicator(containerName);
            replicator.Start();
            #endregion

            #region configurator

            List<ConfigurationConstraint> constraints = new List<ConfigurationConstraint>();
            //constraints.Add(new LocationConstraint(containerName, "dbteastasiastorage", LocationConstraintType.Replicate));
            constraints.Add(new ReplicationFactorConstraint(containerName, configuration, 1, 2));

            DateTime startTime = DateTime.Now;
            Thread.Sleep(startTickOfConfiguration * sleepTimeBetweenTicks);
            while (DateTime.Now.Subtract(startTime).TotalMilliseconds < (experimentDurationInTicks * sleepTimeBetweenTicks))
            {
                try
                {
                    configuration = ClientRegistry.GetConfiguration(containerName, false);
                    Console.WriteLine("Starting to reconfigure. Current Epoch: " + configuration.Epoch);
                    conf.Configure(ClientRegistry.GetConfigurationAccount(), configuration.Epoch, configuration, constraints);
                    Console.WriteLine(Configurator.Logs);
                    Console.WriteLine(">>>>>>>>>>> Finished. Current Epoch: " + configuration.Epoch + "<<<<<<<<<<<<<<<<");
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.StackTrace);
                }
                Thread.Sleep(ticksBetweenConfigurations * sleepTimeBetweenTicks);
            }

            #endregion

            return;

        }

        protected static void Cancelled(object sender, ConsoleCancelEventArgs args)
        {
            Console.WriteLine("\nThe read operation has been interrupted.");

            string resultFile = string.Format(@"{0}\{1}.csv", resultFileFolderName, "Configuration");

            using (StreamWriter sw = new StreamWriter(resultFile))
            {
                sw.Write(Configurator.Logs);
            }
        }


    }

}