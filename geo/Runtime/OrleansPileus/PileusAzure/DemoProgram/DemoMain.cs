using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
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
using PileusApp;
using PileusApp.Utils;
using System.Diagnostics;
using System.IO;
using PileusApp.YCSB;
using Microsoft.WindowsAzure.Storage.Pileus.Configuration.Actions;

namespace TechFestDemo
{
    public class DemoMain
    {
        #region Local variables

        static bool restoreConfiguration = false;
        static bool createDatabase = false;
        
        static int numBlobs = 1000;

        static Sampler sampler;

        #endregion

        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [STAThread]
        public static void Main()
        {
            Print("Running demo program...");
            DemoLib.RegisterLogger(Print);

            //
            // Create initial configuration
            //
            Print("Reading configuration...");
            DemoLib.Initialize();
            Print(DemoLib.PrintCurrentConfiguration());

            // ping servers to measure and record round-trip times in the server state
            Print("Pinging servers...");
            DemoLib.PingAllServers();

            //
            // Restore initial configuration to prepare for demo
            //
            if (restoreConfiguration)
            {
                Print("Restoring initial configuration...");
                DemoLib.SetInitialConfiguration();
                Print(DemoLib.PrintCurrentConfiguration());
            }

            //
            // Create initial set of blobs
            //
            if (createDatabase)
            {
                Print("Creating set of blobs...");
                DemoLib.CreateDatabase(numBlobs);
            }

            //
            // Read and write blobs while syncing data
            //
            Print("Reading and writing blobs...");
            sampler = DemoLib.PerformReadsWritesSyncs();

            //
            // Display read performance
            //
            Print("Read and write average latencies:");
   
            //
            // Compute better configuration
            //
            Print("Proposing new configuration...");
            DemoLib.ProposeNewConfiguration();
            Print(DemoLib.PrintReconfigurationActions());
  
            Print("Installing new configuration...");
            DemoLib.InstallNewConfiguration();
            Print(DemoLib.PrintCurrentConfiguration());

            //
            // Again, read and write blobs while syncing data
            //
            Print("Reading and writing blobs...");
            sampler = DemoLib.PerformReadsWritesSyncs();

            //
            // Display read performance for new configuration
            //
            Print("Read and write results:");
            Print(DemoLib.PrintReadWriteTimes(sampler));

            //
            // Restore initial configuration to prepare for next demo
            //
            Print("Restoring initial configuration...");
            DemoLib.SetInitialConfiguration();
            Print(DemoLib.PrintCurrentConfiguration());

            Print("Done. Enter return to exit.");
            Console.Read();
        }

        static void Print(string s)
        {
            Console.WriteLine(s);
        }

    }
}
