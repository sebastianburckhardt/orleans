using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Pileus.Utils;
using Microsoft.WindowsAzure.Storage.Pileus.Configuration;
using Microsoft.WindowsAzure.Storage.Pileus.Configuration.Actions;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Microsoft.WindowsAzure.Storage.Pileus.Configuration
{
    /// <summary>
    /// This is a simple replicator for <see cref="CapCloudBlobContainer"/>
    /// 
    /// For a particular CapCloudBlobContainer, it synchronizes the container's secondary replicas with the primary.
    /// 
    /// This class assumes that synchronization duration is smaller than synchronization intervals.
    /// Note that it's very hard to fix this with current architecture where we read from primary, and write to secondary.
    /// In real systems, synchronizations should be performed by replica servers directly. I.e., secondary should pull updates from a primary.
    /// </summary>
    public class Replicator
    {
        private string name;
        private ReplicaConfiguration configuration;
        private Task task;

        public Replicator(string capCloudBlobContainerName)
        {
            configuration = ClientRegistry.GetConfiguration(capCloudBlobContainerName, false);
            name = configuration.Name;
        }

        public void Start()
        {
            task = Task.Factory.StartNew(StartReplicating);
        }

        private void StartReplicating()
        {
            Dictionary<string, SynchronizeContainer> syncing = new Dictionary<string, SynchronizeContainer>();
            Dictionary<string, DateTimeOffset> lastSynced = new Dictionary<string, DateTimeOffset>();
            int alreadySleep = 0;

            while (true)
            {
                if (configuration.IsInFastMode() && configuration.SecondaryServers.Count > 0)
                {
                    alreadySleep = 0;
                    syncing.Clear();
                    
                    // compute shortest sync period
                    int sleepTime = ConstPool.DEFAULT_SYNC_INTERVAL;
                    foreach (string server in configuration.SecondaryServers)
                    {
                        if (configuration.GetSyncPeriod(server) < sleepTime)
                        {
                            sleepTime = configuration.GetSyncPeriod(server);
                        }
                    }
                    Thread.Sleep(sleepTime - alreadySleep);

                    foreach (string server in configuration.SecondaryServers)
                    {
                        //server should not exist in primary servers. This case happens during first phase of MakeSoloPrimaryServer action
                        //where a secondary server also exist in primary server set.
                        if (configuration.GetSyncPeriod(server) == sleepTime && !configuration.PrimaryServers.Contains(server))
                        {
                            Console.WriteLine("Starting to sync " + configuration.Name + " with " + server);

                            DateTimeOffset? tmpOffset=null ;
                            if (lastSynced.ContainsKey(server))
                                tmpOffset = lastSynced[server];
                            lastSynced[server] = DateTimeOffset.Now;
                            SynchronizeContainer synchronizer = new SynchronizeContainer(ClientRegistry.GetMainPrimaryContainer(name), ClientRegistry.GetCloudBlobContainer(server, name), tmpOffset);
                            synchronizer.BeginSyncContainers();
                            syncing[server] = synchronizer;
                        }
                    }
                    foreach (string syncedServer in syncing.Keys)
                    {
                        syncing[syncedServer].EndSyncContainers();
                    }
                    Console.WriteLine("Finished synchronization. ");
                    alreadySleep = sleepTime;
                }
                else
                {
                    lock (configuration)
                        Monitor.Wait(configuration, ConstPool.CONFIGURATION_ACTION_DURATION);
                }

            }
        }
    }
}
