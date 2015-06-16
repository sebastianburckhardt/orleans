using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Pileus.Configuration;
using Microsoft.WindowsAzure.Storage.Pileus.Utils;
using Microsoft.WindowsAzure.Storage.Shared.Protocol;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.Storage.Pileus
{
    
    /// <summary>
    /// Maintains information about the state of each server that may hold replicated data.
    /// This data is shared among all sessions and SLA engines for a replicated container.
    /// But each replicated object, e.g. blob container, needs its own server monitor to track 
    /// specific replica state, e.g. high timestamps.
    /// </summary>
    public class ServerMonitor
    {
        /// <summary>
        /// Holds all servers, both those currently serving as a replica and those that might someday
        /// </summary>
        private IDictionary<string, ServerState> replicas;

        private ReplicaConfiguration configuration;

        private static Task periodicPingTask;

        /// <summary>
        /// Constructs a new monitor to keep track of the state of each server and optionally to ping servers periodically.
        /// </summary>
        /// <param name="config">The configuration of servers</param>
        /// <param name="periodicPing">Whether to ping servers periodically</param>
        public ServerMonitor(ReplicaConfiguration config, bool periodicPing = false)
        {
            this.replicas = new Dictionary<string, ServerState>();
            this.configuration = config;
            foreach (string primary in config.PrimaryServers)
            {
                // Note that the configuration may change later, and so clients should view the isPrimary bit as simply a hint
                RegisterServer(primary, true, 1);
            }
            foreach (string secondary in config.SecondaryServers)
            {
                RegisterServer(secondary, false, 2);
            }
            foreach (string nonreplica in config.NonReplicaServers)
            {
                RegisterServer(nonreplica, false, 3);
            }
            if (periodicPing)
            {
                periodicPingTask = Task.Factory.StartNew(() => PeriodPing());
            }
        }

        /// <summary>
        /// Registers a server that replicates data and accepts read/write operations.
        /// </summary>
        /// <param name="name">Server name</param>
        /// <param name="rank">How this server is ordered relative to others (lower rank is preferrable)</param>
        /// <returns>A handle to the server state</returns>
        public ServerState RegisterServer(string name, bool isPrimary, int rank)
        {
            ServerState state;
            // bool isPrimary = configuration.PrimaryServers.Contains(name);
            if (replicas.ContainsKey(name))
            {
                state = replicas[name];
                state.Rank = rank;
                state.IsPrimary = isPrimary;
            }
            else
            {
                state = new ServerState(name, isPrimary, rank);
                replicas[name] = state;  // inserts or replaces this server state
            }
            return state;
        }

        /// <summary>
        /// Removes a server from the list of available replicas.
        /// 
        /// Unregistering a primary server is O(number of serves). 
        /// However, since we expect this operation to be performed rarely, and preseting primary servers as a hashset has not conversion overhead during get operations, it is acceptable.
        /// </summary>
        /// <param name="name">Name of a server that was previously registered</param>
        public void UnregisterServer(string name)
        {
            //Remove replica from replica set. 
            if (replicas.ContainsKey(name))
                replicas.Remove(name);
        }

        public void UnregisterOldServers(List<string> newServers)
        {
            List<string> tmp = replicas.Keys.ToList();
            foreach (string replica in tmp)
            {
                if (!newServers.Contains(replica))
                {
                    UnregisterServer(replica);
                }
            }
        }

        /// <summary>
        /// Gets the state of a named server.
        /// </summary>
        /// <param name="serverName">The name of a server</param>
        /// <returns>A server state record</returns>
        public ServerState GetServerState(string serverName)
        {
            ServerState state;
            if (replicas.ContainsKey(serverName))
            {
                state = replicas[serverName];
            }
            else
            {
                state = new ServerState(serverName, false, 100);
                replicas[serverName] = state;  
            }
            return state;
        }

        /// <summary>
        /// Gets the state of every server.
        /// </summary>
        /// <returns>A list of server state records</returns>
        public List<ServerState> GetAllServersState()
        {
            return replicas.Values.ToList();
        }

        /// <summary>
        /// Pings all servers to obtain their round-trips times and their high timestamps.
        /// </summary>
        public void PingTimestampsNow()
        {
            Stopwatch watch = new Stopwatch();
            foreach (string server in configuration.SecondaryServers)
            {
                // if the server is not reached yet, we perform a dummy operation for it.
                if (!replicas[server].IsContacted())
                {
                    try
                    {
                        //we perform a dummy operation to get the rtt latency!
                        DateTimeOffset? serverTime = null;
                        long rtt;
                        if (server.EndsWith("-secondary") && configuration.PrimaryServers.Contains(server.Replace("-secondary", "")))
                        {
                            // get the server's last sync time from Azure
                            // this call only works on Azure secondaries
                            CloudBlobClient blobClient = ClientRegistry.GetCloudBlobClient(server);
                            watch.Restart();
                            ServiceStats stats = blobClient.GetServiceStats();
                            rtt = watch.ElapsedMilliseconds;
                            replicas[server].AddRtt(rtt);
                            if (stats.GeoReplication.LastSyncTime.HasValue)
                            {
                                serverTime = stats.GeoReplication.LastSyncTime.Value;
                            }
                        }
                        else
                        {
                            // get the server's last sync time from the container's metadata
                            CloudBlobContainer blobContainer = ClientRegistry.GetCloudBlobContainer(server, configuration.Name);
                            watch.Restart();
                            blobContainer.FetchAttributes();
                            rtt = watch.ElapsedMilliseconds;
                            if (blobContainer.Metadata.ContainsKey("lastsync"))
                            {
                                //if no lastmodified time is provided in the constructor, we still try to be fast.
                                //So, we check to see if by any chance the container previously has synchronized.
                                serverTime = DateTimeOffset.Parse(blobContainer.Metadata["lastsync"]);
                            }
                        }
                        if (serverTime.HasValue && serverTime > replicas[server].HighTime)
                        {
                            replicas[server].HighTime = serverTime.Value;
                        }
                    }
                    catch (StorageException)
                    {
                        // do nothing
                    }
                }
            }
        }

        /// <summary>
        /// Pings all servers to obtain their round-trips times and their high timestamps.
        /// </summary>
        public void PingNow()
        {
            Stopwatch watch = new Stopwatch();
            foreach (string server in replicas.Keys)
            {
                // if the server is not reached yet, we perform a dummy operation for it.
                if (!replicas[server].IsContacted())
                {
                    //we perform a dummy operation to get the rtt latency!
                    CloudBlobClient blobClient = ClientRegistry.GetCloudBlobClient(server);
                    long rtt;
                    watch.Restart();
                    blobClient.GetServiceProperties();
                    rtt = watch.ElapsedMilliseconds;
                    replicas[server].AddRtt(rtt);
                }
            }
        }

        /// <summary>
        /// Periodically pings servers. 
        /// This is fundamental for having a correct reconfiguration since each client needs to send its latency view of the whole system to the configurator.
        /// </summary>
        public void PeriodPing()
        {
            while (true)
            {
                PingTimestampsNow();
                PingNow();
                Thread.Sleep(ConstPool.LOOKUP_PING_INTERVAL);
            }
        }
    }
}
