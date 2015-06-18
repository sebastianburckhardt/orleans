using Microsoft.WindowsAzure.Storage.Pileus.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.Storage.Pileus
{
    /// <summary>
    /// Selects servers that can meet specific consistency guarantees.
    /// </summary>
    public class ServerSelector
    {
        
        private SessionState session;

        private ReplicaConfiguration config;

        private ServerMonitor monitor;

        /// <summary>
        /// Constructs a new server selector instance.
        /// </summary>
        /// <param name="session">holds the session state</param>
        /// <param name="config">holds the current replica configuration</param>
        public ServerSelector(SessionState session, ReplicaConfiguration config, ServerMonitor monitor)
        {
            this.session = session;
            this.config = config;
            this.monitor = monitor;
        }
        
        /// <summary>
        /// Returns the servers that can provide the desired consistency for the given object.
        /// </summary>
        /// <param name="objectName">Name of the object being read</param>
        /// <param name="consistency">Desired consistency</param>
        /// <param name="bound">Time bound for bounded staleness (in seconds)</param>
        /// <returns>The selected servers.</returns>
        public HashSet<ServerState> SelectServersForConsistency(string objectName, Consistency consistency, int bound)
        {
            HashSet<ServerState> selected;

            switch (consistency)
            {
                case Consistency.Strong:
                    selected = SelectServersForStrongConsistency(objectName);
                    break;

                case Consistency.ReadMyWrites:
                    selected = SelectServersForReadMyWrites(objectName);
                    break;

                case Consistency.MonotonicReads:
                    selected = SelectServersForMonotonicReads(objectName);
                    break;

                case Consistency.Session:
                    selected = SelectServersForReadMyWrites(objectName);
                    selected.IntersectWith(SelectServersForMonotonicReads(objectName));
                    break;

                case Consistency.Causal:
                    selected = SelectServersForCausal(objectName);
                    break;

                case Consistency.Bounded:
                    selected = SelectServersForBoundedStaleness(objectName, bound);
                    break;

                case Consistency.BoundedReadMyWrites:
                    selected = SelectServersForBoundedStaleness(objectName, bound);
                    selected.IntersectWith(SelectServersForReadMyWrites(objectName));
                    break;

                case Consistency.BoundedMonotonicReads:
                    selected = SelectServersForBoundedStaleness(objectName, bound);
                    selected.IntersectWith(SelectServersForMonotonicReads(objectName));
                    break;

                case Consistency.BoundedSession:
                    selected = SelectServersForBoundedStaleness(objectName, bound);
                    selected.IntersectWith(SelectServersForReadMyWrites(objectName));
                    selected.IntersectWith(SelectServersForMonotonicReads(objectName));
                    break;

                case Consistency.Eventual:
                    selected = SelectServersForEventualConsistency(objectName);
                    break;

                default:
                    selected = SelectServersForStrongConsistency(objectName);
                    break;
            }

            return selected;
        }

        /// <summary>
        /// Returns the best (lowest rank) server that can provide the desired consistency for the given object.
        /// </summary>
        /// <param name="objectName">Name of the object being read</param>
        /// <param name="consistency">Desired consistency</param>
        /// <param name="bound">Time bound for bounded staleness (in seconds)</param>
        /// <returns>The selected server.</returns>
        public ServerState SelectServerForConsistency(string objectName, Consistency consistency, int bound)
        {
            HashSet<ServerState> set;
            set = SelectServersForConsistency(objectName, consistency, bound);
            return SelectBestServer(set);
        }

        /// <summary>
        /// Returns an indication of whether reads should be directed to the primary replica in order to obtain the desired consistency.
        /// </summary>
        /// <param name="objectName">Name of the object being read</param>
        /// <param name="consistency">Desired consistency</param>
        /// <param name="bound">Time bound for bounded staleness (in seconds)</param>
        /// <returns>Whether to read from the primary.</returns>
        public bool MustUsePrimary(string objectName, Consistency consistency, int bound)
        {
            HashSet<ServerState> set;
            set = SelectServersForConsistency(objectName, consistency, bound);
            if (set.Count == 1 && config.PrimaryServers.Contains(set.SingleOrDefault().Name))
            {
                // the primary is the only server in the selected set
                return true;
            }
            return false;
        }

        /// <summary>
        /// Selects the servers to use for strong consistency reads.
        /// This always returns the primary, though, in some cases, it may be possible to
        /// determine whether a secondary replica is sufficiently up-to-date.
        /// </summary>
        /// <param name="objectName">The object being read</param>
        /// <returns>The selected servers.</returns>
        public HashSet<ServerState> SelectServersForStrongConsistency(string objectName)
        {
            HashSet<ServerState> set = new HashSet<ServerState>();
            foreach (string server in config.PrimaryServers)
            {
                if (!config.WriteOnlyPrimaryServers.Contains(server)) {
                     set.Add(monitor.GetServerState(server));
                }
            }
            return set;
        }

        /// <summary>
        /// Selects the servers to use for eventual consistency reads.
        /// It returns the complete set of servers since any replica can be used.
        /// </summary>
        /// <param name="objectName">The object being read</param>
        /// <returns>The selected servers.</returns>
        public HashSet<ServerState> SelectServersForEventualConsistency(string objectName)
        {
            HashSet<ServerState> set = SelectServersForStrongConsistency(objectName);
            foreach (string server in config.SecondaryServers)
            {
                set.Add(monitor.GetServerState(server));
            }
            return set;
        }

        /// <summary>
        /// Selects the servers to use for the ReadMyWrites consistency guarantee.
        /// Usually, this is the primary server since a secondary may not be sufficiently up-to-date.
        /// But sometimes we have enough info to select a secondary.
        /// </summary>
        /// <param name="objectName">The object being read</param>
        /// <returns>The selected servers.</returns>
        public HashSet<ServerState> SelectServersForReadMyWrites(string objectName)
        {
            // if object has been written by this client, then determine when
            DateTimeOffset minHighTime;
            if (session.objectsWritten.ContainsKey(objectName))
                minHighTime = session.objectsWritten[objectName].Timestamp;
            else
                minHighTime = new DateTime(0);

            // find servers that are sufficiently up-to-date
            HashSet<ServerState> set = SelectServersForStrongConsistency(objectName);
            foreach (string serverName in config.SecondaryServers)
            {
                ServerState server = monitor.GetServerState(serverName);
                if (server.HighTime >= minHighTime && server.LowTime <= minHighTime)
                {
                    set.Add(server);
                }
            }
            return set;
        }

        /// <summary>
        /// Selects the servers to use for the MonotonicReads consistency guarantee.
        /// Note that we can always read from the same server as the previous read.
        /// But, even if we previously selected the primary, a secondary server may now be acceptable.
        /// </summary>
        /// <param name="objectName">The object being read</param>
        /// <returns>The selected servers.</returns>
        public HashSet<ServerState> SelectServersForMonotonicReads(string objectName)
        {
            // if object has been read by this client, then determine timestamp
            DateTimeOffset minHighTime;
            if (session.objectsRead.ContainsKey(objectName))
                minHighTime = session.objectsRead[objectName].Timestamp;
            else
                minHighTime = new DateTime(0);

            // find servers that are sufficiently up-to-date
            HashSet<ServerState> set = SelectServersForStrongConsistency(objectName);
            foreach (string serverName in config.SecondaryServers)
            {
                ServerState server = monitor.GetServerState(serverName);
                if (server.HighTime >= minHighTime && server.LowTime <= minHighTime)
                {
                    set.Add(server);
                }
            }
            return set;
        }

        /// <summary>
        /// Selects the servers to use for Bounded staleness.
        /// This is a bit tricky since we may not have accurate information about the freshness of each server.
        /// </summary>
        /// <param name="objectName">The object being read</param>
        /// <param name="bound">Time bound for bounded staleness (in seconds)</param>
        /// <returns>The selected servers.</returns>
        public HashSet<ServerState> SelectServersForBoundedStaleness(string objectName, int bound)
        {
            // Note: This assumes that the client's clock is synchronized with the servers' clocks.
            // It also assumes that the server's highTime is an indication of when it last synced with a master
            // and that the server received all versions with a lower timestamp during its last sync.
            DateTimeOffset minHighTime = DateTimeOffset.Now - TimeSpan.FromSeconds(bound);

            // find servers that are not too stale
            HashSet<ServerState> set = SelectServersForStrongConsistency(objectName);
            foreach (string serverName in config.SecondaryServers)
            {
                ServerState server = monitor.GetServerState(serverName);
                // TODO: check timestamp on Azure secondary site
                if (server.HighTime >= minHighTime)
                {
                    set.Add(server);
                }
            }
            return set;
        }


        /// <summary>
        /// Selects the servers to use for Causal staleness.
        /// Servers should contain at least the maxReadTimestamp.
        /// </summary>
        /// <param name="objectName">The object being read</param>
        /// <param name="bound">Time bound for bounded staleness (in seconds)</param>
        /// <returns>The selected servers.</returns>
        public HashSet<ServerState> SelectServersForCausal(string objectName)
        {
            // minimum high timestamp is at least as large as any 
            DateTimeOffset minHighTime = session.maxReadTimestamp;

            // if object has been written by this client, then its time must be included
            if (session.objectsWritten.ContainsKey(objectName))
            {
                if (session.objectsWritten[objectName].Timestamp > minHighTime)
                    minHighTime = session.objectsWritten[objectName].Timestamp;
            }

            // find servers that are sufficiently up-to-date
            HashSet<ServerState> set = SelectServersForStrongConsistency(objectName);
            foreach (string serverName in config.SecondaryServers)
            {
                ServerState server = monitor.GetServerState(serverName);
                if (server.HighTime >= minHighTime)
                {
                    set.Add(server);
                }
            }
            return set;
        }

        /// <summary>
        /// Selects the server with the lowest rank among the candidate set.
        /// </summary>
        /// <param name="servers">The set of servers being considered</param>
        /// <returns>The selected server.</returns>
        private ServerState SelectBestServer(HashSet<ServerState> servers)
        {
            ServerState best = null;
            foreach (ServerState server in servers)
            {
                if (best == null || best.Rank > server.Rank)
                    best = server;
            }
            return best;
        }
        
    }
}
