using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Pileus.Configuration;
using Microsoft.WindowsAzure.Storage.Pileus.Utils;
using System.Threading;

namespace Microsoft.WindowsAzure.Storage.Pileus
{
    /// <summary>
    /// Manages a collection of containers that hold replicated blobs.  
    /// Each blob is replicated in each container in accordance with a replication configuration.
    /// Some containers serve as primary replicas that hold up-to-date data,
    /// and others are secondary replicas with possibly stale data.
    /// Creates CapCloudBlobs objects for reading and writing replicated blobs.
    /// Read operations are controlled by consistency-based SLAs.
    /// </summary>
    public class CapCloudBlobContainer
    {

        // container name
        public string Name { get; internal set; }

        // default consistency-based SLA for blobs in this container
        public ServiceLevelAgreement SLA { get; set; }

        // configuration of primary and secondary replicas
        public ReplicaConfiguration Configuration { get; set; }

        // registry of round-trip latencies to each site/container
        public ServerMonitor Monitor { get; set; }
        
        // state for various named client sessions
        public Dictionary<string, SessionState> Sessions;

        // handles to the containers that store replicas
        private Dictionary<string, CloudBlobContainer> containers;
        
        /// <summary>
        /// Initializes a new instance of a <see cref="CapCloudBlobContainer"/> class that manages the specified
        /// blob containers.
        /// </summary>
        /// <param name="replicas">The set of containers in which blobs are replicated with their associated sites names.</param>
        /// <param name="primary">The name of the site that stores the primary replica.</param>
        public CapCloudBlobContainer(Dictionary<string, CloudBlobContainer> replicas, string primary)
        {
            this.containers = replicas;
            this.Name = replicas.First().Value.Name;

            // Create default configuration with one primary and one or more secondary servers
            List<string> primaries = new List<string>();
            List<string> secondaries = new List<string>();
            foreach (string site in replicas.Keys) {
                if (site == primary) {
                    primaries.Add(site);
                } else {
                    secondaries.Add(site);
                }
                secondaries.Add(site + "-secondary");
            }
            this.Configuration = new ReplicaConfiguration(this.Name, primaries, secondaries, null, null, false, true);

            // Create default SLA requesting strong consistency
            this.SLA = new ServiceLevelAgreement("strong", new SubSLA(5000, Consistency.Strong));
            
            // Create a server monitor to be shared by all operations from this client
            this.Monitor = new ServerMonitor(Configuration);

            // Create default session state
            this.Sessions = new Dictionary<string, SessionState>();
            this.Sessions["default"] = new SessionState();
        }

        /// <summary>
        /// Initializes a new instance of a <see cref="CapCloudBlobContainer"/> class that encapsulates a single
        /// blob container.
        /// </summary>
        /// <param name="container">The container in which blobs are stored.</param>
        public CapCloudBlobContainer(CloudBlobContainer container): this (new Dictionary<string, CloudBlobContainer>(), "")
        {
            this.containers.Add("main", container);
            this.Configuration.PrimaryServers.Add("main");
        }

        /// <summary>
        /// Gets a reference to a blob in this container.
        /// </summary>
        /// <param name="blobName">The name of the blob.</param>
        /// <returns>A reference to the blob.</returns>
        public ICloudBlob GetBlobReference(string blobName, string session = "default")
        {
            if (!Sessions.ContainsKey(session))
            {
                Sessions[session] = new SessionState();
            }
            
            ConsistencySLAEngine slaEngine = new ConsistencySLAEngine(SLA, Configuration, Sessions[session], Monitor);
            return new CapCloudBlob(blobName, Configuration, slaEngine);
        }

        /// <summary>
        /// Gets the state for a given session.
        /// </summary>
        /// <param name="session">The name of the session.</param>
        /// <returns>A reference to the session state.</returns>
        public SessionState GetSession(string session = "default")
        {
            if (!Sessions.ContainsKey(session))
            {
                Sessions[session] = new SessionState();
            }
            return Sessions[session];
        }

    }
}
