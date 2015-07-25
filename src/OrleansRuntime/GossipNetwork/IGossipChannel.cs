using Orleans.Concurrency;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


// should this go into SystemTargetInterfaces?

namespace Orleans.Runtime.GossipNetwork
{
    /// <summary>
    /// Interface for a gossip channel
    /// </summary>
    internal interface IGossipChannel
    {
        /// <summary>
        /// One-way small-scale gossip: send partial gossip data to recipient
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        Task Push(GossipData data);

         /// <summary>
        /// Two-way bulk gossip: send all known gossip data to recipient, and receive all unknown gossip
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        Task<GossipData> PushAndPull(GossipData data);

    }


    /// <summary>
    /// Multicluster configuration, as injected by user, and stored/transmitted in the gossip network.
    /// </summary>
    [Serializable]
    public class MultiClusterConfiguration : IEquatable<MultiClusterConfiguration>
    {
        /// <summary>
        /// The UTC timestamp of this configuration. Used for propagating configuration changes (admin injects new configurations with higher timestamp).
        /// </summary>
        public DateTime AdminTimestamp { get; private set; }

        /// <summary>
        /// List of clusters in this multicluster.
        /// </summary>
        public IReadOnlyList<string> Clusters { get; private set; }


        public override string ToString()
        {
            return string.Format("{0} [{1}]",
                AdminTimestamp, string.Join(",", Clusters)
            );
        }

        public MultiClusterConfiguration(DateTime Timestamp, IReadOnlyList<string> Clusters)
        {
            Debug.Assert(Clusters != null);
            this.AdminTimestamp = Timestamp;
            this.Clusters = Clusters;
        }

        public static bool OlderThan(MultiClusterConfiguration a, MultiClusterConfiguration b)
        {
            if (a == null)
                return b != null;
            else
                return b != null && a.AdminTimestamp < b.AdminTimestamp;
        }

        public bool Equals(MultiClusterConfiguration other)
        {
            if (!AdminTimestamp.Equals(other.AdminTimestamp)
                || Clusters.Count != other.Clusters.Count)
                return false;

            for (int i = 0; i < Clusters.Count; i++)
                if (Clusters[i] != other.Clusters[i])
                    return false;

            return true;
        }
    }


    /// <summary>
    /// Gateways are either active (silo is active & a gateway), 
    /// None (silo is not known to be a gateway), or Dead (silo was authoritatively determined dead)
    /// </summary>
    public enum GatewayStatus
    {
        None,
        Active,
        Dead
    }

    /// <summary>
    /// Information about gateways, as stored/transmitted in the gossip network.
    /// </summary>
    [Serializable]
    public class GatewayEntry : IEquatable<GatewayEntry>
    {
        public string ClusterId { get { return this.SiloAddress.ClusterId; } }

        public SiloAddress SiloAddress { get; set; }

        public GatewayStatus Status { get; set; }

        public DateTime HeartbeatTimestamp { get; set; }   // for gossip ordering and removing expired entries


        // define retention time for gateway entries
        public bool Expired
        {
            get
            {
                return DateTime.UtcNow - HeartbeatTimestamp > new TimeSpan(hours: 0, minutes: 10, seconds: 0);
            }
        }

        public bool Equals(GatewayEntry other)
        {
            return SiloAddress.Equals(other.SiloAddress)
                && Status.Equals(other.Status)
                && HeartbeatTimestamp.Equals(other.HeartbeatTimestamp);
        }
    }

    /// <summary>
    /// Data stored and transmitted in the gossip network. Can represent both entire store content and delta.
    /// So far includes multicluster-configuration and multicluster-gateway information.
    /// </summary>
    [Serializable]
    public class GossipData
    {
        public IReadOnlyDictionary<SiloAddress,GatewayEntry> Gateways { get; private set; }
        
        // may be null to indicate initial configuration (empty list)
        public MultiClusterConfiguration Configuration { get; private set; }

        public bool IsEmpty { get { return Gateways.Count == 0 && Configuration == null; } }

        public GossipData(IReadOnlyDictionary<SiloAddress, GatewayEntry> d, MultiClusterConfiguration config)
        {
            Gateways = d;
            Configuration = config;
        }

        public GossipData()
        {
            Gateways = emptyd;
            Configuration = null;
        }

        private static IReadOnlyDictionary<SiloAddress,GatewayEntry> emptyd = new Dictionary<SiloAddress,GatewayEntry>();

        public GossipData(GatewayEntry gatewayentry)
        {
            var l = new Dictionary<SiloAddress, GatewayEntry>();
            l.Add(gatewayentry.SiloAddress, gatewayentry);
            Gateways = l;
            Configuration = null;
        }
        public GossipData(IEnumerable<GatewayEntry> gatewayentries)
        {
            var l = new Dictionary<SiloAddress, GatewayEntry>();
            foreach(var gatewayentry in gatewayentries)
                l.Add(gatewayentry.SiloAddress, gatewayentry);
            Gateways = l;
            Configuration = null;
        }

        public GossipData(MultiClusterConfiguration config)
        {
            Gateways = emptyd;
            Configuration = config;
        }

        public override string ToString()
        {
            int active = Gateways.Values.Count(e => e.Status == GatewayStatus.Active);

            return string.Format("Conf=[{0}] Gateways {1}/{2} Active",
                Configuration.ToString(),
                active,
                Gateways.Count
            );
        }

        // incorporate source, producing new GossipData result, and report delta.
        public GossipData Merge(GossipData source, out GossipData delta)
        {
            //--  configuration 
            var sourceconf = source.Configuration;
            var thisconf = this.Configuration;
            MultiClusterConfiguration resultconf = null;
            MultiClusterConfiguration deltaconf = null;
            if (MultiClusterConfiguration.OlderThan(thisconf, sourceconf))
            {
                resultconf = sourceconf;

            }
            else
                resultconf = thisconf;
            
            //--  gateways
            var sourcelist = source.Gateways;
            var thislist = this.Gateways;
            var resultlist = new Dictionary<SiloAddress, GatewayEntry>();
            var deltalist = new Dictionary<SiloAddress, GatewayEntry>();
            foreach (var key in sourcelist.Keys.Union(thislist.Keys).Distinct())
            {
                GatewayEntry thisentry;
                GatewayEntry sourceentry;
                thislist.TryGetValue(key, out thisentry);
                sourcelist.TryGetValue(key, out sourceentry);

                if (sourceentry != null && !sourceentry.Expired
                     && (thisentry == null || thisentry.HeartbeatTimestamp < sourceentry.HeartbeatTimestamp))
                {
                    resultlist.Add(key, sourceentry);
                    deltalist.Add(key, sourceentry);
                }
                else if (thisentry != null)
                {
                    if (!thisentry.Expired)
                        resultlist.Add(key, thisentry);
                    else
                        deltalist.Add(key,thisentry);
                }
            }

            delta = new GossipData(deltalist, deltaconf);
            return new GossipData(resultlist, resultconf);
        }
    }

}
