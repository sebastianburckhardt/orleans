using System;
using System.Collections.Generic;
using System.Linq;
using Orleans.MultiCluster;

namespace Orleans.Runtime.MultiClusterNetwork
{
    /// <summary>
    /// Data stored and transmitted in the multicluster network. 
    /// IMPORTANT: these objects can represent full state, partial state, or delta.
    /// So far includes multicluster-configuration and multicluster-gateway information.
    /// Data is gossip-able.
    /// </summary>
    [Serializable]
    public class MultiClusterData : IEquatable<MultiClusterData>, IMultiClusterGossipData
    {
        /// <summary>
        /// The dictionary of gateway entries and their current status.
        /// </summary>
        public IReadOnlyDictionary<SiloAddress, GatewayEntry> Gateways { get; private set; }

        /// <summary>
        /// The admin-injected configuration.
        /// May be null if none has been injected yet, or if this object represents a partial state or delta.
        /// </summary>
        public MultiClusterConfiguration Configuration { get; private set; }

        /// <summary>
        /// Whether there is actually any data in here.
        /// </summary>
        public bool IsEmpty
        {
            get
            {
                return Gateways.Count == 0 && Configuration == null;
            }
        }

   
        private static Dictionary<SiloAddress, GatewayEntry> emptyd = new Dictionary<SiloAddress, GatewayEntry>();

        #region constructor overloads

        public MultiClusterData(IReadOnlyDictionary<SiloAddress, GatewayEntry> d, MultiClusterConfiguration config)
        {
            Gateways = d;
            Configuration = config;
        }
        public MultiClusterData()
        {
            Gateways = emptyd;
            Configuration = null;
        }
        public MultiClusterData(GatewayEntry gatewayentry)
        {
            var l = new Dictionary<SiloAddress, GatewayEntry>();
            l.Add(gatewayentry.SiloAddress, gatewayentry);
            Gateways = l;
            Configuration = null;
        }
        public MultiClusterData(IEnumerable<GatewayEntry> gatewayentries)
        {
            var l = new Dictionary<SiloAddress, GatewayEntry>();
            foreach (var gatewayentry in gatewayentries)
                l.Add(gatewayentry.SiloAddress, gatewayentry);
            Gateways = l;
            Configuration = null;
        }
        public MultiClusterData(MultiClusterConfiguration config)
        {
            Gateways = emptyd;
            Configuration = config;
        }

        #endregion

        public override string ToString()
        {
            int active = Gateways.Values.Count(e => e.Status == GatewayStatus.Active);

            return string.Format("Conf=[{0}] Gateways {1}/{2} Active",
                Configuration == null ? "null" : Configuration.ToString(),
                active,
                Gateways.Count
            );
        }

        /// <summary>
        /// Check whether a particular silo is an active gateway for a cluster
        /// </summary>
        /// <param name="address">the silo address</param>
        /// <param name="clusterid">the id of the cluster</param>
        /// <returns></returns>
        public bool IsActiveGatewayForCluster(SiloAddress address, string clusterid)
        {
            GatewayEntry info;
            return  Gateways.TryGetValue(address, out info) 
                && info.ClusterId == clusterid && info.Status == GatewayStatus.Active;
        }


        /// <summary>
        ///  merge source into this object, and return result.
        ///  Ignores expired entries in source, and removes expired entries from this.
        /// <param name="source">The source data to apply to the data in this object</param>
        /// <returns>The updated data</returns>
        public MultiClusterData Merge(MultiClusterData source)
        {
            MultiClusterData ignore;
            return Merge(source, out ignore);
        }


        /// <summary>
        ///  incorporate source, producing new result, and report delta.
        ///  Ignores expired entries in source, and removes expired entries from this.
        /// </summary>
        /// <param name="source">The source data to apply to the data in this object</param>
        /// <param name="delta">A delta of what changes were actually applied, used for change listeners</param>
        /// <returns>The updated data</returns>
        public MultiClusterData Merge(MultiClusterData source, out MultiClusterData delta)
        {
            //--  configuration 
            var sourceconf = source.Configuration;
            var thisconf = this.Configuration;
            MultiClusterConfiguration resultconf = null;
            MultiClusterConfiguration deltaconf = null;
            if (MultiClusterConfiguration.OlderThan(thisconf, sourceconf))
            {
                resultconf = sourceconf;
                deltaconf = sourceconf;
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
                        deltalist.Add(key, thisentry);
                }
            }

            delta = new MultiClusterData(deltalist, deltaconf);
            return new MultiClusterData(resultlist, resultconf);
        }

        /// <summary>
        /// Returns all data of this object except for what keys appear in exclude
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        public MultiClusterData Minus(MultiClusterData exclude)
        {

            IReadOnlyDictionary<SiloAddress, GatewayEntry> resultlist;
            if (exclude.Gateways.Count == 0)
                resultlist = this.Gateways;
            else {
                var list = new Dictionary<SiloAddress, GatewayEntry>();
                foreach(var g in this.Gateways)
                    if (!exclude.Gateways.ContainsKey(g.Key))
                       list.Add(g.Key, g.Value);
                resultlist = list;
            }

            MultiClusterConfiguration resultconf;
            if (exclude.Configuration == null)
                resultconf = this.Configuration;
            else
                resultconf = null;

            return new MultiClusterData(resultlist, resultconf);
        }

        public bool Equals(MultiClusterData other)
        {
            if ((this.Configuration == null) != (other.Configuration == null))
              return false;

            if (this.Gateways.Count != other.Gateways.Count)
              return false;

            if ((this.Configuration != null) && !this.Configuration.Equals(other.Configuration))
              return false;

            foreach (var g in this.Gateways)
            {
                GatewayEntry othergateway;
                if (!other.Gateways.TryGetValue(g.Key, out othergateway))
                    return false;
                if (!g.Value.Equals(othergateway))
                    return false;
            }

            return true;
        }

    }

     

    /// <summary>
    /// Information about gateways, as stored/transmitted in the multicluster network.
    /// </summary>
    [Serializable]
    public class GatewayEntry : IMultiClusterGatewayInfo, IEquatable<GatewayEntry>, IComparable<GatewayEntry>
    {
        public string ClusterId { get; set; }

        public SiloAddress SiloAddress { get; set; }

        public GatewayStatus Status { get; set; }

        /// <summary>
        /// UTC timestamp of this gateway entry.
        /// </summary>
        public DateTime HeartbeatTimestamp { get; set; }


        /// <summary>
        /// Whether this entry has expired based on its timestamp.
        /// </summary>
        public bool Expired
        {
            get
            {
                return DateTime.UtcNow - HeartbeatTimestamp > ExpiresAfter;
            }
        }
        /// <summary>
        /// time after which entries expire.
        /// </summary>
        public static TimeSpan ExpiresAfter = new TimeSpan(hours: 0, minutes: 30, seconds: 0);

        public bool Equals(GatewayEntry other)
        {
            return SiloAddress.Equals(other.SiloAddress)
                && Status.Equals(other.Status)
                && HeartbeatTimestamp.Equals(other.HeartbeatTimestamp)
                && ClusterId.Equals(other.ClusterId);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = this.SiloAddress.GetHashCode();
                hashCode = (hashCode * 397) ^ this.Status.GetHashCode();
                hashCode = (hashCode * 397) ^ this.HeartbeatTimestamp.GetHashCode();
                hashCode = (hashCode * 397) ^ this.ClusterId.GetHashCode();
                return hashCode;
            }
        }

        public int CompareTo(GatewayEntry other)
        {
            var diff = ClusterId.CompareTo(other.ClusterId);
            if (diff != 0) return diff;
            diff = SiloAddress.ToString().CompareTo(other.SiloAddress.ToString());
            if (diff != 0) return diff;
            diff = HeartbeatTimestamp.CompareTo(other.HeartbeatTimestamp);
            return diff;
        }

        public override string ToString()
        {
            return string.Format("[Gateway {0} {1} {2} {3}]", ClusterId, SiloAddress, Status, HeartbeatTimestamp);
        }
    }
}
