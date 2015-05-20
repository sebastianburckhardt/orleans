using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Xml;
using System.Net;

namespace Orleans
{
    [Serializable]
    public class ClusterConfiguration
    {
        // Gateway information we've parsed from a config file.
        private Dictionary<int, List<SiloAddress>> clusterGateways;

        public TimeSpan LookupTimeout { get; private set; }

        public Dictionary<int, Dictionary<int, int>> DelayDictionary { get; private set; }

        public void SetLookupTimeout(TimeSpan newTimeout)
        {
            LookupTimeout = newTimeout;
        }

        // Hash of this node's silo address is used to deterministically select a gateway. 
        public int LocalHash { get; set; }     
 
        public int NumClusters
        {
            get
            {
                return clusterGateways.Count;
            }
        }

        public SiloAddress GetGateway(int clusterId)
        {
            var gatewayList = clusterGateways[clusterId];
            var listIndex = LocalHash % gatewayList.Count;
            SiloAddress addr = gatewayList[listIndex];
            return addr;
        }

        public IEnumerable<SiloAddress> GetAllGateways()
        {
            return clusterGateways.Select(x => x.Value[LocalHash % x.Value.Count]);
        }

        public ClusterConfiguration()
        {
            LocalHash = 0;
            clusterGateways = new Dictionary<int, List<SiloAddress>>();
            DelayDictionary = new Dictionary<int, Dictionary<int, int>>();
        }

        private void Validate()
        {
            if (LookupTimeout == null)
            {
                throw new OrleansException("ClusterConfiguration couldn't find a lookup timeout.");
            }

            var clusterIds = DelayDictionary.Keys.ToArray();
            for (int i = 0; i < clusterIds.Count(); ++i)
            {
                var dict = DelayDictionary[clusterIds[i]];
                for (int j = 0; j < clusterIds.Count(); ++j)
                {
                    if (i != j && !dict.ContainsKey(clusterIds[j]))
                    {
                        throw new OrleansException("Incomplete delay dictionary.");
                    }
                }
            }

        }

        internal void Load(XmlElement root)
        {
            XmlElement child;
            foreach (XmlNode c in root.ChildNodes)
            {
                child = c as XmlElement;
                if (child != null && child.LocalName == "Gateway")
                {
                    // Parse the gateway's address and the cluster it belongs to.
                    IPEndPoint gatewayIP = ConfigUtilities.ParseIPEndPoint(child, null);
                    int clusterId = ConfigUtilities.ParseInt(child.GetAttribute("Cluster"), 
                        "ClusterConfiguration: Couldn't parse cluster id!");

                    // XXX Not sure about the "0" specifying the generation number!
                    SiloAddress gatewayAddress = SiloAddress.New(gatewayIP, 0, clusterId);

                    // Keep track of the gateway.
                    List<SiloAddress> gatewayList;
                    if (!clusterGateways.TryGetValue(clusterId, out gatewayList))
                    {
                        gatewayList = new List<SiloAddress>();
                        clusterGateways.Add(clusterId, gatewayList);
                    }
                    gatewayList.Add(gatewayAddress);
                }
                else if (child != null && child.LocalName == "LookupTimeout")
                {
                    int seconds = ConfigUtilities.ParseInt(child.GetAttribute("Seconds"),
                        "ClusterConfiguration: Couldn't parse LookupTimeout!");
                    int milliseconds = ConfigUtilities.ParseInt(child.GetAttribute("Milliseconds"),
                        "ClusterConfiguration: Couldn't parse LookupTimeout!");
                    LookupTimeout = new TimeSpan(0, 0, 0, seconds, milliseconds);
                }
                else if (child != null && child.LocalName == "InterClusterDelay")
                {
                    int fromCluster = ConfigUtilities.ParseInt(child.GetAttribute("FromCluster"),
                        "ClusterConfiguration: Couldn't parse FromCluster!");
                    int toCluster = ConfigUtilities.ParseInt(child.GetAttribute("ToCluster"),
                        "ClusterConfiguration: Couldn't parse ToCluster!");
                    int delay = ConfigUtilities.ParseInt(child.GetAttribute("Delay"),
                        "ClusterConfiguration: Couldn't parse Delay!");

                    Dictionary<int, int> dict;
                    if (!DelayDictionary.TryGetValue(fromCluster, out dict))
                    {
                        dict = new Dictionary<int, int>();
                        DelayDictionary[fromCluster] = dict;
                    }
                    dict.Add(toCluster, delay);
                }
                else
                {
                    // throw an exception, we don't expect any other kind of child.
                }
            }
            Validate();
        }
    }
}
