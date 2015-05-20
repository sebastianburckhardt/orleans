using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime;
using System.Text;
using System.Net;
using System.IO;
using System.Xml;
using System.Net.Sockets;
using System.Net.NetworkInformation;

namespace Orleans
{
    /// <summary>
    /// Data object holding Silo configuration parameters.
    /// </summary>
    [Serializable]
    public class OrleansConfiguration
    {
        /// <summary>
        /// The global configuration parameters that apply uniformly to all silos.
        /// </summary>
        public GlobalConfiguration Globals { get; private set; }

        /// <summary>
        /// The default configuration parameters that apply to each and every silo. 
        /// These can be over-written on a per silo basis.
        /// </summary>
        public NodeConfiguration Defaults { get; private set; }

        /// <summary>
        ///  
        /// </summary>
        public ClusterConfiguration Cluster { get; private set; }

        /// <summary>
        /// The configuration file.
        /// </summary>
        public string SourceFile { get; private set; }

        private IPEndPoint primaryNode;
        /// <summary>
        /// The Primary Node IP and port (in dev setting).
        /// </summary>
        public IPEndPoint PrimaryNode { get { return primaryNode; } set { setPrimaryNode(value); } }

        /// <summary>
        /// Per silo configuration parameters overrides.
        /// </summary>
        public Dictionary<string, NodeConfiguration> Overrides { get; private set; }

        private Dictionary<string, string> overrideXml;
        private readonly Dictionary<string, List<Action>> listeners;
        internal bool IsRunningAsUnitTest { get; set; }

        // Note: Should not use orleans.xml in this list as that is likely to be present in the distribution and contain the IntelliSense doc comments for Orleans.dll
        private string[] DefaultConfigPaths = { "OrleansConfiguration.xml", ".\\orleans.config", ".\\config.xml", ".\\orleans.config.xml" };

        /// <summary>
        /// OrleansConfiguration constructor.
        /// </summary>
        public OrleansConfiguration()
        {
            listeners = new Dictionary<string, List<Action>>();
            Init();
        }

        /// <summary>
        /// OrleansConfiguration constructor.
        /// </summary>
        public OrleansConfiguration(TextReader input)
        {
            Load(input);
        }

        private void Init()
        {
            Globals = new GlobalConfiguration();
            Defaults = new NodeConfiguration();
            Cluster = new ClusterConfiguration();
            Overrides = new Dictionary<string, NodeConfiguration>();
            overrideXml = new Dictionary<string, string>();
            SourceFile = "";
            IsRunningAsUnitTest = false;
        }

        /// <summary>
        /// Loads configuration from a given input text reader.
        /// </summary>
        /// <param name="input">The TextReader to use.</param>
        public void Load(TextReader input)
        {
            Init();

            LoadFromXml(ParseXml(input));
        }

        internal void LoadFromXml(XmlElement root)
        {
            XmlElement child;
            foreach (XmlNode c in root.ChildNodes)
            {
                child = c as XmlElement;
                if (child == null) continue; // Skip comment lines

                switch (child.LocalName)
                {
                    case "ClusterGateways":
                        Cluster.Load(child);
                        break;
                    case "Deployment":
                        LoadDeployment(child);
                        break;
                    case "Globals":
                        Globals.Load(child);
                        // set subnets so this is independent of order
                        Defaults.Subnet = Globals.Subnet;
                        foreach (var o in Overrides.Values)
                        {
                            o.Subnet = Globals.Subnet;
                        }
                        if (Globals.SeedNodes.Count > 0)
                        {
                            primaryNode = Globals.SeedNodes[0];
                        }
                        break;
                    case "Defaults":
                        Defaults.Load(child);
                        Defaults.Subnet = Globals.Subnet;
                        break;
                    case "Override":
                        overrideXml[child.GetAttribute("Node")] = WriteXml(child);
                        break;
                }
            }
            CalculateOverrides();
        }

        private static string WriteXml(XmlElement element)
        {
            var text = new StringWriter();
            var xml = new XmlTextWriter(text);
            element.WriteTo(xml);
            return text.ToString();
        }

        private void CalculateOverrides()
        {
            foreach (var p in overrideXml)
            {
                NodeConfiguration n = new NodeConfiguration(Defaults);
                n.Load(ParseXml(new StringReader(p.Value)));
                //Debug.Assert(n.SiloName != null && n.SiloName != NodeConfiguration.DEFAULT_NODE_NAME && n.SiloName == p.Key);
                InitNodeSettingsFromGlobals(n);
                Overrides[n.SiloName] = n;
            }
        }

        internal void AdjustConfiguration()
        {
            GlobalConfiguration.AdjustConfiguration(Globals.ProviderConfigurations, Globals.DeploymentId);
        }

        private void InitNodeSettingsFromGlobals(NodeConfiguration n)
        {
            //Debug.Assert(n.SiloName != null);
            if (n.Endpoint.Equals(this.PrimaryNode)) n.IsPrimaryNode = true;
            //if (Globals.GatewayNodes.Contains(n.Endpoint)) n.IsGatewayNode = true;
            if (Globals.SeedNodes.Contains(n.Endpoint)) n.IsSeedNode = true;
        }

        public void LoadFromFile(string fileName)
        {
            TextReader input = File.OpenText(fileName);
            try
            {
                Load(input);
                SourceFile = fileName;
            }
            finally
            {
                input.Close();
            }
        }

        /// <summary>
        /// Returns the configuration for a given silo.
        /// </summary>
        /// <param name="name">Silo name.</param>
        /// <returns>NodeConfiguration associated with the specified silo.</returns>
        public NodeConfiguration GetConfigurationForNode(string name)
        {
            NodeConfiguration n;
            if (!Overrides.TryGetValue(name, out n))
            {
                n = new NodeConfiguration(Defaults);
                n.SiloName = name;
                InitNodeSettingsFromGlobals(n);
                Overrides[name] = n;
            }
            return n;
        }

        private void setPrimaryNode(IPEndPoint primary)
        {
            primaryNode = primary;
            foreach (NodeConfiguration node in Overrides.Values)
            {
                if (node.Endpoint.Equals(primary))
                {
                    node.IsPrimaryNode = true;
                }
            }
        }


        /// <summary>
        /// Loads the configuration from the standard paths
        /// </summary>
        /// <returns></returns>
        public bool StandardLoad()
        {
            foreach (string s in DefaultConfigPaths)
            {
                if (File.Exists(s))
                {
                    LoadFromFile(s);
                    return true;
                }
            }
            return false;
        }

        internal void AddStandardPath(string path)
        {
            int n = DefaultConfigPaths.Length;
            string[] old = DefaultConfigPaths;
            DefaultConfigPaths = new string[n + 1];
            for (int i = 0; i < n; i++)
            {
                DefaultConfigPaths[i] = old[i];
            }
            DefaultConfigPaths[n] = path;
        }

        /// <summary>
        /// Subset of XML configuration file that is updatable at runtime
        /// </summary>
        private static readonly XmlElement UpdatableXml = ParseXml(new StringReader(@"
        <OrleansConfiguration>
            <Globals>
                <Messaging ResponseTimeout=""?""/>
                <Caching CacheSize=""?""/>
                <Liveness ProbeTimeout=""?"" TableRefreshTimeout=""?"" NumMissedProbesLimit=""?""/>
            </Globals>
            <Defaults>
                <LoadShedding Enabled=""?"" LoadLimit=""?""/>
                <Tracing DefaultTraceLevel=""?"" PropagateActivityId=""?"">
                    <TraceLevelOverride LogPrefix=""?"" TraceLevel=""?""/>
                </Tracing>
            </Defaults>
        </OrleansConfiguration>"));

        /// <summary>
        /// Updates existing configuration.
        /// </summary>
        /// <param name="input">The input string in XML format to use to update the existing configuration.</param>
        /// <returns></returns>
        public void Update(string input)
        {
            var xml = ParseXml(new StringReader(input));
            var disallowed = new List<string>();
            CheckSubtree(UpdatableXml, xml, "", disallowed);
            if (disallowed.Count > 0)
                throw new ArgumentException("Cannot update configuration with" + disallowed.ToStrings());
            var dict = ToChildDictionary(xml);
            XmlElement globals;
            if (dict.TryGetValue("Globals", out globals))
            {
                Globals.Load(globals);
                ConfigChanged("Globals");
                foreach (var key in ToChildDictionary(globals).Keys)
                {
                    ConfigChanged("Globals/" + key);
                }
            }
            XmlElement defaults;
            if (dict.TryGetValue("Defaults", out defaults))
            {
                Defaults.Load(defaults);
                CalculateOverrides();
                ConfigChanged("Defaults");
                foreach (var key in ToChildDictionary(defaults).Keys)
                {
                    ConfigChanged("Defaults/" + key);
                }
            }
        }

        private static void CheckSubtree(XmlElement allowed, XmlElement test, string prefix, List<string> disallowed)
        {
            prefix = prefix + "/" + test.LocalName;
            if (allowed.LocalName != test.LocalName)
            {
                disallowed.Add(prefix);
                return;
            }
            foreach (var attribute in AttributeNames(test))
            {
                if (! allowed.HasAttribute(attribute))
                {
                    disallowed.Add(prefix + "/@" + attribute);
                }
            }
            var allowedChildren = ToChildDictionary(allowed);
            foreach (var t in test.ChildNodes)
            {
                var testChild = t as XmlElement;
                if (testChild == null)
                    continue;
                XmlElement allowedChild;
                if (! allowedChildren.TryGetValue(testChild.LocalName, out allowedChild))
                {
                    disallowed.Add(prefix + "/" + testChild.LocalName);
                }
                else
                {
                    CheckSubtree(allowedChild, testChild, prefix, disallowed);
                }
            }
        }

        private static Dictionary<string, XmlElement> ToChildDictionary(XmlElement xml)
        {
            var result = new Dictionary<string, XmlElement>();
            foreach (var c in xml.ChildNodes)
            {
                var child = c as XmlElement;
                if (child == null)
                    continue;
                result[child.LocalName] = child;
            }
            return result;
        }

        private static IEnumerable<string> AttributeNames(XmlElement element)
        {
            foreach (var a in element.Attributes)
            {
                var attr = a as XmlAttribute;
                if (attr != null)
                    yield return attr.LocalName;
            }
        }

        internal void OnConfigChange(string path, Action action, bool invokeNow = true)
        {
            List<Action> list;
            if (listeners.TryGetValue(path, out list))
                list.Add(action);
            else
                listeners.Add(path, new List<Action> { action });
            if (invokeNow)
                action();
        }

        internal void ConfigChanged(string path)
        {
            List<Action> list;
            if (listeners.TryGetValue(path, out list))
            {
                foreach (var action in list)
                    action();
            }
        }

        /// <summary>
        /// Prints the current config for a given silo.
        /// </summary>
        /// <param name="siloName">The name of the silo to print its configuration.</param>
        /// <returns></returns>
        public string ToString(string siloName)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("Config File Name: ").AppendLine(string.IsNullOrEmpty(SourceFile) ? "" : Path.GetFullPath(SourceFile));
            sb.Append("Host: ").AppendLine(Dns.GetHostName());
            sb.Append("Start time: ").AppendLine(Logger.PrintDate(DateTime.UtcNow));
            sb.Append("Primary node: ").AppendLine(PrimaryNode == null ? "null" : PrimaryNode.ToString());
            sb.AppendLine("Platform version info:").Append(RuntimeVersionInfo());
            sb.AppendLine("Global configuration:").Append(Globals.ToString());
            NodeConfiguration nc = GetConfigurationForNode(siloName);
            sb.AppendLine("Silo configuration:").Append(nc.ToString());
            sb.AppendLine();
            return sb.ToString();
        }

        /// <summary>
        /// Returns the Runtime Version information.
        /// </summary>
        /// <returns>the Runtime Version information</returns>
        public static string RuntimeVersionInfo()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("   .NET version: ").AppendLine(Environment.Version.ToString());
            sb.Append("   Is .NET 4.5=").AppendLine(IsNet45OrNewer().ToString());
            sb.Append("   OS version: ").AppendLine(Environment.OSVersion.ToString());
            sb.AppendFormat("   GC Type={0} GCLatencyMode={1}",
                              GCSettings.IsServerGC ? "Server" : "Client",
                              Enum.GetName(typeof (GCLatencyMode), GCSettings.LatencyMode))
                .AppendLine();
            return sb.ToString();
        }

        internal static IPAddress ResolveIPAddress(string addrOrHost, byte[] subnet, AddressFamily family)
        {
            IPAddress loopback = (family == AddressFamily.InterNetwork) ? IPAddress.Loopback : IPAddress.IPv6Loopback;

            if (addrOrHost.Equals("loopback", StringComparison.OrdinalIgnoreCase) ||
                addrOrHost.Equals("localhost", StringComparison.OrdinalIgnoreCase))
            {
                return loopback;
            }
            else if (addrOrHost == "0.0.0.0")
            {
                return IPAddress.Any;
            }
            else
            {
                // IF the address is an empty string, default to the local machine, but not the loopback address
                if (String.IsNullOrEmpty(addrOrHost))
                {
                    addrOrHost = Dns.GetHostName();
                }

                List<IPAddress> candidates = new List<IPAddress>();
                IPAddress[] nodeIps = Dns.GetHostAddresses(addrOrHost);
                for (int n = 0; n < nodeIps.Length; n++)
                {
                    IPAddress nodeIp = nodeIps[n];
                    if (nodeIp.AddressFamily == family && !nodeIp.Equals(loopback))
                    {
                        // If the subnet does not match - we can't resolve this address.
                        // If subnet is not specified - pick smallest address deterministically.
                        if (subnet == null)
                        {
                            candidates.Add(nodeIp);
                        }
                        else
                        {
                            if (subnet.Select((b, i) => nodeIp.GetAddressBytes()[i] == b).All(x => x))
                            {
                                candidates.Add(nodeIp);
                            }
                        }
                    }
                }
                if (candidates.Count > 0)
                {
                    return PickIPAddress(candidates);
                }
                string subnetStr = Utils.IEnumerableToString(subnet, null, ".", false);
                throw new ArgumentException("Hostname '" + addrOrHost + "' with subnet " + subnetStr + " and family " + family + " is not a valid IP address or DNS name");
            }
        }

        private static IPAddress PickIPAddress(List<IPAddress> candidates)
        {
            IPAddress chosen = null;
            for (int i = 0; i < candidates.Count; i++)
            {
                if (chosen == null)
                {
                    chosen = candidates[i];
                }
                else
                {
                    if(CompareIPAddresses(candidates[i], chosen)) // pick smallest address deterministically
                        chosen = candidates[i];
                }
            }
            return chosen;
        }

        // returns true if lhs is "less" (in some repeatable sense) than rhs
        private static bool CompareIPAddresses(IPAddress lhs, IPAddress rhs)
        {
            byte[] lbytes = lhs.GetAddressBytes();
            byte[] rbytes = rhs.GetAddressBytes();

            if (lbytes.Length == rbytes.Length)
            {
                // compare starting from most significant octet.
                // 10.68.20.21 < 10.98.05.04
                for (int i = 0; i < lbytes.Length; i++) 
                {
                    if (lbytes[i] != rbytes[i])
                    {
                        return lbytes[i] < rbytes[i];
                    }
                }
                // They're equal
                return false;
            }
            else
            {
                return lbytes.Length < rbytes.Length;
            }
        }

        /// <summary>
        /// Gets the address of the local server.
        /// If there are multiple addresses in the correct family in the server's DNS record, the first will be returned.
        /// </summary>
        /// <returns>The server's IPv4 address.</returns>
        internal static IPAddress GetLocalIPAddress(AddressFamily family = AddressFamily.InterNetwork, string interfaceName = null)
        {
            IPAddress loopback = (family == AddressFamily.InterNetwork) ? IPAddress.Loopback : IPAddress.IPv6Loopback;
            // get list of all network interfaces
            NetworkInterface[] netInterfaces = NetworkInterface.GetAllNetworkInterfaces();

            List<IPAddress> candidates = new List<IPAddress>();
            // loop through interfaces
            for (int i=0; i < netInterfaces.Length; i++)
            {
                NetworkInterface netInterface = netInterfaces[i];
                
                if (netInterface.OperationalStatus != OperationalStatus.Up)
                {
                    // Skip network interfaces that are not operational
                    continue;
                }
                if (string.IsNullOrWhiteSpace(interfaceName)
                    || netInterface.Name.StartsWith(interfaceName, StringComparison.Ordinal))
                {
                    bool isLoopbackInterface = (i == NetworkInterface.LoopbackInterfaceIndex);
                    // get list of all unicast IPs from current interface
                    UnicastIPAddressInformationCollection ipAddresses = netInterface.GetIPProperties().UnicastAddresses;

                    // loop through IP address collection
                    foreach (UnicastIPAddressInformation ip in ipAddresses)
                    {
                        if (ip.Address.AddressFamily == family) // TODO: picking the first address of the requested family for now. Will need to revisit later
                        {
                            //don't pick loopback address, unless we were asked for a loopback interface
                            if(!(isLoopbackInterface && ip.Address.Equals(loopback)))
                            {
                                candidates.Add(ip.Address); // collect all candidates.
                            }
                        }
                    }
                }
            }
            if (candidates.Count > 0)
            {
                return OrleansConfiguration.PickIPAddress(candidates);
            }
            throw new OrleansException("Failed to get a local IP address.");
        }

        private void LoadDeployment(XmlElement root)
        {
            // nothing now
        }

        private static XmlElement ParseXml(TextReader input)
        {
            var doc = new XmlDocument();
            var xmlReader = XmlReader.Create(input);
            doc.Load(xmlReader);
            return doc.DocumentElement;
        }

        internal static bool IsNet45OrNewer()
        {
            // From: http://stackoverflow.com/questions/8517159/how-to-detect-at-runtime-that-net-version-4-5-currently-running-your-code

            // Class "ReflectionContext" exists from .NET 4.5 onwards.
            return Type.GetType("System.Reflection.ReflectionContext", false) != null;
        }
    }
}
