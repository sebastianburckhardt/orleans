using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Xml;
using Orleans.Counters;

namespace Orleans
{
    /// <summary>
    /// Orleans client configuration parameters.
    /// </summary>
    public class ClientConfiguration : MessagingConfiguration, ITraceConfiguration, IStatisticsConfiguration, ILimitsConfiguration
    {
        /// <summary>
        /// Specifies the type of the gateway provider.
        /// </summary>
        public enum GatewayProviderType
        {
            None,               // 
            AzureTable,              // use Azure, requires Azure element
#if !DISABLE_WF_INTEGRATION
            WindowsFabric,      // use WindowsFabric, requires WindowsFabric element
#endif
            Config              // use Config based static list, requires Config element(s)
        }

        /// <summary>
        /// The name of this client.
        /// </summary>
        public static string ClientName = "Client";
        private const string Azure_AppRoot_Dir = "approot";
        private const string Current_Dir = ".";

        private string traceFilePattern;
        private readonly DateTime creationTimestamp;

        public string SourceFile { get; private set; }

        /// <summary>
        /// The list fo the gateways to use.
        /// Each GatewayNode element specifies an outside grain client gateway node.
        /// If outside (non-Orleans) clients are to connect to the Orleans system, then at least one gateway node must be specified.
        /// Additional gateway nodes may be specified if desired, and will add some failure resilience and scalability.
        /// If multiple gateways are specified, then each client will select one from the list at random.
        /// </summary>
        public List<IPEndPoint> Gateways { get; set; }
        /// <summary>
        /// </summary>
        public int PreferedGatewayIndex { get; set; }
        /// <summary>
        /// </summary>
        public GatewayProviderType GatewayProvider { get; set; }
        /// <summary>
        /// Specifies a unique identifier of this deployment.
        /// If the silos are deployed on Azure (run as workers roles), deployment id is set automatically by Azure runtime, 
        /// accessible to the role via RoleEnvironment.DeploymentId static variable and is passed to the silo automatically by the role via config. 
        /// So if the silos are run as Azure roles this variable should not be specified in the OrleansConmfiguration.xml (it will be overwritten if specified).
        /// If the silos are deployed on the cluster and not as Azure roles, this variable should be set by a deployment script in the OrleansConmfiguration.xml file.
        /// </summary>
        public string DeploymentId { get; set; }
        /// <summary>
        /// Specifies the connection string for azure storage account.
        /// If the silos are deployed on Azure (run as workers roles), DataConnectionString may be specified via RoleEnvironment.GetConfigurationSettingValue("DataConnectionString");
        /// In such a case it is taken from there and passed to the silo automatically by the role via config.
        /// So if the silos are run as Azure roles and this config is specified via RoleEnvironment, 
        /// this variable should not be specified in the OrleansConmfiguration.xml (it will be overwritten if specified).
        /// If the silos are deployed on the cluster and not as Azure roles,  this variable should be set in the OrleansConmfiguration.xml file.
        /// If not set at all, DevelopmentStorageAccount will be used.
        /// </summary>
        public string DataConnectionString { get; set; }
#if !DISABLE_WF_INTEGRATION
        public Uri WindowsFabricServiceName { get; set; }
#endif

        public OrleansLogger.Severity DefaultTraceLevel { get; set; }
        public List<Tuple<string, OrleansLogger.Severity>> TraceLevelOverrides { get; private set; }
        public bool WriteMessagingTraces { get; set; }
        public bool TraceToConsole { get; set; }
        public int LargeMessageWarningThreshold { get; set; }
        public bool PropagateActivityId { get; set; }
        public int BulkMessageLimit { get; set; }

        /// <summary>
        /// </summary>
        public AddressFamily PreferredFamily { get; set; }
        /// <summary>
        /// The Interface attribute specifies the name of the network interface to use to work out an IP address for this machine.
        /// </summary>
        public string NetInterface { get; private set; }
        /// <summary>
        /// The Port attribute specifies the specific listen port for this client machine.
        /// If value is zero, then a random machine-assigned port number will be used.
        /// </summary>
        public int Port { get; private set; }
        /// <summary>
        /// </summary>
        public string DNSHostName { get; private set; } // This is a true host name, no IP address. It is NOT settable, equals Dns.GetHostName().
        /// <summary>
        /// </summary>
        public TimeSpan GatewayListRefreshPeriod { get; set; }
        public TimeSpan StatisticsMetricsTableWriteInterval { get; set; }
        public TimeSpan StatisticsPerfCountersWriteInterval { get; set; }
        public TimeSpan StatisticsLogWriteInterval { get; set; }
        public bool StatisticsWriteLogStatisticsToTable { get; set; }
        public StatisticsLevel StatisticsCollectionLevel { get; set; }

        public IDictionary<string, LimitValue> LimitValues { get; private set; }

        private static readonly TimeSpan DEFAULT_GW_LIST_REFRESH_PERIOD = TimeSpan.FromMinutes(5);
        private static readonly TimeSpan DEFAULT_STATS_METRICS_TABLE_WRITE_PERIOD = TimeSpan.FromSeconds(30);
        private static readonly TimeSpan DEFAULT_STATS_PERF_COUNTERS_WRITE_PERIOD = Constants.INFINITE_TIMESPAN;
        private static readonly TimeSpan DEFAULT_STATS_LOG_WRITE_PERIOD = TimeSpan.FromMinutes(5);

        /// <summary>
        /// </summary>
        public bool UseAzureStorage { get { return !String.IsNullOrWhiteSpace(DeploymentId) && !String.IsNullOrWhiteSpace(DataConnectionString); } }
#if !DISABLE_WF_INTEGRATION
        private bool HasWindowsFabricElement { get { return WindowsFabricServiceName != null; } }
#endif
        private bool HasStaticGWs { get { return Gateways != null && Gateways.Count > 0; } }
        /// <summary>
        /// </summary>
        public Dictionary<string, ProviderCategoryConfiguration> ProviderConfigurations { get; set; }

        public string TraceFilePattern
        {
            get { return traceFilePattern; }
            set
            {
                traceFilePattern = value;
                ConfigUtilities.SetTraceFileName(this, ClientName, this.creationTimestamp);
            }
        }
        public string TraceFileName { get; set; }

        private static readonly string[] DefaultConfigPaths = { "OrleansClientConfiguration.xml", "OrleansClient.config", "ClientConfiguration.xml", "OrleansClient.xml" };

        /// <summary>
        /// </summary>
        public ClientConfiguration()
            : base(false)
        {
            creationTimestamp = DateTime.UtcNow;
            SourceFile = null;
            PreferedGatewayIndex = -1;
            Gateways = new List<IPEndPoint>();
            GatewayProvider = GatewayProviderType.None;
            PreferredFamily = AddressFamily.InterNetwork;
            NetInterface = null;
            Port = 0;
            DNSHostName = Dns.GetHostName();
            DeploymentId = Environment.UserName;
            DataConnectionString = "";

            DefaultTraceLevel = OrleansLogger.Severity.Info;
            TraceLevelOverrides = new List<Tuple<string, OrleansLogger.Severity>>();
            TraceToConsole = true;
            TraceFilePattern = "{0}-{1}.log";
            WriteMessagingTraces = false;
            LargeMessageWarningThreshold = Constants.LARGE_OBJECT_HEAP_THRESHOLD;
            PropagateActivityId = Constants.DEFAULT_PROPAGATE_E2E_ACTIVITY_ID;
            BulkMessageLimit = Constants.DEFAULT_LOGGER_BULK_MESSAGE_LIMIT;

            GatewayListRefreshPeriod = DEFAULT_GW_LIST_REFRESH_PERIOD;
            StatisticsMetricsTableWriteInterval = DEFAULT_STATS_METRICS_TABLE_WRITE_PERIOD;
            StatisticsPerfCountersWriteInterval = DEFAULT_STATS_PERF_COUNTERS_WRITE_PERIOD;
            StatisticsLogWriteInterval = DEFAULT_STATS_LOG_WRITE_PERIOD;
            StatisticsWriteLogStatisticsToTable = true;
            StatisticsCollectionLevel = NodeConfiguration.DEFAULT_STATS_COLLECTION_LEVEL;
            LimitValues = new Dictionary<string, LimitValue>();
            ProviderConfigurations = new Dictionary<string, ProviderCategoryConfiguration>();
        }

        /// <summary>
        /// </summary>
        public LimitValue GetLimit(string name)
        {
            LimitValue limit;
            LimitValues.TryGetValue(name, out limit);
            return limit;
        }

        internal void Load(TextReader input)
        {
            XmlDocument xml = new XmlDocument();
            var xmlReader = XmlReader.Create(input);
            xml.Load(xmlReader);
            XmlElement root = xml.DocumentElement;

            foreach (XmlNode node in root.ChildNodes)
            {
                XmlElement child = node as XmlElement;
                if (child != null)
                {
                    switch (child.LocalName)
                    {
                        case "GatewayProvider":
                            if (child.HasAttribute("ProviderType"))
                            {
                                GatewayProvider = (GatewayProviderType)Enum.Parse(typeof(GatewayProviderType), child.GetAttribute("ProviderType"));
                            }
                            break;
                        case "Gateway":
                            Gateways.Add(ConfigUtilities.ParseIPEndPoint(child));
                            break;
                        case "Azure":
                            if (child.HasAttribute("DeploymentId"))
                            {
                                DeploymentId = child.GetAttribute("DeploymentId");
                            }
                            if (child.HasAttribute("DataConnectionString"))
                            {
                                DataConnectionString = child.GetAttribute("DataConnectionString");
                                if (String.IsNullOrWhiteSpace(DataConnectionString))
                                {
                                    throw new FormatException("Azure.DataConnectionString cannot be blank");
                                }
                            }
                            break;
#if !DISABLE_WF_INTEGRATION
                        case "WindowsFabric":
                            if (child.HasAttribute("WindowsFabricServiceName"))
                            {
                                WindowsFabricServiceName = new Uri(child.GetAttribute("WindowsFabricServiceName"));
                            }
                            break;
#endif
                        case "Tracing":
                            ConfigUtilities.ParseTracing(this, child, ClientName);
                            break;
                        case "Statistics":
                            ConfigUtilities.ParseStatistics(this, child, ClientName);
                            break;
                        case "Limits":
                            ConfigUtilities.ParseLimitValues(this, child, ClientName);
                            break;
                        case "Debug":
                            break;
                        case "Messaging":
                            base.Load(child);
                            break;
                        case "LocalAddress":
                            if (child.HasAttribute("PreferredFamily"))
                            {
                                PreferredFamily = ConfigUtilities.ParseEnum<AddressFamily>(child.GetAttribute("PreferredFamily"),
                                    "Invalid address family for the PreferredFamily attribute on the LocalAddress element");
                            }
                            else
                            {
                                throw new FormatException("Missing PreferredFamily attribute on the LocalAddress element");
                            }
                            if (child.HasAttribute("Interface"))
                            {
                                NetInterface = child.GetAttribute("Interface");
                            }
                            if (child.HasAttribute("Port"))
                            {
                                Port = ConfigUtilities.ParseInt(child.GetAttribute("Port"),
                                    "Invalid integer value for the Port attribute on the LocalAddress element");
                            }
                            break;
                        default:
                            if (child.LocalName.EndsWith("Providers", StringComparison.Ordinal))
                            {
                                var providerConfig = new ProviderCategoryConfiguration();
                                providerConfig.Load(child);
                                ProviderConfigurations.Add(providerConfig.Name, providerConfig);
                            }
                            break;
                    }
                }
            }

            //if (Gateways.Count == 0)
            //{
            //    throw new FormatException("Client configuration must contain at least one Gateway element");
            //}
        }

        /// <summary>
        /// </summary>
        public static ClientConfiguration LoadFromFile(string fileName)
        {
            if (fileName == null) return null;

            TextReader input = null;
            try
            {
                ClientConfiguration config = new ClientConfiguration();
                input = File.OpenText(fileName);
                config.Load(input);
                config.SourceFile = fileName;
                return config;
            }
            finally
            {
                if (input != null) input.Close();
            }
        }

        internal void AdjustConfiguration()
        {
            GlobalConfiguration.AdjustConfiguration(ProviderConfigurations, DeploymentId);
        }

        /// <summary>
        /// Loads the configuration from the standard paths, looking up the directory hierarchy
        /// </summary>
        /// <returns>Client configuration data if a configuration file was found.</returns>
        /// <exception cref="FileNotFoundException">Thrown if no configuration file could be found in any of the standard locations</exception>
        public static ClientConfiguration StandardLoad()
        {
            String dir = Azure_AppRoot_Dir; // Look in the Azure approot dir first
            while (true)
            {
                foreach (string s in DefaultConfigPaths)
                {
                    if (File.Exists(Path.Combine(dir, s)))
                    {
                        return LoadFromFile(Path.Combine(dir, s));
                    }
                }

                if (Current_Dir == dir)
                {
                    // Then look in application directory
                    dir = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
                    continue;
                }
                if (Azure_AppRoot_Dir == dir)
                {
                    // Then look in current directory
                    dir = Current_Dir;
                    continue;
                }

                if (Directory.GetParent(dir) == null)
                    break;
                dir = Path.Combine(dir, "..");
            }
            throw new FileNotFoundException("Cannot locate Orleans client config file");
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();

            sb.AppendLine("Client Configuration:");
            sb.Append("   Config File Name: ").AppendLine(SourceFile == null ? "" : Path.GetFullPath(SourceFile));

            sb.Append("   Gateway Provider: ").Append(GatewayProvider);
            if (GatewayProvider == GatewayProviderType.None)
            {
                sb.Append(".   Gateway Provider that will be used instead: ").Append(GatewayProviderToUse);
            }
            sb.AppendLine();
            if (Gateways != null && Gateways.Count > 0 )
            {
                sb.AppendFormat("   Gateways[{0}]:", Gateways.Count).AppendLine();
                foreach (var endpoint in Gateways)
                {
                    sb.Append("      ").AppendLine(endpoint.ToString());
                }
            }
            else
            {
                sb.Append("   Gateways: ").AppendLine("Unspecified");
            }
            sb.Append("   Preferred Gateway Index: ").AppendLine(PreferedGatewayIndex.ToString());
            if (Gateways != null && PreferedGatewayIndex >= 0 && PreferedGatewayIndex < Gateways.Count)
            {
                sb.Append("   Preferred Gateway Address: ").AppendLine(Gateways[PreferedGatewayIndex].ToString());
            }
            sb.Append("   GatewayListRefreshPeriod: ").Append(GatewayListRefreshPeriod).AppendLine();
            if (!String.IsNullOrEmpty(DeploymentId) || !String.IsNullOrEmpty(DataConnectionString))
            {
                sb.Append("   Azure:").AppendLine();
                sb.Append("      DeploymentId: ").Append(DeploymentId).AppendLine();
                string dataConnectionInfo = ConfigUtilities.PrintDataConnectionInfo(DataConnectionString); // Don't print Azure account keys in log files
                sb.Append("      DataConnectionString: ").Append(dataConnectionInfo).AppendLine();
            }
#if !DISABLE_WF_INTEGRATION
            if (WindowsFabricServiceName != null)
            {
                sb.Append("   Windows Fabric:").AppendLine();
                sb.Append("      Service Name: ").Append(WindowsFabricServiceName).AppendLine();
            }
#endif
            if (!string.IsNullOrWhiteSpace(NetInterface))
            {
                sb.Append("   Network Interface: ").AppendLine(NetInterface);
            }
            if (Port != 0)
            {
                sb.Append("   Network Port: ").Append(Port).AppendLine();
            }
            sb.Append("   Preferred Address Family: ").AppendLine(PreferredFamily.ToString());
            sb.Append("   DNS Host Name: ").AppendLine(DNSHostName);
            sb.Append("   Client Name: ").AppendLine(ClientName);
            sb.Append(ConfigUtilities.ITraceConfigurationToString(this));
            sb.Append(ConfigUtilities.IStatisticsConfigurationToString(this));
            if (LimitValues.Count > 0)
            {
                sb.Append("   Limits Values: ").AppendLine();
                foreach (var limit in LimitValues.Values)
                {
                    sb.AppendFormat("       {0}", limit).AppendLine();
                }
            }
            sb.AppendFormat(base.ToString());
            sb.AppendFormat("   Providers:").AppendLine();
            sb.Append(GlobalConfiguration.PrintProviderConfigurations(ProviderConfigurations));
            return sb.ToString();
        }

        internal GatewayProviderType GatewayProviderToUse
        {
            get
            {
                // order is important here for establishing defaults.
                if (GatewayProvider != GatewayProviderType.None) return GatewayProvider;
                if (UseAzureStorage) return GatewayProviderType.AzureTable;
                //if (HasWindowsFabricElement) return GatewayProviderType.WindowsFabric;
                if (HasStaticGWs) return GatewayProviderType.Config;
                return GatewayProviderType.None;
            }
        }

        internal void CheckGatewayProviderSettings()
        {
            if (GatewayProvider == GatewayProviderType.AzureTable)
            {
                if(!UseAzureStorage) 
                {
                    throw new ArgumentException("Config specifies Azure based GatewayProviderType, but Azure element is not specified or not complete.", "GatewayProvider");
                }
            }
#if !DISABLE_WF_INTEGRATION
            else if (GatewayProvider == GatewayProviderType.WindowsFabric)
            {
                if(!HasWindowsFabricElement) 
                {
                    throw new ArgumentException("Config specifies WindowsFabric based GatewayProviderType, but WindowsFabric element is not specified or not complete.", "GatewayProvider");
                }
            }
#endif
            else if (GatewayProvider == GatewayProviderType.Config)
            {
                if (!HasStaticGWs) 
                {
                    throw new ArgumentException("Config specifies Config based GatewayProviderType, but Gateway element(s) is/are not specified.", "GatewayProvider");
                }
            }
            else if (GatewayProvider == GatewayProviderType.None)
            {

#if !DISABLE_WF_INTEGRATION
                if (!UseAzureStorage && !HasWindowsFabricElement && !HasStaticGWs) 
                {
                    throw new ArgumentException("Config does not specify GatewayProviderType, and also does not have the adequate defaults: no Azure, WindowsFabric or Gateway element(s) are specified.", "GatewayProvider");
                }
#else
                if (!UseAzureStorage && !HasStaticGWs)
                {
                    throw new ArgumentException("Config does not specify GatewayProviderType, and also does not have the adequate defaults: no Azure and or Gateway element(s) are specified.", "GatewayProvider");
                }
#endif
            }
        }
    }
}
