using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Net;
using System.Xml;
using Orleans.AzureUtils;
using Orleans.Providers;
using Orleans.RuntimeCore.Configuration;

namespace Orleans
{
    // helper utility class to handle default vs. explicitly set config value.
    [Serializable]
    internal class ConfigValue<T>
    {
        public T Value;
        public bool IsDefaultValue;

        public ConfigValue(T val, bool isDefaultValue)
        {
            Value = val;
            IsDefaultValue = isDefaultValue;
        }
    }

    /// <summary>
    /// Data object holding Silo global configuration parameters.
    /// </summary>
    [Serializable]
    public class GlobalConfiguration : MessagingConfiguration
    {
        /// <summary>
        /// Liveness configuration that controls the type of the liveness protocol that silo use for membership.
        /// </summary>
        public enum LivenessProviderType
        {
            /// <summary>Default value to allow discrimination of override values.</summary>
            NotSpecified,
            /// <summary>Grain is used to store membership information. 
            /// This option is not reliable and thus should only be used in local development setting.</summary>
            MembershipTableGrain,
            //File,
            /// <summary>AzureTable is used to store membership information. 
            /// This option should be used in production.</summary>
            AzureTable,
#if !DISABLE_WF_INTEGRATION
            WindowsFabricNamingService,
#endif
        }

        /// <summary>
        /// Reminders configuration that controls the type of the protocol that silo use to implement Reminders.
        /// </summary>
        public enum ReminderServiceProviderType
        {
            /// <summary>Default value to allow discrimination of override values.</summary>
            NotSpecified,
            /// <summary>Grain is used to store reminders information. 
            /// This option is not reliable and thus should only be used in local development setting.</summary>
            ReminderTableGrain,
            /// <summary>AzureTable is used to store reminders information. 
            /// This option should be used in production.</summary>
            AzureTable,
        }

        /// <summary>
        /// Configuration type that controls the type of the grain directory caching algorithm that silo use.
        /// </summary>
        public enum DirectoryCachingStrategyType
        {
            /// <summary>Don't cache.</summary>
            None,
            /// <summary>Standard fixed-size LRU.</summary>
            LRU,
            /// <summary>Adaptive caching with fixed maximum size and refresh. This option should be used in production.</summary>
            Adaptive
        }

        public ApplicationConfiguration Application { get; private set; }

        /// <summary>
        /// SeedNodes are only used in local development setting with LivenessProviderType.MembershipTableGrain
        /// SeedNodes are never used in production.
        /// </summary>
        public List<IPEndPoint> SeedNodes { get; private set; }

        /// <summary>
        /// The subnet on which the silos run. 
        /// This option should only be used when running on multi-homed cluster. It should not be used when running in Azure.
        /// </summary>
        public byte[] Subnet { get; set; }

        /// <summary>
        /// Determines if primary node is required to be configured as a seed node.
        /// True if LivenessType is set to LivenessProviderType.MembershipTableGrain, faklse otherwise.
        /// </summary>
        public bool PrimaryNodeIsRequired
        {
            get { return LivenessType == LivenessProviderType.MembershipTableGrain; }
        }

        /// <summary>
        /// The LivenessType attribute controls the liveness method used for silo reliability.
        /// </summary>
        public LivenessProviderType LivenessType { get; set; }
        /// <summary>
        /// Global switch to disbale silo liveness protocol (should be used only for testing).
        /// The LivenessEnabled attribute, if provided and set to "false", suppresses liveness enforcement.
        /// If a silo is suspected to be dead, but this attribute is set to "false", the suspicions will not propagated to the system and enforced,
        /// This parameter is intended for use only for testing and troubleshooting.
        /// In production, liveness should always be enabled.
        /// Default is true (eanabled)
        /// </summary>
        public bool LivenessEnabled { get; set; }
        /// <summary>
        /// The number of seconds to periodically probe other silos for their liveness or for the silo to send "I am alive" heartbeat  messages about itself.
        /// </summary>
        public TimeSpan ProbeTimeout { get; set; }
        /// <summary>
        /// The number of seconds to periodically fetch updates from the membership table.
        /// </summary>
        public TimeSpan TableRefreshTimeout { get; set; }
        /// <summary>
        /// Expiration time in seconds for death vote in the membership table.
        /// </summary>
        public TimeSpan DeathVoteExpirationTimeout { get; set; }
        /// <summary>
        /// The number of seconds to periodically write in the membership table that this silo is alive. Used ony for diagnostics.
        /// </summary>
        public TimeSpan IAmAliveTablePublishTimeout { get; set; }
        /// <summary>
        /// The number of seconds to attempt to join a cluster of silos before giving up.
        /// </summary>
        public TimeSpan MaxJoinAttemptTime { get; set; }
        internal ConfigValue<int> ExpectedClusterSize_CV { get; set; }
        /// <summary>
        /// The expected size of a cluster. Need not be very accurate, can be an overestimate.
        /// </summary>
        public int ExpectedClusterSize { get { return ExpectedClusterSize_CV.Value; } set { ExpectedClusterSize_CV = new ConfigValue<int>(value, false); } }
        /// <summary>
        /// The number of missed "I am alive" heartbeat messages from a silo or number of un-replied probes that lead to suspecting this silo as dead.
        /// </summary>
        public int NumMissedProbesLimit { get; set; }
        /// <summary>
        /// The number of silos each silo probes for liveness.
        /// </summary>
        public int NumProbedSilos { get; set; }
        /// <summary>
        /// The number of non-expired votes that are needed to declare some silo as dead (should be at most NumMissedProbesLimit)
        /// </summary>
        public int NumVotesForDeathDeclaration { get; set; }
        /// <summary>
        /// The number of missed "I am alive" updates  in the table from a silo that causes warning to be logged. Does not impact the liveness protocol.
        /// </summary>
        public int NumMissedTableIAmAliveLimit { get; set; }
        /// <summary>
        /// Whether to use the gossip optimization to speed up spreading liveness information.
        /// </summary>
        public bool UseLivenessGossip { get; set; }

        /// <summary>
        /// Azure DeploymentId.
        /// </summary>
        public string DeploymentId { get; set; }
        /// <summary>
        /// Azure DataConnectionString for storage connection.
        /// </summary>
        public string DataConnectionString { get; set; }

        public TimeSpan CollectionQuantum { get; set; }

        /// <summary>
        /// The CacheSize attribute specifies the maximum number of grains to cache directory information for.
        /// </summary>
        public int CacheSize { get; set; }
        /// <summary>
        /// The InitialTTL attribute specifies the initial (minimum) time, in seconds, to keep a cache entry before revalidating.
        /// </summary>
        public TimeSpan InitialCacheTTL { get; set; }
        /// <summary>
        /// The MaximumTTL attribute specifies the maximum time, in seconds, to keep a cache entry before revalidating.
        /// </summary>
        public TimeSpan MaximumCacheTTL { get; set; }
        /// <summary>
        /// The TTLExtensionFactor attribute specifies the factor by which cache entry TTLs should be extended when they are found to be stable.
        /// </summary>
        public double CacheTTLExtensionFactor { get; set; }

        internal int DirectoryReplicationFactor { get; set; }
        internal TimeSpan DirectoryReplicationPeriod { get; set; }
        /// <summary>
        /// The DirectoryCachingStrategy attribute specifies the caching strategy to use.
        /// The options are None, which means don't cache directory entries locally;
        /// LRU, which indicates that a standard fixed-size least recently used strategy should be used; and
        /// Adaptive, which indicates that an adaptive strategy with a fixed maximum size should be used.
        /// The Adaptive strategy is used by default.
        /// </summary>
        public DirectoryCachingStrategyType DirectoryCachingStrategy { get; set; }
        /// <summary>
        /// The ReminderServiceType attribute controls the type of the reminder service implementation used by silos.
        /// </summary>
        public ReminderServiceProviderType ReminderServiceType { get; set; }

        /// <summary>
        /// Configuration for various runtime providers.
        /// </summary>
        public Dictionary<string, ProviderCategoryConfiguration> ProviderConfigurations { get; set; }

        /// <summary>
        /// The time span between when we have added an entry for an activation to the grain directory and when we are allowed
        /// to conditionally remove that entry. 
        /// Conditional deregistration is used for lazy clean-up of activations whose prompt deregistration failed for some reason (e.g., message failure).
        /// This should always be at least one minute, since we compare the times on the directory partition, so message delays and clcks skues have
        /// to be allowed.
        /// </summary>
        public TimeSpan DirectoryLazyDeregistrationDelay { get; set; }

        internal bool PerformDeadlockDetection { get; set; }

        internal bool RunsInAzure { get { return !String.IsNullOrWhiteSpace(DeploymentId) && !String.IsNullOrWhiteSpace(DataConnectionString); } }

        private static readonly LivenessProviderType DEFAULT_LIVENESS_TYPE = LivenessProviderType.MembershipTableGrain;
        private static readonly TimeSpan DEFAULT_LIVENESS_PROBE_TIMEOUT = TimeSpan.FromSeconds(10);
        private static readonly TimeSpan DEFAULT_LIVENESS_TABLE_REFRESH_TIMEOUT = TimeSpan.FromSeconds(60);
        private static readonly TimeSpan DEFAULT_LIVENESS_DEATH_VOTE_EXPIRATION_TIMEOUT = TimeSpan.FromSeconds(120);
        private static readonly TimeSpan DEFAULT_LIVENESS_I_AM_ALIVE_TABLE_PUBLISH_TIMEOUT = TimeSpan.FromMinutes(5);
        private static readonly TimeSpan DEFAULT_LIVENESS_MAX_JOIN_ATTEMPT_TIME = TimeSpan.FromMinutes(5); // 5 min
        private static readonly int DEFAULT_LIVENESS_NUM_MISSED_PROBES_LIMIT = 3;
        private static readonly int DEFAULT_LIVENESS_NUM_PROBED_SILOS = 3;
        private static readonly int DEFAULT_LIVENESS_NUM_VOTES_FOR_DEATH_DECLARATION = 2;
        private static readonly int DEFAULT_LIVENESS_NUM_TABLE_I_AM_ALIVE_LIMIT = 2;
        private static readonly bool DEFAULT_LIVENESS_USE_LIVENESS_GOSSIP = true;
        private static readonly int DEFAULT_LIVENESS_EXPECTED_CLUSTER_SIZE = 20;
        private static readonly int DEFAULT_CACHE_SIZE = 1000000;
        private static readonly TimeSpan DEFAULT_INITIAL_CACHE_TTL = TimeSpan.FromSeconds(30);
        private static readonly TimeSpan DEFAULT_MAXIMUM_CACHE_TTL = TimeSpan.FromSeconds(240);
        private static readonly double DEFAULT_TTL_EXTENSION_FACTOR = 2.0;
        private static readonly DirectoryCachingStrategyType DEFAULT_DIRECTORY_CACHING_STRATEGY =
            DirectoryCachingStrategyType.Adaptive;
        internal static readonly TimeSpan DEFAULT_COLLECTION_QUANTUM = TimeSpan.FromMinutes(1);
        private static readonly int DEFAULT_DIRECTORY_REPLICATION_FACTOR = 0;
        private static readonly TimeSpan DEFAULT_DIRECTORY_REPLICATION_PERIOD = TimeSpan.FromSeconds(10);
        private static readonly TimeSpan DEFAULT_UNREGISTER_RACE_DELAY = TimeSpan.FromMinutes(1);
        private static readonly TimeSpan DEFAULT_COMPLETION_INTERVAL = TimeSpan.FromMilliseconds(100);
        public static readonly bool DEFAULT_PERFORM_DEADLOCK_DETECTION = false;
        
        internal GlobalConfiguration() : base(true)
        {
            Application = new ApplicationConfiguration();
            SeedNodes = new List<IPEndPoint>();
            LivenessType = DEFAULT_LIVENESS_TYPE;
            LivenessEnabled = true;
            ProbeTimeout = DEFAULT_LIVENESS_PROBE_TIMEOUT;
            TableRefreshTimeout = DEFAULT_LIVENESS_TABLE_REFRESH_TIMEOUT;
            DeathVoteExpirationTimeout = DEFAULT_LIVENESS_DEATH_VOTE_EXPIRATION_TIMEOUT;
            IAmAliveTablePublishTimeout = DEFAULT_LIVENESS_I_AM_ALIVE_TABLE_PUBLISH_TIMEOUT;
            NumMissedProbesLimit = DEFAULT_LIVENESS_NUM_MISSED_PROBES_LIMIT;
            NumProbedSilos = DEFAULT_LIVENESS_NUM_PROBED_SILOS;
            NumVotesForDeathDeclaration = DEFAULT_LIVENESS_NUM_VOTES_FOR_DEATH_DECLARATION;
            NumMissedTableIAmAliveLimit = DEFAULT_LIVENESS_NUM_TABLE_I_AM_ALIVE_LIMIT;
            UseLivenessGossip = DEFAULT_LIVENESS_USE_LIVENESS_GOSSIP;
            MaxJoinAttemptTime = DEFAULT_LIVENESS_MAX_JOIN_ATTEMPT_TIME;
            ExpectedClusterSize_CV = new ConfigValue<int>(DEFAULT_LIVENESS_EXPECTED_CLUSTER_SIZE, true);
            DeploymentId = Environment.UserName;
            DataConnectionString = "";

            CollectionQuantum = DEFAULT_COLLECTION_QUANTUM;

            CacheSize = DEFAULT_CACHE_SIZE;
            InitialCacheTTL = DEFAULT_INITIAL_CACHE_TTL;
            MaximumCacheTTL = DEFAULT_MAXIMUM_CACHE_TTL;
            CacheTTLExtensionFactor = DEFAULT_TTL_EXTENSION_FACTOR;
            DirectoryCachingStrategy = DEFAULT_DIRECTORY_CACHING_STRATEGY;
            DirectoryReplicationFactor = DEFAULT_DIRECTORY_REPLICATION_FACTOR;
            DirectoryReplicationPeriod = DEFAULT_DIRECTORY_REPLICATION_PERIOD;
            DirectoryLazyDeregistrationDelay = DEFAULT_UNREGISTER_RACE_DELAY;

            PerformDeadlockDetection = DEFAULT_PERFORM_DEADLOCK_DETECTION;
            ReminderServiceType = ReminderServiceProviderType.NotSpecified;

            ProviderConfigurations = new Dictionary<string, ProviderCategoryConfiguration>();
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("   Subnet: ").Append(Subnet == null ? "" : Subnet.ToStrings(x => x.ToString(CultureInfo.InvariantCulture), ".")).AppendLine();
            sb.Append("   Seed nodes: ");
            bool first = true;
            foreach (IPEndPoint node in SeedNodes)
            {
                if (!first)
                {
                    sb.Append(", ");
                }
                sb.Append(node.ToString());
                first = false;
            }
            sb.AppendLine();
            sb.AppendFormat(base.ToString());
            sb.AppendFormat("   Liveness:").AppendLine();
            sb.AppendFormat("      LivenessType: {0}", LivenessType).AppendLine();
            sb.AppendFormat("      LivenessEnabled: {0}", LivenessEnabled).AppendLine();
            sb.AppendFormat("      ProbeTimeout: {0}", ProbeTimeout).AppendLine();
            sb.AppendFormat("      TableRefreshTimeout: {0}", TableRefreshTimeout).AppendLine();
            sb.AppendFormat("      DeathVoteExpirationTimeout: {0}", DeathVoteExpirationTimeout).AppendLine();
            sb.AppendFormat("      NumMissedProbesLimit: {0}", NumMissedProbesLimit).AppendLine();
            sb.AppendFormat("      NumProbedSilos: {0}", NumProbedSilos).AppendLine();
            sb.AppendFormat("      NumVotesForDeathDeclaration: {0}", NumVotesForDeathDeclaration).AppendLine();
            sb.AppendFormat("      UseLivenessGossip: {0}", UseLivenessGossip).AppendLine();
            sb.AppendFormat("      IAmAliveTablePublishTimeout: {0}", IAmAliveTablePublishTimeout).AppendLine();
            sb.AppendFormat("      NumMissedTableIAmAliveLimit: {0}", NumMissedTableIAmAliveLimit).AppendLine();
            sb.AppendFormat("      MaxJoinAttemptTime: {0}", MaxJoinAttemptTime).AppendLine();
            sb.AppendFormat("      ExpectedClusterSize: {0}", ExpectedClusterSize).AppendLine();
            sb.AppendFormat("   Azure:").AppendLine();
            sb.AppendFormat("      DeploymentId: {0}", DeploymentId).AppendLine();
            string dataConnectionInfo = ConfigUtilities.PrintDataConnectionInfo(DataConnectionString); // Don't print Azure account keys in log files
            sb.AppendFormat("      DataConnectionString: {0}", dataConnectionInfo).AppendLine();
            sb.Append(Application.ToString());
            sb.AppendFormat("   Grain directory cache:").AppendLine();
            sb.AppendFormat("      Maximum size: {0} grains", CacheSize).AppendLine();
            sb.AppendFormat("      Initial TTL: {0}", InitialCacheTTL).AppendLine();
            sb.AppendFormat("      Maximum TTL: {0}", MaximumCacheTTL).AppendLine();
            sb.AppendFormat("      TTL extension factor: {0:F2}", CacheTTLExtensionFactor).AppendLine();
            sb.AppendFormat("      Directory Caching Strategy: {0}", DirectoryCachingStrategy).AppendLine();
            sb.AppendFormat("   Grain directory:").AppendLine();
            sb.AppendFormat("      Lazy deregistration delay: {0}", DirectoryLazyDeregistrationDelay).AppendLine();
            //sb.AppendFormat("   Deadlock Detection:").AppendLine();
            //sb.AppendFormat("       PerformDeadlockDetection: {0}", PerformDeadlockDetection).AppendLine();
            sb.AppendFormat("   Reminder Service:").AppendLine();
            sb.AppendFormat("       ReminderServiceType: {0}", ReminderServiceType).AppendLine();
            sb.AppendFormat("   Providers:").AppendLine();
            sb.Append(PrintProviderConfigurations(ProviderConfigurations));

            return sb.ToString();
        }

        internal override void Load(XmlElement root)
        {
            Logger logger = Logger.GetLogger("OrleansConfiguration", Logger.LoggerType.Runtime);
            SeedNodes = new List<IPEndPoint>();

            XmlElement child;
            foreach (XmlNode c in root.ChildNodes)
            {
                child = c as XmlElement;
                if (child != null && child.LocalName == "Networking")
                {
                    Subnet = child.HasAttribute("Subnet")
                        ? ConfigUtilities.ParseSubnet(child.GetAttribute("Subnet"), "Invalid Subnet")
                        : null;
                }
            }
            foreach (XmlNode c in root.ChildNodes)
            {
                child = c as XmlElement;
                if (child == null) continue; // Skip comment lines

                switch (child.LocalName)
                {
                    case "Liveness":
                        // <Liveness LivenessType ="File" LivenessEnabled="true" ProbeTimeout = "10" TableRefreshTimeout ="60" DeathVoteExpirationTimeout ="120" 
                        //      NumMissedProbesLimit = "3" NumProbedSilos="3" NumVotesForDeathDeclaration="2" UseLivenessGossip="false" LivenessFileDirectory="."/>    
                        if (child.HasAttribute("LivenessType"))
                        {
                            LivenessType = (LivenessProviderType)Enum.Parse(typeof(LivenessProviderType), child.GetAttribute("LivenessType"));
                        }
                        if (child.HasAttribute("LivenessEnabled"))
                        {
                            LivenessEnabled = ConfigUtilities.ParseBool(child.GetAttribute("LivenessEnabled"),
                                "Invalid boolean value for the LivenessEnabled attribute on the Liveness element");
                        }
                        if (child.HasAttribute("ProbeTimeout"))
                        {
                            ProbeTimeout = ConfigUtilities.ParseTimeSpan(child.GetAttribute("ProbeTimeout"),
                                "Invalid integer value for the ProbeTimeout attribute on the Liveness element");
                        }
                        if (child.HasAttribute("TableRefreshTimeout"))
                        {
                            TableRefreshTimeout = ConfigUtilities.ParseTimeSpan(child.GetAttribute("TableRefreshTimeout"),
                                "Invalid integer value for the TableRefreshTimeout attribute on the Liveness element");
                        }
                        if (child.HasAttribute("DeathVoteExpirationTimeout"))
                        {
                            DeathVoteExpirationTimeout = ConfigUtilities.ParseTimeSpan(child.GetAttribute("DeathVoteExpirationTimeout"),
                                "Invalid integer value for the DeathVoteExpirationTimeout attribute on the Liveness element");
                        }
                        if (child.HasAttribute("NumMissedProbesLimit"))
                        {
                            NumMissedProbesLimit = ConfigUtilities.ParseInt(child.GetAttribute("NumMissedProbesLimit"),
                                "Invalid integer value for the NumMissedIAmAlive attribute on the Liveness element");
                        }
                        if (child.HasAttribute("NumProbedSilos"))
                        {
                            NumProbedSilos = ConfigUtilities.ParseInt(child.GetAttribute("NumProbedSilos"),
                                "Invalid integer value for the NumProbedSilos attribute on the Liveness element");
                        }
                        if (child.HasAttribute("NumVotesForDeathDeclaration"))
                        {
                            NumVotesForDeathDeclaration = ConfigUtilities.ParseInt(child.GetAttribute("NumVotesForDeathDeclaration"),
                                "Invalid integer value for the NumVotesForDeathDeclaration attribute on the Liveness element");
                        }
                        if (child.HasAttribute("UseLivenessGossip"))
                        {
                            UseLivenessGossip = ConfigUtilities.ParseBool(child.GetAttribute("UseLivenessGossip"),
                                "Invalid boolean value for the UseLivenessGossip attribute on the Liveness element");
                        }
                        if (child.HasAttribute("IAmAliveTablePublishTimeout"))
                        {
                            IAmAliveTablePublishTimeout = ConfigUtilities.ParseTimeSpan(child.GetAttribute("IAmAliveTablePublishTimeout"),
                                "Invalid integer value for the IAmAliveTablePublishTimeout attribute on the Liveness element");
                        }
                        if (child.HasAttribute("NumMissedTableIAmAliveLimit"))
                        {
                            NumMissedTableIAmAliveLimit = ConfigUtilities.ParseInt(child.GetAttribute("NumMissedTableIAmAliveLimit"),
                                "Invalid integer value for the NumMissedTableIAmAliveLimit attribute on the Liveness element");
                        }
                        if (child.HasAttribute("MaxJoinAttemptTime"))
                        {
                            MaxJoinAttemptTime = ConfigUtilities.ParseTimeSpan(child.GetAttribute("MaxJoinAttemptTime"),
                                "Invalid integer value for the MaxJoinAttemptTime attribute on the Liveness element");
                        }
                        if (child.HasAttribute("ExpectedClusterSize"))
                        {
                            int expectedClusterSize = ConfigUtilities.ParseInt(child.GetAttribute("ExpectedClusterSize"),
                                "Invalid integer value for the ExpectedClusterSize attribute on the Liveness element");
                            ExpectedClusterSize_CV = new ConfigValue<int>(expectedClusterSize, false);
                        }
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
                        if (child.HasAttribute("MaxStorageBusyRetries"))
                        {
                            int maxBusyRetries = ConfigUtilities.ParseInt(child.GetAttribute("MaxStorageBusyRetries"),
                                "Invalid integer value for the MaxStorageBusyRetries attribute on the Azure element");
                            AzureTableDefaultPolicies.MaxBusyRetries = maxBusyRetries;
                        }
                        break;

                    case "SeedNode":
                        SeedNodes.Add(ConfigUtilities.ParseIPEndPoint(child, Subnet));
                        break;

                    //case "GatewayNode":
                    //    addr = OrleansConfiguration.ResolveIPAddress(child.GetAttribute("Address"));
                    //    port = Int32.Parse(child.GetAttribute("Port"));
                    //    GatewayNodes.Add(new IPEndPoint(addr, port));
                    //    break;
                    case "Messaging":
                        base.Load(child);
                        break;

                    case "Application":
                        Application.Load(child, logger);
                        break;

                    case "Caching":
                        if (child.HasAttribute("CacheSize"))
                            CacheSize = ConfigUtilities.ParseInt(child.GetAttribute("CacheSize"),
                                "Invalid integer value for Caching.CacheSize");

                        if (child.HasAttribute("InitialTTL"))
                            InitialCacheTTL = ConfigUtilities.ParseTimeSpan(child.GetAttribute("InitialTTL"),
                                "Invalid integer value for Caching.InitialTTL");

                        if (child.HasAttribute("MaximumTTL"))
                            MaximumCacheTTL = ConfigUtilities.ParseTimeSpan(child.GetAttribute("MaximumTTL"),
                                "Invalid integer value for Caching.MaximumTTL");

                        if (child.HasAttribute("TTLExtensionFactor"))
                            CacheTTLExtensionFactor = ConfigUtilities.ParseDouble(child.GetAttribute("TTLExtensionFactor"),
                                "Invalid double value for Caching.TTLExtensionFactor");
                        if (CacheTTLExtensionFactor <= 1.0)
                        {
                            throw new FormatException("Caching.TTLExtensionFactor must be greater than 1.0");
                        }

                        if (child.HasAttribute("DirectoryCachingStrategy"))
                            DirectoryCachingStrategy = ConfigUtilities.ParseEnum<DirectoryCachingStrategyType>(child.GetAttribute("DirectoryCachingStrategy"),
                                "Invalid value for Caching.Strategy");

                        break;
                    case "Directory":
                        if (child.HasAttribute("DirectoryLazyDeregistrationDelay"))
                        {
                            DirectoryLazyDeregistrationDelay = ConfigUtilities.ParseTimeSpan(child.GetAttribute("DirectoryLazyDeregistrationDelay"),
                                "Invalid time span value for Directory.DirectoryLazyDeregistrationDelay");
                        }
                        break;
                    //case "DeadlockDetection":
                    //    if (child.HasAttribute("Enabled"))
                    //    {
                    //        PerformDeadlockDetection = ConfigUtilities.ParseBool(child.GetAttribute("Enabled"),
                    //                                                 "Invalid boolean value for the Enabled attribute on the DeadlockDetection element");
                    //    }
                    //    break;
                    case "ReminderService":
                        if (child.HasAttribute("ReminderServiceType"))
                        {
                            ReminderServiceType = (ReminderServiceProviderType)Enum.Parse(typeof(ReminderServiceProviderType), child.GetAttribute("ReminderServiceType"));
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

            // If user did not explicitely specify ReminderServiceProviderType, use the default one.
            // Otherwise, use what ever user specified.
            if (ReminderServiceType == ReminderServiceProviderType.NotSpecified)
            {
                if (RunsInAzure)
                {
                    ReminderServiceType = ReminderServiceProviderType.AzureTable;
                }
                else
                {
                    ReminderServiceType = ReminderServiceProviderType.ReminderTableGrain;
                }
            }
            // Default: use seed nodes as gateway nodes if no gateway nodes are specified
            //if (GatewayNodes.Count == 0)
            //{
            //    GatewayNodes.AddRange(SeedNodes);
            //}
        }

        internal static void AdjustConfiguration(Dictionary<string, ProviderCategoryConfiguration> providerConfigurations, string deploymentId)
        {
            if (!String.IsNullOrEmpty(deploymentId))
            {
                foreach (ProviderCategoryConfiguration providerConfig in providerConfigurations.Where(kv => kv.Key.Equals("Stream")).Select(kv => kv.Value))
                {
                    providerConfig.AddToConfiguration("DeploymentId", deploymentId);
                }
            }
        }

        internal static string PrintProviderConfigurations(Dictionary<string, ProviderCategoryConfiguration> providerConfigurations)
        {
            StringBuilder sb = new StringBuilder();
            if (providerConfigurations.Keys.Count > 0)
            {
                foreach (string provType in providerConfigurations.Keys)
                {
                    ProviderCategoryConfiguration provTypeConfigs = providerConfigurations[provType];
                    sb.AppendFormat("       {0}Providers:\n{1}", provType, provTypeConfigs.ToString())
                      .AppendLine();
                }
            }
            else
            {
                sb.AppendLine("       No providers configured.");
            }
            return sb.ToString();
        }
    }
}
