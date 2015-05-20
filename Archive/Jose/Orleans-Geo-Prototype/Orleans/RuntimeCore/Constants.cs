using System.Collections.Generic;
using System.Diagnostics;
using System;

namespace Orleans
{
    internal class Constants
    {
        // This needs to be first, as GrainId static initializers reference it. Otherwise, GrainId actually see a uninitialized (ie Zero) value for that "constant"!
        public static readonly TimeSpan INFINITE_TIMESPAN = TimeSpan.FromMilliseconds(-1);

        // We assume that clock skew between silos and between clients and silos is always less than 1 second
        public static readonly TimeSpan MAXIMUM_CLOCK_SKEW = TimeSpan.FromSeconds(1);

        public static readonly string DEFAULT_STORAGE_PROVIDER_NAME = "Default";
        public static readonly string MEMORY_STORAGE_PROVIDER_NAME = "MemoryStore";
        
        public static readonly GrainId DirectoryServiceId = GrainId.GetSystemTargetGrainId(10);
        public static readonly GrainId DirectoryCacheValidatorId = GrainId.GetSystemTargetGrainId(11);
        public static readonly GrainId SiloControlId = GrainId.GetSystemTargetGrainId(12);
        public static readonly GrainId ClientObserverRegistrarId = GrainId.GetSystemTargetGrainId(13);
        public static readonly GrainId CatalogId = GrainId.GetSystemTargetGrainId(14);
        public static readonly GrainId MembershipOracleId = GrainId.GetSystemTargetGrainId(15);
        public static readonly GrainId ReminderServiceId = GrainId.GetSystemTargetGrainId(16);
        public static readonly GrainId TypeManagerId = GrainId.GetSystemTargetGrainId(17);
        public static readonly GrainId TestSystemTargetId = GrainId.GetSystemTargetGrainId(18);
        public static readonly GrainId ProviderManagerSystemTargetId = GrainId.GetSystemTargetGrainId(19);
        public static readonly GrainId PullingAgentSystemTargetId = GrainId.GetSystemTargetGrainId(20);
        public static readonly GrainId GraphPartitionSystemTargetId = GrainId.GetSystemTargetGrainId(21);

        public const int SystemDomainTypeCode = 63;
        public static readonly GrainId SystemMembershipTableId = GrainId.GetGrainId(new Guid("01145FEC-C21E-11E0-9105-D0FB4724019B"));
        public static readonly GrainId SystemReminderTableId = GrainId.GetGrainId(new Guid("012EB12F-330A-423E-B271-659FFAA52B9C"));
        
        /// <summary>
        /// The default timeout before a request is assumed to have failed.
        /// </summary>
        public static readonly TimeSpan DEFAULT_RESPONSE_TIMEOUT = Debugger.IsAttached ? TimeSpan.FromMinutes(30) : TimeSpan.FromSeconds(30);
        //public static readonly int DEFAULT_INIT_TIMEOUT = Debugger.IsAttached ? 500*1000 : 5000;

        /// <summary>
        /// Minimum period for registering a reminder ... we want to enforce a lower bound
        /// </summary>
        public static readonly TimeSpan MIN_REMINDER_PERIOD = TimeSpan.FromMinutes(1); // increase this period, reminders are supposed to be less frequent ... we use 2 seconds just to reduce the running time of the unit tests
        /// <summary>
        /// Refresh local reminder list to reflect the global reminder table every 'REFRESH_REMINDER_LIST' period
        /// </summary>
        public static readonly TimeSpan REFRESH_REMINDER_LIST = TimeSpan.FromMinutes(5);

        public const int LARGE_OBJECT_HEAP_THRESHOLD = 85000;

        public const bool DEFAULT_PROPAGATE_E2E_ACTIVITY_ID = false;

        public const int DEFAULT_LOGGER_BULK_MESSAGE_LIMIT = 5;

        public static readonly bool USE_BLOCKING_COLLECTION = true;

        public static readonly bool ALLOW_GRAPH_PARTITION_STRATEGY = false;

        public static readonly TimeSpan DEFAULT_COLLECTION_AGE_LIMIT = TimeSpan.FromHours(2);

        private static readonly Dictionary<GrainId, string> SystemTargetNames = new Dictionary<GrainId, string>
        {
            {DirectoryServiceId, "DirectoryService"},
            {DirectoryCacheValidatorId, "DirectoryCacheValidator"},
            {SiloControlId,"SiloControl"},
            {ClientObserverRegistrarId,"ClientObserverRegistrar"},
            {CatalogId,"Catalog"},
            {MembershipOracleId,"MembershipOracle"},
            {ReminderServiceId,"ReminderService"},
            {TypeManagerId,"TypeManagerId"},
            {TestSystemTargetId, "TestSystemTarget"},
            {ProviderManagerSystemTargetId, "ProviderManagerSystemTarget"},
            {PullingAgentSystemTargetId, "PullingAgentSystemTargetId"},
            {GraphPartitionSystemTargetId, "GraphPartitionSystemTargetId"},
        };

        public static string SystemTargetName(GrainId id)
        {
            string name;
            return SystemTargetNames.TryGetValue(id, out name) ? name : "";
        }

        private static readonly Dictionary<GrainId, string> SystemGrainNames = new Dictionary<GrainId, string>
        {
            {SystemMembershipTableId,"MembershipTableGrain"},
            {SystemReminderTableId,"ReminderTableGrain"},
        };

        public static bool TryGetSystemGrainName(GrainId id, out string name)
        {
            return SystemGrainNames.TryGetValue(id, out name);
        }

        public static bool IsSystemGrain(GrainId grain)
        {
            return SystemGrainNames.ContainsKey(grain);
        }

    }
}
 