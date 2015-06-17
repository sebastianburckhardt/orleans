using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Pileus.Utils;

namespace Microsoft.WindowsAzure.Storage.Pileus.Utils
{
    public class ConstPool
    {
        #region CONFIGURATION_STRING

        /// <summary>
        /// The blob name used for storing the current configuration.
        /// </summary>
        public static readonly string CURRENT_CONFIGURATION_BLOB_NAME = "currentconfig";

        /// <summary>
        /// The table name used for storing the SLAs.
        /// </summary>
        public static readonly string SLA_CONFIGURATION_TABLE_NAME = "sla";

        /// <summary>
        /// The table name used for storing the session states.
        /// </summary>
        public static readonly string SESSION_STATE_CONFIGURATION_TABLE_NAME = "sessionstate";

        /// <summary>
        /// The container name used for storing configuration blobs.
        /// </summary>
        public static readonly string CONFIGURATION_CONTAINER_PREFIX = "configuration";

        /// <summary>
        /// The key we use to store a configuration epoch number in the metadata of configuration_blob.
        /// </summary>
        public static readonly string EPOCH_NUMBER = "EPOCH";

        public static readonly string EPOCH_MODIFIED_TIME = "EPOCH_TIME";

        #endregion

        #region CONFIGURATION_NUMBER
       
        /// <summary>
        /// The minimum probability to consider a subSLA (with a particular consistency) acceptable.
        /// </summary>
        public static readonly float MIN_ACCEPTABLE_PROB_FOR_CONS = 0.9f;

        /// <summary>
        /// The period (in milliseconds) that the cached configuration of client is considered valid. 
        /// After this period is elapsed, the client needs to refresh its cache. 
        /// </summary>
        public static readonly int CACHED_CONFIGURATION_VALIDITY_DURATION = 60000;

        /// <summary>
        /// Estimated duration (in milliseconds) of performing a <see cref="ConfigurationAction"/>.
        /// </summary>
        public static readonly int CONFIGURATION_ACTION_DURATION = 5000;

        /// <summary>
        /// Time to wait (in milliseconds) before deciding that an in-progress reconfiguration has failed to complete.
        /// </summary>
        public static readonly int CONFIGURATION_ACTION_TIMEOUT = 120000;

        /// <summary>
        /// The metadata field of configuration's blob is set to this value while reconfiguration is in progress.
        /// Hence, if clients read this value, they will execute in slow mode. 
        /// </summary>
        public static readonly string RECONFIGURATION_IN_PROGRESS = "-2";

        /// <summary>
        /// This field is added to the metadata of blobs involved in multi-primary writes.
        /// </summary>
        public static readonly string WRITE_IN_PROGRESS = "WiP";

        /// <summary>
        /// Specifies how often (in milliseconds) a ping operation should be executed to gather latency of non-replica servers.
        /// </summary>
        public static readonly int LOOKUP_PING_INTERVAL = 10000;

        /// <summary>
        /// Default sync interval (in milliseconds) of secondary replica with the primary replica
        /// </summary>
        public static readonly int DEFAULT_SYNC_INTERVAL = 500;

        /// <summary>
        /// Minimum value acceptable for setting as the sync interval (in milliseconds) between a secondary and primary replica.
        /// This value affects the configurator's behavior. 
        /// For instance, if current sync period is 250, and the adjusting multiplier is 0.5, then upon executing a <see cref="AdjustSyncPeriod.cs"/> action, the resulting sync interval will be 125.
        /// Since 125 is less than 200, this action will not be selected as a reconfiguration candidate. 
        /// </summary>
        public static readonly int MINIMUM_ALLOWED_SYNC_INTERVAL = 200;

        /// <summary>
        /// Specifies the multiplier for adjusting the sync interval.
        /// Hence, upon executing <see cref="AdjustSyncPeriod"/>, the new sync period is: oldSyncPeriod times this multiplier.
        /// </summary>
        public static readonly float ADJUSTING_SYNC_INTERVAL_MULTIPLIER = 0.5f;

        /// <summary>
        /// Interval of performing a reconfiguration in milliseconds. 
        /// </summary>
        public static readonly int CONFIGURATION_UPLOAD_INTERVAL = 100000;
       
        #endregion

    }
}
