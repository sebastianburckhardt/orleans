using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics.CodeAnalysis;

namespace Orleans
{
    /// <summary>
    /// System status values and current register
    /// </summary>
    internal sealed class SystemStatus : IEquatable<SystemStatus>
    {
        // Current system status
        public static SystemStatus Current {
            get
            {
                // System should always have some status, even if it is Status==Unknown
                if (currentStatus == null) currentStatus = SystemStatus.Unknown;
                
                return currentStatus;
            }
            set
            {
                // System should always have some status, even if it is Status==Unknown
                if (value == null) value = SystemStatus.Unknown;

                currentStatus = value;

                if (!value.Equals(SystemStatus.Creating)) // don't print Creating because the logger has not been initialzed properly yet.
                {
                    logger.Info(ErrorCode.Runtime_Error_100294, "SystemStatus={0}", value);
                }
            }
        }

        private enum InternalSystemStatus
        {
            Unknown = 0,
            Error,
            Deploying,
            Deployed,
            Creating,
            Created,
            Starting,
            Running,
            Stopping,
            ShuttingDown,
            Terminated,
            Pausing,
            Paused
        }

        private static SystemStatus currentStatus;

        private static readonly Logger logger = Logger.GetLogger("SystemStatus", Logger.LoggerType.Runtime);

        /// <summary>Status = Unknown</summary>
        [SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes")]
        public static readonly SystemStatus Unknown = new SystemStatus(InternalSystemStatus.Unknown);

        /// <summary>Status = Error</summary>
        [SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes")]
        public static readonly SystemStatus Error = new SystemStatus(InternalSystemStatus.Error);
        
        /// <summary>Status = Deploying</summary>
        [SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes")]
        public static readonly SystemStatus Deploying = new SystemStatus(InternalSystemStatus.Deploying);
        
        /// <summary>Status = Deployed</summary>
        [SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes")]
        public static readonly SystemStatus Deployed = new SystemStatus(InternalSystemStatus.Deployed);

        /// <summary>Status = Creating</summary>
        [SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes")]
        public static readonly SystemStatus Creating = new SystemStatus(InternalSystemStatus.Creating);

        /// <summary>Status = Created</summary>
        [SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes")]
        public static readonly SystemStatus Created = new SystemStatus(InternalSystemStatus.Created);

        /// <summary>Status = Starting</summary>
        [SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes")]
        public static readonly SystemStatus Starting = new SystemStatus(InternalSystemStatus.Starting);
        
        /// <summary>Status = Running</summary>
        [SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes")]
        public static readonly SystemStatus Running = new SystemStatus(InternalSystemStatus.Running);
        
        /// <summary>Status = Stopping</summary>
        [SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes")]
        public static readonly SystemStatus Stopping = new SystemStatus(InternalSystemStatus.Stopping);
        
        /// <summary>Status = Shuttingdown</summary>
        [SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes")]
        public static readonly SystemStatus ShuttingDown = new SystemStatus(InternalSystemStatus.ShuttingDown);

        /// <summary>Status = Terminated</summary>
        [SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes")]
        public static readonly SystemStatus Terminated = new SystemStatus(InternalSystemStatus.Terminated);

        /// <summary>Status = Pausing</summary>
        [SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes")]
        public static readonly SystemStatus Pausing = new SystemStatus(InternalSystemStatus.Pausing);
        
        /// <summary>Status = Paused</summary>
        [SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes")]
        public static readonly SystemStatus Paused = new SystemStatus(InternalSystemStatus.Paused);

        private InternalSystemStatus value;
        private SystemStatus(InternalSystemStatus name) { this.value = name; }

        /// <see cref="Object.ToString"/>
        public override string ToString() { return this.value.ToString(); }
        /// <see cref="Object.GetHashCode"/>
        public override int GetHashCode() { return this.value.GetHashCode(); }
        /// <see cref="Object.Equals(Object)"/>
        public override bool Equals(object obj) { SystemStatus ss = obj as SystemStatus; return ss == null ? false : this.Equals(ss); }
        /// <see cref="IEquatable{T}.Equals"/>
        public bool Equals(SystemStatus other) { return (other == null) ? false : this.value.Equals(other.value); }
    }
}
