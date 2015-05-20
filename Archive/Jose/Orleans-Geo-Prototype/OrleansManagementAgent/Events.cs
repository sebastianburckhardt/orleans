using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using ManagementFramework.Events;
using Orleans.RuntimeCore;

namespace Orleans.Management.Events
{
    /// <summary>
    /// Lifecycle event actions
    /// </summary>
    public enum LifecycleActions    
    {
        Start = 1,
        Pause = 2,
        Resume = 3,
        Stop = 4
    }

    /// <summary>
    /// Base class for management events for the Orleans host process
    /// Management events may be correlated into request-response pairs using the CorrelationId field
    /// </summary>
    [Serializable]
    public abstract class OrleansManagementEvent : AbstractEvent
    {
        protected const int TOO_MUCH_OUTPUT_TO_LIST = 100;
        protected const int TOO_MANY_ITEMS_TO_LIST = 10;

        /// <summary>The name of this silo.</summary>
        public string SiloName { get; set; }

        /// <summary>The runtime instance id of this silo.</summary>
        public Guid RuntimeInstanceId { get; set; }

        /// <summary>The runtime deployment group id of this silo (if applicable)</summary>
        public Guid DeploymentGroupId { get; set; }

        public override string ToString()
        {
            return string.Format(
                "{0}: SiloName={1} DeploymentGroup={2} Instance={3} Host={4} Recipient={5}",
                this.GetType().Name,
                this.SiloName,
                this.DeploymentGroupId,
                this.RuntimeInstanceId,
                this.MachineName,
                this.Recipient);
        }
    }

    /// <summary>
    /// Event to issue a lifecycle event to the Orleans host process
    /// </summary>
    public class CommandAcknowledgeEvent : OrleansManagementEvent
    {
        /// <summary>The event that was processed</summary>
        public OrleansManagementEvent ProcessedEvent { get; set; }

        /// <summary>Whether the event was processed successfully</summary>
        public bool ProcessedOk { get; set; }

        /// <summary>Error message, if <c>ProcessedOk == false</c></summary>
        public string ErrorMessage { get; set; }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder(base.ToString());
            sb.Append(" ProcessedOk=").Append(this.ProcessedOk);
            if (!this.ProcessedOk) { sb.Append(" Error='").Append(this.ErrorMessage).Append("'"); }
            sb.Append(" Request=[ ").Append(ProcessedEvent).Append(" ]");
            return sb.ToString();
        }
    }

    /// <summary>
    /// Event to issue a lifecycle event to the Orleans host process
    /// </summary>
    public class LifecycleCommandEvent : OrleansManagementEvent
    {
        /// <summary>Command type</summary>
        public LifecycleActions Command { get; set; }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder(base.ToString());
            sb.Append(" Command=").Append(Enum.GetName(typeof(LifecycleActions), this.Command));
            return sb.ToString();
        }
    }

    /// <summary>
    /// Event to dynamically set log level in the Orleans host process
    /// </summary>
    public class SetLogLevelEvent : OrleansManagementEvent
    {
        /// <summary>Log level(s)</summary>
        public Dictionary<string,int> LogLevels { get; set; }

        /// <summary>Initializes a new instance of this event class. Parameterless constructor.</summary>
        public SetLogLevelEvent()
            : base()
        {
            if (this.LogLevels == null)
            {
                this.LogLevels = new Dictionary<string, int>();
            }
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder(base.ToString());
            sb.Append(" LogLevels=[");
            foreach (string logName in LogLevels.Keys)
            {
                sb.Append(" ");
                sb.Append(logName).Append("=");
                sb.Append(Enum.GetName(typeof(Logger.Severity), LogLevels[logName]));
            }
            sb.Append(" ]");
            return sb.ToString();
        }
    }

    /// <summary>
    /// Request to explicitly ping the Orleans host process to obtain a <c>HealthEvent</c> response
    /// </summary>
    /// <seealso cref="HealthEvent"/>
    public class GetStatusRequestEvent : OrleansManagementEvent
    {
    }

    /// <summary>
    /// Health / heartbeat event send by Orleans host process
    /// </summary>
    public class HealthEvent : OrleansManagementEvent
    {
        /// <summary>The timestamp of this heartbeat from the originating server.</summary>
        public DateTime Timestamp { get; set; }

        /// <summary>Current summary status of this server</summary>
        public ManagementFramework.Common.LifecycleState Status { get; set; }

        /// <summary>Detailed status info from this server</summary>
        public string StatusDetails { get; set; }

        /// <summary>Initializes a new instance of this event class. Parameterless constructor.</summary>
        public HealthEvent()
            : base()
        {
            this.Timestamp = DateTime.Now;
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder(base.ToString());
            sb.Append(" Timestamp=").Append(this.Timestamp);
            sb.Append(" Status=");
            sb.Append(Enum.GetName(typeof(ManagementFramework.Common.LifecycleState), this.Status));
            if (this.StatusDetails != null && this.StatusDetails.Length < TOO_MUCH_OUTPUT_TO_LIST)
            {
                sb.Append("Details=[ ").Append(this.StatusDetails).Append(" ]");
            }
            return sb.ToString();
        }
    }

    /// <summary>
    /// Request to perform a log search using the specified search paramaters.
    /// Silo will return a corresponding <c>SearchLogsResponseEvent</c>
    /// </summary>
    /// <seealso cref="SearchLogsResponseEvent"/>
    public class SearchLogsRequestEvent : OrleansManagementEvent
    {
        /// <summary>Name of the log to search. Null = default log.</summary>
        public string LogName { get; set; }

        /// <summary>Search pattern regexp to scan this log for. Null = everything.</summary>
        public Regex SearchPattern { get; set; }

        /// <summary>Start of time period to scan this log for. Null = no limit.</summary>
        public DateTime SearchFrom { get; set; }

        /// <summary>End of time period to scan this log for. Null = no limit.</summary>
        public DateTime SearchTo { get; set; }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder(base.ToString());
            sb.Append(" LogName=").Append(this.LogName);
            sb.Append(" SearchPattern=").Append(this.SearchPattern);
            sb.Append(" SearchFrom=").Append(this.SearchFrom);
            sb.Append(" SearchTo=").Append(this.SearchTo);
            return sb.ToString();
        }
    }

    /// <summary>
    /// Response from a log search using the specified search paramaters.
    /// </summary>
    /// <seealso cref="SearchLogsRequestEvent"/>
    public class SearchLogsResponseEvent : SearchLogsRequestEvent
    {
        /// <summary>The timestamp of these search results from the originating server.</summary>
        public DateTime Timestamp { get; set; }

        /// <summary>List of the matching log entries</summary>
        public List<string> LogEntries { get; set; }

        /// <summary>Initializes a new instance of this event class. Parameterless constructor.</summary>
        public SearchLogsResponseEvent() : base()
        {
            this.Timestamp = DateTime.Now;
            if (this.LogEntries == null)
            {
                this.LogEntries = new List<string>();
            }
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder(base.ToString());
            sb.Append(" Timestamp=").Append(this.Timestamp);
            sb.Append(" LogEntries=[");
            if (LogEntries.Count < TOO_MANY_ITEMS_TO_LIST)
            {
                foreach (string logEntry in LogEntries)
                {
                    sb.Append(" [").Append(logEntry).Append("]");
                }
            }
            else
            {
                sb.Append("<Too many search results to list>");
            }
            sb.Append(" ]");
            return sb.ToString();
        }
    }
}