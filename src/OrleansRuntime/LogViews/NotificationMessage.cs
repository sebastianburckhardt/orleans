using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans.MultiCluster;

namespace Orleans.Runtime.LogViews
{
    /// <summary>
    /// Base class for notification messages that are sent by log view adaptors to other clusters, after updating the log
    /// </summary>
    [Serializable]
    public abstract class NotificationMessage : IProtocolMessage
    {
        /// The version number
        public int Version { get; set; }

        // a log view provider can subclass this to add more information
        // for example, the log entries that were appended, or the view
    }

    /// <summary>
    /// Simple notification message containing only the version
    /// </summary>
    [Serializable]
    public class VersionNotificationMessage : NotificationMessage
    {
    }

}
