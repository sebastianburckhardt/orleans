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
    public interface INotificationMessage : IProtocolMessage
    {
        /// The version number
        int Version { get; }

        // a log view provider can subclass this to add more information
        // for example, the log entries that were appended, or the view
    }

    /// <summary>
    /// Simple notification message containing only the version
    /// </summary>
    [Serializable]
    public class VersionNotificationMessage : INotificationMessage
    {
        /// <summary>
        /// The version number
        /// </summary>
        public int Version { get; set;  }
    }


    /// <summary>
    /// notification messages can be batched
    /// </summary>
    [Serializable]
    public class BatchedNotificationMessage : INotificationMessage
    {
        /// <summary>
        /// The notification messages contained in this batch
        /// </summary>
        public List<INotificationMessage> Notifications { get; set; }

        public int Version {
            get
            {
                return Notifications.Aggregate(0, (v, m) => Math.Max(v, m.Version));
            }
        }
    }

}
