using Orleans.LogViews;
using Orleans.MultiCluster;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime.LogViews
{

    /// <summary>
    /// Helper class for tracking notifications that a log view grain sends to other clusters after updating the log.
    /// </summary>
    internal class NotificationTracker
    {
        private IProtocolServices services;
        private Dictionary<string, NotificationWorker> sendWorkers;
        private int maxNotificationBatchSize;

        public NotificationTracker(IProtocolServices services, MultiClusterConfiguration configuration, int maxNotificationBatchSize)
        {
            this.services = services;
            sendWorkers = new Dictionary<string, NotificationWorker>();
            this.maxNotificationBatchSize = maxNotificationBatchSize;

            foreach (var x in configuration.Clusters)
                if (x != services.MyClusterId)
                {
                    services.Verbose("Now sending notifications to {0}", x);
                    sendWorkers.Add(x, new NotificationWorker(services, x, maxNotificationBatchSize));
                }
        }

        public void BroadcastNotification(INotificationMessage msg, string exclude = null)
        {
            foreach (var kvp in sendWorkers)
                if (kvp.Key != exclude)
                {
                    var w = kvp.Value;
                    w.Enqueue(msg);
                }
        }

        /// <summary>
        /// last observed exception, or null if last notification attempts were successful for all clusters
        /// </summary>
        public Exception LastException
        {
            get
            {
                return sendWorkers.Values.OrderBy(ns => ns.LastFailure).Select(ns => ns.LastException).LastOrDefault();
            }
        }

        /// <summary>
        /// Update the multicluster configuration (change who to send notifications to)
        /// </summary>
        public void ProcessConfigurationChange(MultiClusterConfiguration oldConfig, MultiClusterConfiguration newConfig)
        {
            var removed = sendWorkers.Keys.Except(newConfig.Clusters);
            foreach (var x in removed)
            {
                services.Verbose("No longer sending notifications to {0}", x);
                sendWorkers[x].Done = true;
                sendWorkers.Remove(x);
            }

            var added = oldConfig == null ? newConfig.Clusters : newConfig.Clusters.Except(oldConfig.Clusters);
            foreach (var x in added)
                if (x != services.MyClusterId)
                {
                    services.Verbose("Now sending notifications to {0}", x);
                    sendWorkers.Add(x, new NotificationWorker(services, x, maxNotificationBatchSize));
                }
        }


        public enum NotificationQueueState : byte
        {
            Empty,
            Single,
            Batch,
            VersionOnly
        }

        /// <summary>
        /// Asynchronous batch worker that sends notfications to a particular cluster.
        /// </summary>
        public class NotificationWorker : BatchWorker
        {
            private IProtocolServices services;
            private string clusterId;
            private int maxNotificationBatchSize;

            /// <summary>
            /// Queue messages
            /// </summary>
            public INotificationMessage QueuedMessage = null;
            /// <summary>
            /// Queue state
            /// </summary>
            public NotificationQueueState QueueState = NotificationQueueState.Empty;
            /// <summary>
            /// Last exception
            /// </summary>
            public Exception LastException;
            /// <summary>
            /// Time of last failure
            /// </summary>
            public DateTime LastFailure;
            /// <summary>
            /// Number of consecutive failures
            /// </summary>
            public int NumConsecutiveFailures;
            /// <summary>
            /// Is current task done or not
            /// </summary>
            public bool Done;

            /// <summary>
            /// Constructor
            /// </summary>
            /// <param name="services"></param>
            /// <param name="clusterId"></param>
            /// <param name="maxNotificationBatchSize"></param>
            public NotificationWorker(IProtocolServices services, string clusterId, int maxNotificationBatchSize)
            {
                this.services = services;
                this.clusterId = clusterId;
                this.maxNotificationBatchSize = maxNotificationBatchSize;
            }

            /// <summary>
            /// Enqueue method
            /// </summary>
            /// <param name="msg">The message to enqueue</param>
            public void Enqueue(INotificationMessage msg)
            {
                switch (QueueState)
                {
                    case (NotificationQueueState.Empty):
                        {
                            QueuedMessage = msg;
                            QueueState = NotificationQueueState.Single;
                            break;
                        }
                    case (NotificationQueueState.Single):
                        {
                            var m = new List<INotificationMessage>();
                            m.Add(QueuedMessage);
                            m.Add(msg);
                            QueuedMessage = new BatchedNotificationMessage() { Notifications = m };
                            QueueState = NotificationQueueState.Batch;
                            break;
                        }
                    case (NotificationQueueState.Batch):
                        {
                            var batchmsg = (BatchedNotificationMessage)QueuedMessage;
                            if (batchmsg.Notifications.Count < maxNotificationBatchSize)
                            {
                                batchmsg.Notifications.Add(msg);
                                break;
                            }
                            else
                            {
                                // keep only a version notification
                                QueuedMessage = new VersionNotificationMessage() { Version = msg.Version };
                                QueueState = NotificationQueueState.VersionOnly;
                                break;
                            }
                        }
                    case (NotificationQueueState.VersionOnly):
                        {
                            ((VersionNotificationMessage)QueuedMessage).Version = msg.Version;
                            QueueState = NotificationQueueState.VersionOnly;
                            break;
                        }
                }
                Notify();
            }

            protected override async Task Work()
            {
                if (Done) return; // has been terminated - now garbage.

                // take all of current queue
                var msg = QueuedMessage;
                var state = QueueState;

                if (state == NotificationQueueState.Empty)
                    return;

                // queue is now empty (and may grow while this worker is doing awaits)
                QueuedMessage = null;
                QueueState = NotificationQueueState.Empty;

                // try to send it
                try
                {
                    await services.SendMessage(msg, clusterId);
                    services.Verbose("Sent notification to cluster {0}: {1}", clusterId, msg);
                    LastException = null;
                    NumConsecutiveFailures = 0;
                }
                catch (Exception e)
                {
                    services.Info("Could not send notification to cluster {0}: {1}", clusterId, e);

                    // next time, send only version (this is an optimization that 
                    // avoids the queueing and sending of lots of data when there are errors observed)
                    QueuedMessage = new VersionNotificationMessage() { Version = msg.Version };
                    QueueState = NotificationQueueState.VersionOnly;
                    Notify();

                    LastException = e;
                    LastFailure = DateTime.UtcNow;
                    NumConsecutiveFailures++;
                }

                // throttle retries, based on number of consecutive failures
                if (NumConsecutiveFailures > 0)
                {
                    if (NumConsecutiveFailures < 3) await Task.Delay(TimeSpan.FromMilliseconds(1));
                    else if (NumConsecutiveFailures < 1000) await Task.Delay(TimeSpan.FromSeconds(30));
                    else await Task.Delay(TimeSpan.FromMinutes(1));
                }
            }
        }
    }
}
