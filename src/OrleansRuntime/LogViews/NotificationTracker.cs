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
        private Dictionary<string, NotificationWorker> sendworkers;
        private int maxnotificationbatchsize;

        public NotificationTracker(IProtocolServices services, MultiClusterConfiguration configuration, int maxnotificationbatchsize)
        {
            this.services = services;
            sendworkers = new Dictionary<string, NotificationWorker>();
            this.maxnotificationbatchsize = maxnotificationbatchsize;

            foreach (var x in configuration.Clusters)
                if (x != services.MyClusterId)
                {
                    services.Verbose("Now sending notifications to {0}", x);
                    sendworkers.Add(x, new NotificationWorker(services, x, maxnotificationbatchsize));
                }
        }

        public void BroadcastNotification(INotificationMessage msg, string exclude = null)
        {
            foreach (var kvp in sendworkers)
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
                return sendworkers.Values.OrderBy(ns => ns.LastFailure).Select(ns => ns.LastException).LastOrDefault();
            }
        }

        /// <summary>
        /// Update the multicluster configuration (change who to send notifications to)
        /// </summary>
        public void ProcessConfigurationChange(MultiClusterConfiguration oldconf, MultiClusterConfiguration newconf)
        {
            var removed = sendworkers.Keys.Except(newconf.Clusters);
            foreach (var x in removed)
            {
                services.Verbose("No longer sending notifications to {0}", x);
                sendworkers[x].Done = true;
                sendworkers.Remove(x);
            }

            var added = oldconf == null ? newconf.Clusters : newconf.Clusters.Except(oldconf.Clusters);
            foreach (var x in added)
                if (x != services.MyClusterId)
                {
                    services.Verbose("Now sending notifications to {0}", x);
                    sendworkers.Add(x, new NotificationWorker(services, x, maxnotificationbatchsize));
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

            public NotificationWorker(IProtocolServices services, string clusterId, int maxnotificationbatchsize)
            {
                this.services = services;
                this.clusterId = clusterId;
                this.maxnotificationbatchsize = maxnotificationbatchsize;
            }

            public INotificationMessage QueuedMessage = null;
            public NotificationQueueState queuestate = NotificationQueueState.Empty;
            public Exception LastException;
            public DateTime LastFailure;
            public int NumConsecutiveFailures;
            public bool Done;
            private int maxnotificationbatchsize;

            public void Enqueue(INotificationMessage msg)
            {
                switch (queuestate)
                {
                    case (NotificationQueueState.Empty):
                        {
                            QueuedMessage = msg;
                            queuestate = NotificationQueueState.Single;
                            break;
                        }
                    case (NotificationQueueState.Single):
                        {
                            var m = new List<INotificationMessage>();
                            m.Add(QueuedMessage);
                            m.Add(msg);
                            QueuedMessage = new BatchedNotificationMessage() { Notifications = m };
                            queuestate = NotificationQueueState.Batch;
                            break;
                        }
                    case (NotificationQueueState.Batch):
                        {
                            var batchmsg = (BatchedNotificationMessage)QueuedMessage;
                            if (batchmsg.Notifications.Count < maxnotificationbatchsize)
                            {
                                batchmsg.Notifications.Add(msg);
                                break;
                            }
                            else
                            {
                                // keep only a version notification
                                QueuedMessage = new VersionNotificationMessage() { Version = msg.Version };
                                queuestate = NotificationQueueState.VersionOnly;
                                break;
                            }
                        }
                    case (NotificationQueueState.VersionOnly):
                        {
                            ((VersionNotificationMessage)QueuedMessage).Version = msg.Version;
                            queuestate = NotificationQueueState.VersionOnly;
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
                var state = queuestate;

                if (state == NotificationQueueState.Empty)
                    return;

                // queue is now empty (and may grow while this worker is doing awaits)
                QueuedMessage = null;
                queuestate = NotificationQueueState.Empty;

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
                    queuestate = NotificationQueueState.VersionOnly;
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
