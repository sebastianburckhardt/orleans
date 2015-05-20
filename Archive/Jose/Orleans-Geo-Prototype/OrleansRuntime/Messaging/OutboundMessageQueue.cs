using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using Orleans.Counters;

using Orleans.Messaging;

namespace Orleans.Runtime.Messaging
{
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1711:IdentifiersShouldNotHaveIncorrectSuffix")]
    internal sealed class OutboundMessageQueue : IOutboundMessageQueue
    {
        //private BlockingCollection<Message> queue;
        private readonly Lazy<SiloMessageSender>[] senders;
        private readonly ClusterMessageSender clusterSender;
        private readonly SiloMessageSender pingSender;
        private readonly SiloMessageSender systemSender;
        private readonly MessageCenter mc;
        private readonly Logger logger;
        private readonly ClusterConfiguration clusterConfig;
        private bool stopped;
        private bool clusterMessagingOn;

        public int Count
        {
            get
            {
                int n = senders.Where(sender => sender.IsValueCreated).Sum(sender => sender.Value.Count);
                n += systemSender.Count + pingSender.Count;
                return n;
            }
        }

        internal const string QUEUED_TIME_METADATA = "QueuedTime";

        internal OutboundMessageQueue(MessageCenter m, IMessagingConfiguration config, ClusterConfiguration clusterConfig)
        {
            //queue = new BlockingCollection<Message>();
            mc = m;
            pingSender = new SiloMessageSender("PingSender", mc);
            systemSender = new SiloMessageSender("SystemSender", mc);
            clusterSender = new ClusterMessageSender("ClusterSender", mc, mc.ClusterConfig);
            senders = new Lazy<SiloMessageSender>[config.SiloSenderQueues];
            for (int i = 0; i < senders.Length; i++)
            {
                int capture = i;
                senders[capture] = new Lazy<SiloMessageSender>(() =>
                {
                    var sender = new SiloMessageSender("AppMsgsSender_" + capture, mc);
                    sender.Start();
                    return sender;
                }, LazyThreadSafetyMode.ExecutionAndPublication);
            }
            logger = Logger.GetLogger("Messaging.OutboundMessageQueue");
            stopped = false;
            this.clusterConfig = clusterConfig;
            this.clusterMessagingOn = true;
        }

        public void SwitchClusterMessaging(bool val)
        {
            clusterMessagingOn = val;
        }

        public async Task<bool> SendMessage(Message msg)
        {
            if (msg != null)
            {
                if (stopped)
                {
                    logger.Info(ErrorCode.Runtime_Error_100112, "Message was queued for sending after outbound queue was stopped: {0}", msg);
                    return true;
                }

                // Don't process messages that have already timed out
                if (msg.IsExpired)
                {
                    msg.DropExpiredMessage(MessagingStatisticsGroup.Phase.Send);
                    return true;
                }

                if (!msg.ContainsMetadata(QUEUED_TIME_METADATA))
                {
                    msg.SetMetadata(QUEUED_TIME_METADATA, DateTime.UtcNow);
                }

                if ((mc.Gateway != null) && mc.Gateway.TryDeliverToProxy(msg))
                {
                    return true;
                }

                if (!msg.ContainsHeader(Message.Header.TargetSilo))
                {
                    logger.Error(ErrorCode.Runtime_Error_100113, "Message does not have a target silo: " + msg + " -- Call stack is: " + (new System.Diagnostics.StackTrace()));
                    mc.SendRejection(msg, Message.RejectionTypes.FutureTransient, "Message to be sent does not have a target silo");
                    return true;
                }

                //var header = msg.TaskHeader;
                //if (header != null && header.Active == null)
                //    logger.Error(ErrorCode.Runtime_Error_100114, "Task header invalid in " + msg);

                msg.AddTimestamp(Message.LifecycleTag.EnqueueOutgoing);

                // Shortcut messages to this silo
                if (msg.TargetSilo.Equals(mc.MyAddress))
                {
                    // First check to see if it's really destined for a proxied client, instead of a local grain
                    if (!mc.IsProxying || !mc.Gateway.TryDeliverToProxy(msg))
                    {
                        if (logger.IsVerbose3) logger.Verbose3("Message has been looped back to this silo: {0}", msg);
                        MessagingStatisticsGroup.LocalMessagesSent.Increment();
                        mc.InboundQueue.PostMessage(msg);
                    }
                }
                else
                {
                    //queue.Add(msg);
                    if (stopped)
                    {
                        logger.Info(ErrorCode.Runtime_Error_100115, "Message was queued for sending after outbound queue was stopped: {0}", msg);
                        return true;
                    }

                    // Inter-cluster messages can be either System or Application. Grain directory protocol messages are
                    // of type System. An activation in this cluster can also call an activation in a remote cluster, these
                    // correspond to Application messages.
                    if (!msg.TargetSilo.IsSameCluster(mc.MyAddress))
                    {
                        if (clusterMessagingOn)
                        {
                            await Task.Delay(clusterConfig.DelayDictionary[mc.MyAddress.ClusterId][msg.TargetSilo.ClusterId]);
                            clusterSender.QueueRequest(msg);
                        }
                    }
                    else if (msg.Category == Message.Categories.Ping)
                    {
                        pingSender.QueueRequest(msg);
                    }
                    else if (msg.Category == Message.Categories.System)
                    {
                        systemSender.QueueRequest(msg);
                    }
                    else
                    {
                        int index = Math.Abs(msg.TargetSilo.GetHashCode()) % senders.Length;
                        senders[index].Value.QueueRequest(msg);
                    }
                }
            }
            return true;
        }

        public void Start()
        {
            pingSender.Start();
            systemSender.Start();
            clusterSender.Start();
            //foreach (var sender in senders)
            //{
            //    sender.Value.Start();
            //}
        }

        public void Stop()
        {
            stopped = true;
            foreach (var sender in senders)
            {
                if (sender.IsValueCreated)
                {
                    sender.Value.Stop();
                }
            }
            systemSender.Stop();
            pingSender.Stop();
        }

        #region IDisposable Members

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA1816:CallGCSuppressFinalizeCorrectly")]
        public void Dispose()
        {
            foreach (var sender in senders)
            {
                sender.Value.Stop();
                sender.Value.Dispose();
            }
            systemSender.Stop();
            pingSender.Stop();
            GC.SuppressFinalize(this);
        }

        #endregion
    }
}
