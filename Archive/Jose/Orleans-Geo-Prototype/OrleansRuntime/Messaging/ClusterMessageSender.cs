using System;
using Orleans.Messaging;

namespace Orleans.Runtime.Messaging
{
    internal class ClusterMessageSender : SiloMessageSender
    {
        private readonly ClusterConfiguration clusterConfiguration;

        internal ClusterMessageSender(string nameSuffix, MessageCenter mc, ClusterConfiguration clusterConf)
            : base(nameSuffix, mc)
        {
            clusterConfiguration = clusterConf;
        }

        protected override SocketDirection GetSocketDirection() { return SocketDirection.SiloToCluster;  }

        protected SiloAddress GetRemoteGateway(int clusterId)
        {
            return clusterConfiguration.GetGateway(clusterId);
        }

        protected override bool PrepareMessageForSend(Message wmsg)
        {
            if (!base.PrepareMessageForSend(wmsg))
            {
                return false;
            }

            // Must specify a valid sender.
            if (wmsg.SendingSilo.ClusterId == -1)
            {
                base.FailMessage(wmsg, "Trying to send intercluster message without specifying sender!");
                return false;
            }
            // Must have a target silo, which specifies the cluster we want to send the message to.
            if (wmsg.TargetSilo == null) 
            {
                base.FailMessage(wmsg, "ClusterMessageSender needs a dummy TargetSilo");
                return false;
            }
            // Make sure we're not sending to ourselves. XXX should probably flag this somehow
            if (wmsg.TargetSilo.ClusterId == -1 || wmsg.TargetSilo.ClusterId == mc.MyAddress.ClusterId)
            {
                base.FailMessage(wmsg, "Trying to send an intercluster message to the same cluster!");
                return false;
            }

            // Find an appropriate gateway
            if (wmsg.Direction == Message.Directions.Request || wmsg.Direction == Message.Directions.OneWay)
            {
                int clusterId = wmsg.TargetSilo.ClusterId;
                SiloAddress gw = GetRemoteGateway(wmsg.TargetSilo.ClusterId);
                wmsg.TargetSilo = SiloAddress.New(gw.Endpoint, gw.Generation, clusterId);
                wmsg.SetHeader(Message.Header.ReroutingRequested, true);
            }
            return true;
        }

        protected override void FailMessage(Message msg, string reason)
        {
            msg.ReleaseBodyAndHeaderBuffers();
            // MessagingStatisticsGroup.OnFailedSentMessage(msg);

            if (msg.Direction == Message.Directions.Request)
            {
                if (log.IsVerbose) log.Verbose(ErrorCode.MessagingSendingRejection, "Silo {0} is rejecting message: {1}. Reason = {2}", mc.MyAddress, msg, reason);
                // Done retrying, send back an error instead
                mc.SendRejection(msg, Message.RejectionTypes.FutureTransient, String.Format("Silo {0} is rejecting message: {1}. Reason = {2}", mc.MyAddress, msg, reason));

                // Invalidate this cluster's directory cache if this is an application message.
                if (msg.Category == Message.Categories.Application)
                {
                    mc.LocalGrainDirectory.FlushCachedPartitionEntry(msg.TargetAddress);
                }
            }
            else
            {
                log.Info(ErrorCode.Messaging_OutgoingMS_DroppingMessage, "Silo {0} is dropping message: {1}. Reason = {2}", mc.MyAddress, msg, reason);
                // MessagingStatisticsGroup.OnDroppedSentMessage(msg);
            }
        }
    }
}
