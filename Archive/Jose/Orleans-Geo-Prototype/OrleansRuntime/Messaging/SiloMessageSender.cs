using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using Orleans.Counters;

using Orleans.Messaging;
using System.Net;

namespace Orleans.Runtime.Messaging
{
    internal class SiloMessageSender : OutgoingMessageSender
    {
        private MessageCenter mc;
        internal const string RETRY_COUNT_TAG = "RetryCount";
        private const int DEFAULT_MAX_RETRIES = 0;
        public static readonly TimeSpan CONNECTION_RETRY_DELAY = TimeSpan.FromMilliseconds(1000);

        private readonly Dictionary<SiloAddress, DateTime> lastConnectionFailure;

        internal SiloMessageSender(string nameSuffix, MessageCenter msgCtr)
            : base(nameSuffix, msgCtr.MessagingConfiguration)
        {
            mc = msgCtr;
            lastConnectionFailure = new Dictionary<SiloAddress, DateTime>();

            onFault = FaultBehavior.RestartOnFault;
        }

        //------- IMPL. -----//

        protected override SocketDirection GetSocketDirection() { return SocketDirection.SiloToSilo; }

        protected override bool PrepareMessageForSend(Message wmsg)
        {
            // Don't send messages that have already timed out
            if (wmsg.IsExpired)
            {
                wmsg.DropExpiredMessage(MessagingStatisticsGroup.Phase.Send);
                return false;
            }

            // Fill in the outbound message with our silo address, if it's not already set
            if (!wmsg.ContainsHeader(Message.Header.SendingSilo))
            {
                wmsg.SendingSilo = mc.MyAddress;
            }

            // If there's no target silo set, then we shouldn't see this message; send it back
            if (wmsg.TargetSilo == null)
            {
                FailMessage(wmsg, "No target silo provided -- internal error");
                return false;
            }

            // If we know this silo is dead, don't bother
            if ((mc.SiloDeadOracle != null) && mc.SiloDeadOracle(wmsg.TargetSilo))
            {
                FailMessage(wmsg, String.Format("Target {0} silo is known to be dead", wmsg.TargetSilo.ToLongString()));
                return false;
            }

            // If we had a bad connection to this address recently, don't even try
            DateTime failure;
            if (lastConnectionFailure.TryGetValue(wmsg.TargetSilo, out failure))
            {
                TimeSpan since = DateTime.UtcNow.Subtract(failure);
                if (since < CONNECTION_RETRY_DELAY)
                {
                    FailMessage(wmsg, String.Format("Recent ({0} ago, at {1}) connection failure trying to reach target silo {2}. Going to drop {3} msg {4} without sending. CONNECTION_RETRY_DELAY = {5}.",
                        since, Logger.PrintDate(failure), wmsg.TargetSilo.ToLongString(), wmsg.Direction, wmsg.Id, CONNECTION_RETRY_DELAY));
                    return false;
                }
            }
            wmsg.AddTimestamp(Message.LifecycleTag.SendOutgoing);
            return true;
        }

        protected override bool GetSendingSocket(Message wmsg, out Socket sock, out SiloAddress targetSilo, out string error)
        {
            sock = null;
            targetSilo = wmsg.TargetSilo;
            error = null;
            try
            {
                sock = mc.socketManager.GetSendingSocket(targetSilo.Endpoint);
                if (!sock.Connected)
                {
                    mc.socketManager.InvalidateEntry(targetSilo.Endpoint);
                    sock = mc.socketManager.GetSendingSocket(targetSilo.Endpoint);
                }
                return true;
            }
            catch (Exception ex)
            {
                error = "Exception getting a sending socket to endpoint " + targetSilo.ToString();
                log.Warn(ErrorCode.Messaging_UnableToGetSendingSocket, error, ex);
                mc.socketManager.InvalidateEntry(targetSilo.Endpoint);
                lastConnectionFailure[targetSilo] = DateTime.UtcNow;
                return false;
            }
        }

        protected override void OnGetSendingSocketFailure(Message wmsg, string error)
        {
            FailMessage(wmsg, error);
        }

        protected override void OnMessageSerializationFailure(Message wmsg, Exception exc)
        {
            // we only get here if we failed to serialize the msg (or any other catastrophic failure).
            // Request msg fails to serialize on the sending silo, so we just enqueue a rejection msg.
            // Response msg fails to serialize on the responding silo, so we try to send an error response back.
            log.Warn(ErrorCode.MessagingUnexpectedSendError, String.Format("Unexpected error sending message {0}", wmsg.ToString()), exc);
            
            wmsg.ReleaseBodyAndHeaderBuffers();
            MessagingStatisticsGroup.OnFailedSentMessage(wmsg);
            if (wmsg.Direction == Message.Directions.Request)
            {
                mc.SendRejection(wmsg, Message.RejectionTypes.Unrecoverable, exc.ToString());
            }
            else if (wmsg.Direction == Message.Directions.Response && wmsg.Result != Message.ResponseTypes.Error)
            {
                // if we failed sending an original response, turn the response body into an error and reply with it.
                // unless the response was already an error response (so we don't loop forever).
                wmsg.Result = Message.ResponseTypes.Error;
                wmsg.BodyObject = OrleansResponse.ExceptionResponse(exc);
                mc.SendMessage(wmsg);
            }
            else
            {
                MessagingStatisticsGroup.OnDroppedSentMessage(wmsg);
            }
        }

        protected override void OnSendFailure(Socket socket, SiloAddress targetSilo)
        {
            mc.socketManager.InvalidateEntry(targetSilo.Endpoint);
        }

        protected override void ProcessMessageAfterSend(Message wmsg, bool sendError, string sendErrorStr)
        {
            if (sendError)
            {
                wmsg.ReleaseHeadersOnly();
                RetryMessage(wmsg);
                return;
            }
            else
            {
                wmsg.ReleaseBodyAndHeaderBuffers();
                if (log.IsVerbose3) log.Verbose3("Sending queue delay time for: {0} is {1}", wmsg, DateTime.UtcNow.Subtract((DateTime)wmsg.GetMetadata(OutboundMessageQueue.QUEUED_TIME_METADATA)));
            }
        }

        protected override void FailMessage(Message msg, string reason)
        {
            msg.ReleaseBodyAndHeaderBuffers();
            MessagingStatisticsGroup.OnFailedSentMessage(msg);
            if (msg.Direction == Message.Directions.Request)
            {
                if (log.IsVerbose) log.Verbose(ErrorCode.MessagingSendingRejection, "Silo {0} is rejecting message: {0}. Reason = {1}", mc.MyAddress, msg, reason);
                // Done retrying, send back an error instead
                mc.SendRejection(msg, Message.RejectionTypes.FutureTransient, String.Format("Silo {0} is rejecting message: {1}. Reason = {2}", mc.MyAddress, msg, reason));
            }else
            {
                log.Info(ErrorCode.Messaging_OutgoingMS_DroppingMessage, "Silo {0} is dropping message: {0}. Reason = {1}", mc.MyAddress, msg, reason);
                MessagingStatisticsGroup.OnDroppedSentMessage(msg);
            }
        }

        private void RetryMessage(Message msg, Exception ex = null)
        {
            if (msg != null)
            {
                int maxRetries = DEFAULT_MAX_RETRIES;
                if (msg.ContainsMetadata(Message.Metadata.MaxRetries))
                {
                    maxRetries = (int)msg.GetMetadata(Message.Metadata.MaxRetries);
                }

                int retryCount = 0;
                if (msg.ContainsMetadata(RETRY_COUNT_TAG))
                {
                    retryCount = (int)msg.GetMetadata(RETRY_COUNT_TAG);
                }

                if (retryCount < maxRetries)
                {
                    msg.SetMetadata(RETRY_COUNT_TAG, retryCount + 1);
                    mc.OutboundQueue.SendMessage(msg);
                }
                else
                {
                    var reason = new StringBuilder("Retry count exceeded. ");
                    if (ex != null)
                    {
                        reason.Append("Original exception is: ").Append(ex.ToString());
                    }
                    reason.Append("Msg is: ").Append(msg);
                    FailMessage(msg, reason.ToString());
                }
            }
        }
    }
}
