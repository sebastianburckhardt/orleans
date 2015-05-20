using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Net;
using Orleans.Counters;


namespace Orleans.Messaging
{
    internal enum SocketDirection
    {
        SiloToSilo,
        ClientToGW,
        GWToClient,
        SiloToCluster,
        ClusterToSilo,
    }

    internal abstract class OutgoingMessageSender : AsynchQueueAgent<Message>
    {
        internal OutgoingMessageSender(string nameSuffix, IMessagingConfiguration config)
            : base(nameSuffix, config)
        {
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        protected override void Process(Message wmsg)
        {
            /* Jose: Log the following line for debugging.
             * log.Info("Got a {0} message to send: {1}", wmsg.Direction, wmsg);
             */

            if (log.IsVerbose2) log.Verbose2("Got a {0} message to send: {1}", wmsg.Direction, wmsg);
            bool continueSend = PrepareMessageForSend(wmsg);
            if (!continueSend)
            {
                return;
            }

            Socket sock;
            string error;
            SiloAddress targetSilo;
            continueSend = GetSendingSocket(wmsg, out sock, out targetSilo, out error);
            if (!continueSend)
            {
                OnGetSendingSocketFailure(wmsg, error);
                return;
            }

            List<ArraySegment<byte>> data = null;
            int headerLength = 0;
            try
            {
                data = wmsg.Serialize(out headerLength);
            }
            catch (Exception exc)
            {
                OnMessageSerializationFailure(wmsg, exc);
                return;
            }

            int length = data.Sum<ArraySegment<byte>>(x => x.Count);
            int bytesSent = 0;
            bool exceptionSending = false;
            bool countMismatchSending = false;
            string sendErrorStr = null;
            try
            {
                bytesSent = sock.Send(data);
                if (bytesSent != length)
                {
                    // The complete message wasn't sent, even though no error was reported; treat this as an error
                    countMismatchSending = true;
                    sendErrorStr = String.Format("Byte count mismatch on sending to {0}: sent {1}, expected {2}", targetSilo, bytesSent, length);
                    log.Warn(ErrorCode.Messaging_CountMismatchSending, sendErrorStr);
                }
            }
            catch (Exception exc)
            {
                exceptionSending = true;
                if (!(exc is ObjectDisposedException))
                {
                    sendErrorStr = String.Format("Exception sending message to {0}. Message: {1}. {2}", targetSilo, wmsg, exc);
                    //log.Warn(ErrorCode.Messaging_ExceptionSending, "Exception sending to endpoint " + targetEndpoint, exc);
                    log.Warn(ErrorCode.Messaging_ExceptionSending, sendErrorStr, exc);
                }
                //if (exc is SocketException)
                //{
                //    sendErrorStr = String.Format("Network error sending message to gateway {0}. Message: {1}", targetEndpoint, wmsg);
                //    log.Warn(ErrorCode.ProxyClient_SocketSendError, sendErrorStr, exc);
                //}
                //else
                //{
                //    sendErrorStr = String.Format("Exception sending message to gateway {0}. Message: {1}", targetEndpoint, wmsg);
                //    log.Warn(ErrorCode.ProxyClient_SendException, sendErrorStr, exc);
                //}
            }
            MessagingStatisticsGroup.OnMessageSend(targetSilo, wmsg.Direction, bytesSent, headerLength, GetSocketDirection());
            bool sendError = exceptionSending || countMismatchSending;
            if (sendError)
            {
                OnSendFailure(sock, targetSilo);
            }
            ProcessMessageAfterSend(wmsg, sendError, sendErrorStr);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        protected override void ProcessBatch(List<Message> msgs)
        {
            if (log.IsVerbose2) log.Verbose2("Got {0} messages to send.", msgs.Count);
            for (int i = 0; i < msgs.Count; )
            {
                bool sendThisMessage = PrepareMessageForSend(msgs[i]);
                if (sendThisMessage)
                {
                    i++;
                }
                else
                {
                    msgs.RemoveAt(i); // don't advance i
                }
            }
            if (msgs.Count <= 0)
            {
                return;
            }

            Socket sock;
            string error;
            SiloAddress targetSilo;
            bool continueSend = GetSendingSocket(msgs[0], out sock, out targetSilo, out error);
            if (!continueSend)
            {
                foreach (Message wmsg in msgs)
                {
                    OnGetSendingSocketFailure(wmsg, error);
                }
                return;
            }

            List<ArraySegment<byte>> data = null;
            int headerLength = 0;
            continueSend = SerializeMessages(msgs, out data, out headerLength, this.OnMessageSerializationFailure);
            if (!continueSend)
            {
                return;
            }

            int length = data.Sum<ArraySegment<byte>>(x => x.Count);
            int bytesSent = 0;
            bool exceptionSending = false;
            bool countMismatchSending = false;
            string sendErrorStr = null;
            try
            {
                bytesSent = sock.Send(data);
                if (bytesSent != length)
                {
                    // The complete message wasn't sent, even though no error was reported; treat this as an error
                    countMismatchSending = true;
                    sendErrorStr = String.Format("Byte count mismatch on sending to {0}: sent {1}, expected {2}", targetSilo, bytesSent, length);
                    log.Warn(ErrorCode.Messaging_CountMismatchSending, sendErrorStr);
                }
            }
            catch (Exception exc)
            {
                exceptionSending = true;
                if (!(exc is ObjectDisposedException))
                {
                    sendErrorStr = String.Format("Exception sending message to {0}. {1}", targetSilo, Logger.PrintException(exc));
                    //log.Warn(ErrorCode.Messaging_ExceptionSending, "Exception sending to endpoint " + targetEndpoint, exc);
                    log.Warn(ErrorCode.Messaging_ExceptionSending, sendErrorStr, exc);
                }
            }
            MessagingStatisticsGroup.OnMessageBatchSend(targetSilo, msgs[0].Direction, bytesSent, headerLength, GetSocketDirection(), msgs.Count);
            bool sendError = exceptionSending || countMismatchSending;
            if (sendError)
            {
                OnSendFailure(sock, targetSilo);
            }
            foreach (Message wmsg in msgs)
            {
                ProcessMessageAfterSend(wmsg, sendError, sendErrorStr);
            }
        }

        //------- IMPL. -----//

        public static bool SerializeMessages(List<Message> msgs, out List<ArraySegment<byte>> data, out int headerLengthOut, Action<Message, Exception> msgSerializationFailureHandler)
        {
            int numberOfValidMessages = 0;
            List<ArraySegment<byte>> lengths = new List<ArraySegment<byte>>();
            List<ArraySegment<byte>> bodies = new List<ArraySegment<byte>>();
            data = null;
            headerLengthOut = 0;
            int totalHeadersLen = 0;

            foreach(Message message in msgs)
            {
                try
                {
                    int headerLength = 0;
                    int bodyLength = 0;
                    List<ArraySegment<byte>> body = message.SerializeForBatching(out headerLength, out bodyLength);
                    ArraySegment<byte> headerLen = new ArraySegment<byte>(BitConverter.GetBytes(headerLength));
                    ArraySegment<byte> bodyLen = new ArraySegment<byte>(BitConverter.GetBytes(bodyLength));

                    bodies.AddRange(body);
                    lengths.Add(headerLen);
                    lengths.Add(bodyLen);
                    numberOfValidMessages++;
                    totalHeadersLen += headerLength;
                }
                catch (Exception exc)
                {
                    if (msgSerializationFailureHandler!=null)
                    {
                        msgSerializationFailureHandler(message, exc);
                    }
                    else
                    {
                        throw;
                    }
                    //OnMessageSerializationFailure(message, exc);
                }
            }

            // at least 1 message was successfully serialized
            if (bodies.Count > 0)
            {
                data = new List<ArraySegment<byte>>();
                data.Add(new ArraySegment<byte>(BitConverter.GetBytes(numberOfValidMessages)));
                data.AddRange(lengths);
                data.AddRange(bodies);
                headerLengthOut = totalHeadersLen;
                return true;
            }
            // no message serialized
            else
            {
                return false;
            }
        }

        protected abstract SocketDirection GetSocketDirection();

        protected abstract bool PrepareMessageForSend(Message wmsg);

        protected abstract bool GetSendingSocket(Message wmsg, out Socket sock, out SiloAddress targetSilo, out string error);

        protected abstract void OnGetSendingSocketFailure(Message wmsg, string error);

        //protected abstract bool SerializeMessage(Message wmsg, out List<ArraySegment<byte>> data);

        //protected abstract bool SerializeMessages(List<Message> msgs, out List<ArraySegment<byte>> data);

        protected abstract void OnMessageSerializationFailure(Message wmsg, Exception exc);

        protected abstract void OnSendFailure(Socket socket, SiloAddress targetSilo);

        protected abstract void ProcessMessageAfterSend(Message wmsg, bool sendError, string sendErrorStr);

        protected abstract void FailMessage(Message msg, string reason);
    }
}
