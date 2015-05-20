using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Orleans.Counters;

namespace Orleans.Messaging
{
    /// <summary>
    /// The GatewayConnection class does double duty as both the manager of the connection itself (the socket) and the sender of messages
    /// to the gateway. It uses a single instance of the Receiver class to handle messages from the gateway.
    /// 
    /// Note that both sends and receives are synchronous.
    /// </summary>
    internal class GatewayConnection : OutgoingMessageSender
    {
        internal bool IsLive { get; private set; }
        internal ProxiedMessageCenter MsgCenter { get; private set; }
        private IPEndPoint addr;
        internal IPEndPoint Address
        {
            get { return addr; }
            private set
            {
                addr = value;
                Silo = SiloAddress.New(addr, 0);
            }
        }
        internal SiloAddress Silo { get; private set; }
        private readonly List<ActivationAddress> registeredObjects;
        //private readonly HashSet<CorrelationId> outstandingRequests;

        private readonly GatewayClientReceiver receiver;
        internal Socket Sock { get; private set; }       // Shared by the receiver

        private DateTime lastConnect;

        internal GatewayConnection(IPEndPoint address, ProxiedMessageCenter mc)
            : base("GatewayClientSender_" + address, mc.MessagingConfiguration)
        {
            Address = address;
            MsgCenter = mc;
            receiver = new GatewayClientReceiver(this);
            lastConnect = new DateTime();
            registeredObjects = new List<ActivationAddress>();
            IsLive = true;
        }

        internal void AddObject(ActivationAddress objAddress)
        {
            lock (lockable)
            {
                registeredObjects.Add(objAddress);
            }
        }

        public override void Start()
        {
            if (log.IsVerbose) log.Verbose(ErrorCode.ProxyClient_GatewayConnStarted, "Starting gateway connection for gateway {0}", Address);
            lock (lockable)
            {
                if (State == ThreadState.Running)
                {
                    return;
                }
                Connect();
                if (IsLive) // If the Connect succeeded
                {
                    receiver.Start();
                    base.Start();
                }
            }
        }

        public override void Stop()
        {
            IsLive = false;
            receiver.Stop();
            base.Stop();
            Socket s;
            lock (lockable)
            {
                s = Sock;
                Sock = null;
            }
            if (s != null)
            {
                SocketManager.CloseSocket(s);
                NetworkingStatisticsGroup.OnClosedGWDuplexSocket();
            }
            //// Reject any outstanding requests sent on this connection.

            // Unregister each client-side observer that we've registered on this connection.
            // Using null as the target silo will cause a random connection to be used, which is fine.
            foreach (var observer in registeredObjects)
            {
                MsgCenter.UnregisterObserver(observer.Grain).Ignore();
            }
        }

        // passed the exact same socket on which it got SocketException. This way we prevent races between connect and disconnect.
        public void MarkAsDisconnected(Socket socket2Disconnect)
        {
            Socket s = null;
            lock (lockable)
            {
                if (socket2Disconnect == null || Sock == null) return;
                if (Sock == socket2Disconnect)  // handles races between connect and disconnect, since sometimes we grab the socket outside lock.
                {
                    s = Sock;
                    Sock = null;
                    log.Warn(ErrorCode.ProxyClient_MarkGatewayDisconnected, String.Format("Marking gateway at address {0} as Disconnected", Address));
                }
            }
            if (s != null)
            {
                SocketManager.CloseSocket(s);
                NetworkingStatisticsGroup.OnClosedGWDuplexSocket();
            }
            if (socket2Disconnect != null && socket2Disconnect != s)
            {
                SocketManager.CloseSocket(socket2Disconnect);
                NetworkingStatisticsGroup.OnClosedGWDuplexSocket();
            }
        }

        public void MarkAsDead()
        {
            log.Warn(ErrorCode.ProxyClient_MarkGatewayDead, String.Format("Marking gateway at address {0} as Dead in my client local gateway list.", Address));
            MsgCenter.gatewayManager.MarkAsDead(Address);
            Stop();
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public void Connect()
        {
            if (!MsgCenter.Running)
            {
                if (log.IsVerbose) log.Verbose(ErrorCode.ProxyClient_MsgCtrNotRunning, "Ignoring connection attempt to gateway {0} because the proxy message center is not running", Address);
                return;
            }

            // Yes, we take the lock around a Sleep. The point is to ensure that no more than one thread can try this at a time.
            // There's still a minor problem as written -- if the sending thread and receiving thread both get here, the first one
            // will try to reconnect. eventually do so, and then the other will try to reconnect even though it doesn't have to...
            // Hopefully the initial "if" statement will prevent that.
            lock (lockable)
            {
                if (!IsLive)
                {
                    if (log.IsVerbose) log.Verbose(ErrorCode.ProxyClient_DeadGateway, "Ignoring connection attempt to gateway {0} because this gateway connection is already marked as non live", Address);
                    return; // if the connection is already marked as dead, don't try to reconnect. It has been doomed.
                }

                for (var i = 0; i < ProxiedMessageCenter.CONNECT_RETRY_COUNT; i++)
                {
                    try
                    {
                        if (Sock != null)
                        {
                            if (Sock.Connected)
                                return;
                            else
                            {
                                MarkAsDisconnected(Sock); // clean up the socket before reconnecting.
                            }
                        }
                        if (lastConnect != new DateTime())
                        {
                            var millisecondsSinceLastAttempt = DateTime.UtcNow - lastConnect;
                            if (millisecondsSinceLastAttempt < ProxiedMessageCenter.MINIMUM_INTERCONNECT_DELAY)
                            {
                                var wait = ProxiedMessageCenter.MINIMUM_INTERCONNECT_DELAY - millisecondsSinceLastAttempt;
                                if (log.IsVerbose) log.Verbose(ErrorCode.ProxyClient_PauseBeforeRetry, "Pausing for {0} before trying to connect to gateway {1} on trial {2}", wait, Address, i);
                                Thread.Sleep(wait);
                            }
                        }
                        lastConnect = DateTime.UtcNow;
                        Sock = new Socket(Address.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
                        Sock.Connect(Address);
                        NetworkingStatisticsGroup.OnOpenedGWDuplexSocket();
                        Sock.Send(MsgCenter.Id.ToByteArray()); // Identifies this client
                        //MsgCenter.MyAddress = SiloAddress.New(Sock.LocalEndPoint as IPEndPoint, -1); // -1 indicates client
                        log.Info(ErrorCode.ProxyClient_Connected, "Connected to gateway at address {0} on trial {1}.", Address, i);
                        return;
                    }
                    catch (Exception ex)
                    {
                        log.Warn(ErrorCode.ProxyClient_CannotConnect, String.Format("Unable to connect to gateway at address {0} on trial {1}.", Address, i), ex);
                        MarkAsDisconnected(Sock);
                    }
                }
                // Failed too many times -- give up
                MarkAsDead();
                return;
            }
        }

        // This is used to send a message to the gateway silo
        //protected override void Process(Message wmsg)
        //{
            //bool continueSend = true;
            //continueSend = PrepareMessageForSend(wmsg);
            //if (!continueSend)
            //{
            //    return;
            //}

            //Socket sock = null;
            //string error;
            //IPEndPoint targetEndpoint;
            //continueSend = GetSendingSocket(wmsg, out sock, out targetEndpoint, out error);
            //if (!continueSend)
            //{
            //    OnGetSendingSocketFailure(wmsg, error);
            //    return;
            //}

            //List<ArraySegment<byte>> data = null;
            //continueSend = SerializeMessage(wmsg, out data);
            //if (!continueSend)
            //{
            //    return;
            //}

            //int length = data.Sum<ArraySegment<byte>>(x => x.Count);
            //int bytesSent = 0;
            //bool exceptionSending = false;
            //bool countMismatchSending = false;
            //string sendErrorStr = null;
            //try
            //{
            //    bytesSent = sock.Send(data);
            //    if (bytesSent != length)
            //    {
            //        // The complete message wasn't sent, even though no error was reported; treat this as an error
            //        countMismatchSending = true;
            //        sendErrorStr = String.Format("Byte count mismatch on send to gateway {0}: sent {1}, expected {2}", targetEndpoint, bytesSent, length);
            //        log.Warn(ErrorCode.ProxyClient_ByteCountMismatch, sendErrorStr);
            //    }
            //}
            //catch (Exception exc)
            //{
            //    exceptionSending = true;
            //    if (cts.IsCancellationRequested)
            //    {
            //        return;
            //    }
            //    if (exc is SocketException)
            //    {
            //        sendErrorStr = String.Format("Network error sending message to gateway {0}. Message: {1}", targetEndpoint, wmsg);
            //        log.Warn(ErrorCode.ProxyClient_SocketSendError, sendErrorStr, exc);
            //    }
            //    else
            //    {
            //        sendErrorStr = String.Format("Exception sending message to gateway {0}. Message: {1}", targetEndpoint, wmsg);
            //        log.Warn(ErrorCode.ProxyClient_SendException, sendErrorStr, exc);
            //    }
            //}
            //MessagingStatisticsGroup.OnMessageSend(wmsg.TargetSilo, bytesSent);
            //bool sendError = exceptionSending || countMismatchSending;
            //if (sendError)
            //{
            //    OnSendFailure(sock, null);
            //}
            //ProcessMessageAfterSend(wmsg, sendError, sendErrorStr);
        //}

        //------- IMPL. -----//

        protected override SocketDirection GetSocketDirection() { return SocketDirection.ClientToGW; }

        protected override bool PrepareMessageForSend(Message wmsg)
        {
            // Check to make sure we're not stopped
            if (cts.IsCancellationRequested)
            {
                // Recycle the message we've dequeued. Note that this will recycle messages that were queued up to be sent when the gateway connection is declared dead
                MsgCenter.SendMessage(wmsg);
                return false;
            }

            if (wmsg.TargetSilo == null)
            {
                wmsg.TargetSilo = SiloAddress.New(Address, 0);
                if (wmsg.TargetGrain.IsSystemTarget)
                {
                    wmsg.TargetActivation = ActivationId.GetSystemActivation(wmsg.TargetGrain, wmsg.TargetSilo);
                }
            }
            return true;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        protected override bool GetSendingSocket(Message wmsg, out Socket socketCapture, out SiloAddress targetSilo, out string error)
        {
            error = null;
            targetSilo = SiloAddress.New(Address, 0);
            socketCapture = null;
            try
            {
                if (Sock == null || !Sock.Connected)
                {
                    Connect();
                }
                socketCapture = Sock;
                if (socketCapture == null || !socketCapture.Connected)
                {
                    // Failed to connect -- Connect will have already declared this connection dead, so recycle the message
                    //MsgCenter.SendMessage(wmsg);
                    return false;
                }
            }
            catch (Exception)
            {
                //No need to log any errors, as we alraedy do it inside Connect().
                //MsgCenter.SendMessage(wmsg);
                return false;
            }
            return true;
        }

        protected override void OnGetSendingSocketFailure(Message wmsg, string error)
        {
            MsgCenter.SendMessage(wmsg);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        protected override void OnMessageSerializationFailure(Message wmsg, Exception exc)
        {
            // we only get here if we failed to serialise the msg (or any other catastrophic failure).
            // Request msg fails to serialise on the sending silo, so we just enqueue a rejection msg.
            log.Warn(ErrorCode.ProxyClient_SerializationError, String.Format("Unexpected error serializing message to gateway {0}.", Address), exc);
            FailMessage(wmsg, String.Format("Unexpected error serializing message to gateway {0}. {1}", Address, exc));
            if (wmsg.Direction == Message.Directions.Request || wmsg.Direction == Message.Directions.OneWay)
            {
                return;
            }

            // Response msg fails to serialize on the responding silo, so we try to send an error response back.
            // if we failed sending an original response, turn the response body into an error and reply with it.
            wmsg.Result = Message.ResponseTypes.Error;
            wmsg.BodyObject = OrleansResponse.ExceptionResponse(exc);
            try
            {
                MsgCenter.SendMessage(wmsg);
                //data = wmsg.Serialize();
            }
            catch (Exception ex)
            {
                // If we still can't serialize, drop the message on the floor
                log.Warn(ErrorCode.ProxyClient_DroppingMsg, "Unable to serialize message - DROPPING " + wmsg, ex);
                wmsg.ReleaseBodyAndHeaderBuffers();
                return;
            }
        }

        protected override void OnSendFailure(Socket socket, SiloAddress targetSilo)
        {
            MarkAsDisconnected(socket);
        }

        protected override void ProcessMessageAfterSend(Message wmsg, bool sendError, string sendErrorStr)
        {
            wmsg.ReleaseBodyAndHeaderBuffers();
            if (sendError)
            {
                // We can't recycle the current message, because that might wind up with it getting delivered out of order, so we have to reject it
                FailMessage(wmsg, sendErrorStr);
            }
        }

        protected override void FailMessage(Message msg, string reason)
        {
            msg.ReleaseBodyAndHeaderBuffers();
            MessagingStatisticsGroup.OnFailedSentMessage(msg);
            if (MsgCenter.Running && msg.Direction == Message.Directions.Request)
            {
                if (log.IsVerbose) log.Verbose(ErrorCode.ProxyClient_RejectingMsg, "Rejecting message: {0}. Reason = {1}", msg, reason);
                MessagingStatisticsGroup.OnRejectedMessage(msg);
                Message error = msg.CreateRejectionResponse(Message.RejectionTypes.Unrecoverable, reason);
                MsgCenter.QueueIncomingMessage(error);
            }
            else
            {
                log.Warn(ErrorCode.ProxyClient_DroppingMsg, "Dropping message: {0}. Reason = {1}", msg, reason);
                MessagingStatisticsGroup.OnDroppedSentMessage(msg);
                return;
            }
        }
    }
}
