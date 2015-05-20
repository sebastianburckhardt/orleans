using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Collections.Concurrent;
using System.Threading;
using Orleans.Counters;
using Orleans.Messaging;

namespace Orleans.Runtime.Messaging
{
    internal class IncomingMessageAcceptor : AsynchAgent
    {
        internal Socket acceptingSocket;
        protected MessageCenter mc;
        protected HashSet<Socket> openReceiveSockets;
        private readonly IPEndPoint listenAddress;
        private Action<Message> sniffIncomingMessageHandler;

        public Action<Message> SniffIncomingMessage
        {
            set
            {
                if (sniffIncomingMessageHandler != null)
                    throw new InvalidOperationException("IncomingMessageAcceptor SniffIncomingMessage already set");
                sniffIncomingMessageHandler = value;
            }
        }

        private const int LISTEN_BACKLOG_SIZE = 1024;

        protected SocketDirection SocketDirection
        {
            get;
            private set;
        }

        // Used for holding enough info to handle receive completion
        internal class ReceiveCallbackContext
        {
            internal enum ReceivePhase
            {
                Lengths,
                Header,
                Body,
                MetaHeader,
                HeaderBodies
            }

            public ReceivePhase Phase { get; set; }
            public Socket Sock { get; set; }
            public EndPoint RemoteEndPoint { get; set; }
            public IncomingMessageAcceptor IMA { get; set; }
            public byte[] LengthBuffer { get; set; }
            public byte[] MetaHeaderBuffer { get; set; }
            public List<ArraySegment<byte>> Lengths { get; set; }
            public List<ArraySegment<byte>> Header { get; set; }
            public List<ArraySegment<byte>> Body { get; set; }
            public List<ArraySegment<byte>> MetaHeader { get; set; }
            public List<ArraySegment<byte>> HeaderBodies { get; set; }
            public int HeaderLength { get; set; }
            public int BodyLength { get; set; }
            public int[] HeaderLengths { get; set; }
            public int[] BodyLengths { get; set; }
            public int HeaderBodiesLength { get; set; }
            public int Offset { get; set; }
            public bool batchingMode { get; set; }
            public int numberOfMessages { get; set; }
            public List<ArraySegment<byte>> CurrentBuffer
            {
                get
                {
                    if (batchingMode)
                    {
                        switch (Phase)
                        {
                            case ReceivePhase.MetaHeader:
                                return MetaHeader;
                            case ReceivePhase.Lengths:
                                return Lengths;
                            default:
                                return HeaderBodies;
                        }
                    }
                    else
                    {
                        switch (Phase)
                        {
                            case ReceivePhase.Lengths:
                                return Lengths;
                            case ReceivePhase.Header:
                                return Header;
                            default:
                                return Body;
                        }
                    }
                }
            }
            public int CurrentLength
            {
                get
                {
                    if (batchingMode)
                    {
                        switch (Phase)
                        {
                            case ReceivePhase.MetaHeader:
                                return Message.LengthMetaHeader;
                            case ReceivePhase.Lengths:
                                if (numberOfMessages == 0)
                                {
                                    IMA.log.Info("Error: numberOfMessages must NOT be 0 here.");
                                    return 0;
                                }
                                return Message.LengthHeaderSize * numberOfMessages;
                            default:
                                return HeaderBodiesLength;
                        }
                    }
                    else
                    {
                        switch (Phase)
                        {
                            case ReceivePhase.Lengths:
                                return Message.LengthHeaderSize;
                            case ReceivePhase.Header:
                                return HeaderLength;
                            default:
                                return BodyLength;
                        }
                    }
                }
            }

            public ReceiveCallbackContext(Socket sock, IncomingMessageAcceptor ima)
            {
                batchingMode = ima.mc.MessagingConfiguration.UseMessageBatching;
                if (batchingMode)
                {
                    Phase = ReceivePhase.MetaHeader;
                    Sock = sock;
                    RemoteEndPoint = sock.RemoteEndPoint;
                    IMA = ima;
                    MetaHeaderBuffer = new byte[Message.LengthMetaHeader];
                    MetaHeader = new List<ArraySegment<byte>>() { new ArraySegment<byte>(MetaHeaderBuffer) };
                    // LengthBuffer and Lengths cannot be allocated here because the sizes varies in response to the number of received messages
                    LengthBuffer = null;
                    Lengths = null;
                    Header = null;
                    Body = null;
                    HeaderBodies = null;
                    HeaderLengths = null;
                    BodyLengths = null;
                    HeaderBodiesLength = 0;
                    numberOfMessages = 0;
                    Offset = 0;
                }
                else
                {
                    Phase = ReceivePhase.Lengths;
                    Sock = sock;
                    RemoteEndPoint = sock.RemoteEndPoint;
                    IMA = ima;
                    LengthBuffer = new byte[Message.LengthHeaderSize];
                    Lengths = new List<ArraySegment<byte>>() { new ArraySegment<byte>(LengthBuffer) };
                    Header = null;
                    Body = null;
                    HeaderLength = 0;
                    BodyLength = 0;
                    Offset = 0;
                }
            }

            public void Reset()
            {
                if (batchingMode)
                {
                    Phase = ReceivePhase.MetaHeader;
                    // MetaHeader MUST NOT set to null because it will be re-used.
                    LengthBuffer = null;
                    Lengths = null;
                    Header = null;
                    Body = null;
                    HeaderLengths = null;
                    BodyLengths = null;
                    HeaderBodies = null;
                    HeaderBodiesLength = 0;
                    numberOfMessages = 0;
                    Offset = 0;
                }
                else
                {
                    Phase = ReceivePhase.Lengths;
                    HeaderLength = 0;
                    BodyLength = 0;
                    Offset = 0;
                    Header = null;
                    Body = null;
                }
            }

            // Builds the list of buffer segments to pass to Socket.BeginReceive, based on the total list (CurrentBuffer)
            // and how much we've already filled in (Offset). We have to do this because the scatter/gather variant of
            // the BeginReceive API doesn't allow you to specify an offset into the list of segments.
            // To build the list, we walk through the complete buffer, skipping segments that we've already filled up; 
            // add the partial segment for whatever's left in the first unfilled buffer, and then add any remaining buffers.
            private List<ArraySegment<byte>> BuildSegmentList()
            {
                return ByteArrayBuilder.BuildSegmentList(CurrentBuffer, Offset);
            }

            //public static List<ArraySegment<byte>> BuildSegmentList(List<ArraySegment<byte>> buffer, int offset)
            //{
            //    if (offset == 0)
            //    {
            //        return buffer;
            //    }

            //    var result = new List<ArraySegment<byte>>();
            //    var lengthSoFar = 0;
            //    foreach (var segment in buffer)
            //    {
            //        var bytesStillToSkip = offset - lengthSoFar;
            //        lengthSoFar += segment.Count;
            //        if (segment.Count <= bytesStillToSkip) // Still skipping past this buffer
            //        {
            //            continue;
            //        }
            //        if (bytesStillToSkip > 0) // This is the first buffer, so just take part of it
            //        {
            //            result.Add(new ArraySegment<byte>(segment.Array, bytesStillToSkip, segment.Count - bytesStillToSkip));
            //        }
            //        else // Take the whole buffer
            //        {
            //            result.Add(segment);
            //        }
            //    }
            //    return result;
            //}

            public void BeginReceive(AsyncCallback callback)
            {
                try
                {
                    Sock.BeginReceive(BuildSegmentList(), SocketFlags.None, callback, this);
                }
                catch (Exception ex)
                {
                    IMA.log.Warn(ErrorCode.MessagingBeginReceiveException, "Exception trying to begin receive from endpoint " + RemoteEndPoint, ex);
                    throw;
                }
            }
            
#if TRACK_DETAILED_STATS
            // Global collection of ThreadTrackingStatistic for thread pool and IO completion threads.
            public static readonly ConcurrentDictionary<int, ThreadTrackingStatistic> trackers = new ConcurrentDictionary<int, ThreadTrackingStatistic>();
#endif

            public void ProcessReceivedBuffer(int bytes)
            {
                Offset += bytes;
                if (Offset < CurrentLength)
                {
                    // Nothing to do except start the next receive
                    return;
                }

#if TRACK_DETAILED_STATS
                ThreadTrackingStatistic tracker = null;
                if (StatisticsCollector.CollectThreadTimeTrackingStats)
                {
                    int id = Thread.CurrentThread.ManagedThreadId;
                    if (!trackers.TryGetValue(id, out tracker))
                    {
                        tracker = new ThreadTrackingStatistic("ThreadPoolThread." + Thread.CurrentThread.ManagedThreadId);
                        bool added = trackers.TryAdd(id, tracker);
                        if (added)
                        {
                            tracker.OnStartExecution();
                        }
                    }
                    tracker.OnStartProcessing();
                }
#endif

                try
                {
                    if (batchingMode)
                    {
                        switch (Phase)
                        {
                            case ReceivePhase.MetaHeader:
                                numberOfMessages = BitConverter.ToInt32(MetaHeaderBuffer, 0);
                                LengthBuffer = new byte[numberOfMessages * Message.LengthHeaderSize];
                                Lengths = new List<ArraySegment<byte>>() { new ArraySegment<byte>(LengthBuffer) };
                                Phase = ReceivePhase.Lengths;
                                Offset = 0;
                                break;
                            case ReceivePhase.Lengths:
                                HeaderBodies = new List<ArraySegment<byte>>();
                                HeaderLengths = new int[numberOfMessages];
                                BodyLengths = new int[numberOfMessages];

                                for (int i = 0; i < numberOfMessages; i++)
                                {
                                    HeaderLengths[i] = BitConverter.ToInt32(LengthBuffer, i * 8);
                                    BodyLengths[i] = BitConverter.ToInt32(LengthBuffer, i * 8 + 4);
                                    HeaderBodiesLength += (HeaderLengths[i] + BodyLengths[i]);

                                    // We need to set the boundary of ArraySegment<byte>s to the same as the header/body boundary
                                    HeaderBodies.AddRange(BufferPool.GlobalPool.GetMultiBuffer(HeaderLengths[i]));
                                    HeaderBodies.AddRange(BufferPool.GlobalPool.GetMultiBuffer(BodyLengths[i]));
                                }

                                Phase = ReceivePhase.HeaderBodies;
                                Offset = 0;
                                break;
                            case ReceivePhase.HeaderBodies:
                                int lengtshSoFar = 0;

                                for (int i = 0; i < numberOfMessages; i++)
                                {
                                    Header = ByteArrayBuilder.BuildSegmentListWithLengthLimit(HeaderBodies, lengtshSoFar, HeaderLengths[i]);
                                    Body = ByteArrayBuilder.BuildSegmentListWithLengthLimit(HeaderBodies, lengtshSoFar + HeaderLengths[i], BodyLengths[i]);
                                    lengtshSoFar += (HeaderLengths[i] + BodyLengths[i]);

                                    Message msg = new Message(Header, Body);
                                    MessagingStatisticsGroup.OnMessageReceive(msg, HeaderLengths[i], BodyLengths[i]);

                                    if (IMA.log.IsVerbose3) IMA.log.Verbose3("Received a complete message of {0} bytes from {1}", HeaderLengths[i] + BodyLengths[i], msg.SendingAddress);
                                    if (HeaderLengths[i] + BodyLengths[i] > Message.LargeMessageSizeThreshold)
                                    {
                                        IMA.log.Info(ErrorCode.Messaging_LargeMsg_Incoming, "Receiving large message Size={0} HeaderLength={1} BodyLength={2}. Msg={3}",
                                            HeaderLengths[i] + BodyLengths[i], HeaderLengths[i], BodyLengths[i], msg.ToString());
                                        if (IMA.log.IsVerbose3) IMA.log.Verbose3("Received large message {0}", msg.ToLongString());
                                    }
                                    IMA.HandleMessage(msg, Sock);
                                }
                                MessagingStatisticsGroup.OnMessageBatchReceive(IMA.SocketDirection, numberOfMessages, lengtshSoFar);

                                Reset();
                                break;
                        }
                    }
                    else
                    {
                        // We've completed a buffer. What we do depends on which phase we were in
                        switch (Phase)
                        {
                            case ReceivePhase.Lengths:
                                // Pull out the header and body lengths
                                HeaderLength = BitConverter.ToInt32(LengthBuffer, 0);
                                BodyLength = BitConverter.ToInt32(LengthBuffer, 4);
                                Header = BufferPool.GlobalPool.GetMultiBuffer(HeaderLength);
                                Body = BufferPool.GlobalPool.GetMultiBuffer(BodyLength);
                                Phase = ReceivePhase.Header;
                                Offset = 0;
                                break;
                            case ReceivePhase.Header:
                                Phase = ReceivePhase.Body;
                                Offset = 0;
                                break;
                            case ReceivePhase.Body:
                                Message msg = new Message(Header, Body);
                                MessagingStatisticsGroup.OnMessageReceive(msg, HeaderLength, BodyLength);

                                if (IMA.log.IsVerbose3) IMA.log.Verbose3("Received a complete message of {0} bytes from {1}", HeaderLength + BodyLength, msg.SendingAddress);
                                if (HeaderLength + BodyLength > Message.LargeMessageSizeThreshold)
                                {
                                    IMA.log.Info(ErrorCode.Messaging_LargeMsg_Incoming, "Receiving large message Size={0} HeaderLength={1} BodyLength={2}. Msg={3}",
                                        HeaderLength + BodyLength, HeaderLength, BodyLength, msg.ToString());
                                    if (IMA.log.IsVerbose3) IMA.log.Verbose3("Received large message {0}", msg.ToLongString());
                                }
                                IMA.HandleMessage(msg, Sock);
                                Reset();
                                break;
                        }
                    }
                }
                catch (Exception exc)
                {
                    try
                    {
                        // Log details of receive state machine
                        IMA.log.Error(ErrorCode.MessagingProcessReceiveBufferException,
                            string.Format(
                            "Exception trying to process {0} bytes from endpoint {1} at offset {2} in phase {3}"
                            + " CurrentLength={4} HeaderLength={5} BodyLength={6}",
                                bytes, RemoteEndPoint, Offset, Phase,
                                CurrentLength, HeaderLength, BodyLength
                            ),
                            exc);
                    }
                    catch (Exception) { }
                    Reset(); // Reset back to a hopefully good base state

                    throw;
                }
                finally
                {
#if TRACK_DETAILED_STATS
                    if (StatisticsCollector.CollectThreadTimeTrackingStats)
                    {
                        tracker.IncrementNumberOfProcessed();
                        tracker.OnStopProcessing();
                    }
#endif
                }
            }
        }

        internal IncomingMessageAcceptor(MessageCenter msgCtr, IPEndPoint here, SocketDirection socketDirection)
        {
            mc = msgCtr;
            listenAddress = here;
            if (here == null)
            {
                listenAddress = mc.MyAddress.Endpoint;
            }
            acceptingSocket = SocketManager.GetAcceptingSocketForEndpoint(listenAddress);
            log.Info(ErrorCode.Messaging_IMA_OpenedListeningSocket, "Opened a listening socket at address " + acceptingSocket.LocalEndPoint);
            openReceiveSockets = new HashSet<Socket>();
            onFault = FaultBehavior.CrashOnFault;
            SocketDirection = socketDirection;
        }

        protected override void Run()
        {
            //log.Verbose("About to start accepting connections.");
            try
            {
                acceptingSocket.Listen(LISTEN_BACKLOG_SIZE);
                acceptingSocket.BeginAccept(new AsyncCallback(AcceptCallback), this);
            }
            catch (Exception ex)
            {
                log.Error(ErrorCode.MessagingBeginAcceptSocketException, "Exception beginning accept on listening socket", ex);
                throw;
            }
            if (log.IsVerbose3) log.Verbose3("Started accepting connections.");
        }

        public override void Stop()
        {
            base.Stop();

            if (log.IsVerbose) log.Verbose("Disconnecting the listening socket");
            SocketManager.CloseSocket(acceptingSocket);

            Socket[] temp;
            lock (lockable)
            {
                temp = new Socket[openReceiveSockets.Count];
                openReceiveSockets.CopyTo(temp);
            }
            foreach (Socket sock in temp)
            {
                SafeCloseSocket(sock);
            }
            lock (lockable)
            {
                ClearSockets();
            }
        }

        protected virtual bool RecordOpenedSocket(Socket sock)
        {
            Guid client;
            if (!ReceiveSocketPreample(sock, false, out client))
            {
                return false;
            }
            NetworkingStatisticsGroup.OnOpenedReceiveSocket();
            return true;
        }

        protected bool ReceiveSocketPreample(Socket sock, bool expectProxiedConnection, out Guid client)
        {
            client = default(Guid);

            if (cts.IsCancellationRequested)
            {
                return false;
            }

            // Receive the client ID
            var buffer = new byte[16];
            int offset = 0;

            while (offset < buffer.Length)
            {
                try
                {
                    int bytesRead = sock.Receive(buffer, offset, buffer.Length - offset, SocketFlags.None);
                    if (bytesRead == 0)
                    {
                        log.Warn(ErrorCode.GatewayAcceptor_SocketClosed, 
                            "Remote socket closed while receiving client ID from endpoint {0}.", sock.RemoteEndPoint);
                        return false;
                    }
                    offset += bytesRead;
                }
                catch (Exception ex)
                {
                    log.Warn(ErrorCode.GatewayAcceptor_ExceptionReceiving, "Exception receiving client ID from endpoint " + sock.RemoteEndPoint, ex);
                    return false;
                }
            }

            client = new Guid(buffer);

            if (log.IsVerbose2) log.Verbose2(ErrorCode.MessageAcceptor_Connection, "Received connection from client {0} at source address {1}", client, sock.RemoteEndPoint.ToString());

            if (expectProxiedConnection)
            {
                // Proxied Gateway Connection - must have sender id
                if (client == SocketManager.SiloDirectConnectionId)
                {
                    log.Error(ErrorCode.MessageAcceptor_NotAProxiedConnection, string.Format("Gateway received unexpected non-proxied connection from client {0} at source address {1}", client, sock.RemoteEndPoint));
                    return false;
                }
            }
            else
            {
                // Direct connection - should not have sender id
                if (client != SocketManager.SiloDirectConnectionId)
                {
                    log.Error(ErrorCode.MessageAcceptor_UnexpectedProxiedConnection, string.Format("Silo received unexpected proxied connection from client {0} at source address {1}", client, sock.RemoteEndPoint));
                    return false;
                }
            }

            lock (lockable)
            {
                openReceiveSockets.Add(sock);
            }

            return true;
        }
        protected virtual void RecordClosedSocket(Socket sock)
        {
            if (this.TryRemoveClosedSocket(sock))
            {
                NetworkingStatisticsGroup.OnClosedReceivingSocket();
            }
        }

        protected bool TryRemoveClosedSocket(Socket sock)
        {
            lock (lockable)
            {
                return openReceiveSockets.Remove(sock);
            }
        }

        protected virtual void ClearSockets()
        {
            openReceiveSockets.Clear();
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA2204:Literals should be spelled correctly", MessageId = "BeginAccept")]
        private static void AcceptCallback(IAsyncResult result)
        {
            var ima = result.AsyncState as IncomingMessageAcceptor;
            try
            {
                if (ima == null)
                {
                    var logger = Logger.GetLogger("IncomingMessageAcceptor", Logger.LoggerType.Runtime);
                    if (result.AsyncState == null)
                    {
                        logger.Warn(ErrorCode.Messaging_IMA_AcceptCallbackNullState, "AcceptCallback invoked with a null unexpected async state");
                    }
                    else
                    {
                        logger.Warn(ErrorCode.Messaging_IMA_AcceptCallbackUnexpectedState, "AcceptCallback invoked with an unexpected async state of type {0}", result.AsyncState.GetType());
                    }
                    return;
                }

                // First check to see if we're shutting down, in which case there's no point in doing anything other
                // than closing the accepting socket and returning.
                if (ima.cts.IsCancellationRequested)
                {
                    SocketManager.CloseSocket(ima.acceptingSocket);
                    ima.log.Info(ErrorCode.Messaging_IMA_ClosingSocket, "Closing accepting socket during shutdown");
                    return;
                }

                // Then, start a new Accept
                try
                {
                    ima.acceptingSocket.BeginAccept(new AsyncCallback(AcceptCallback), ima);
                }
                catch (Exception ex)
                {
                    ima.log.Warn(ErrorCode.MessagingBeginAcceptSocketException, "Exception on accepting socket during BeginAccept", ex);
                    // Open a new one
                    ima.RestartAcceptingSocket();
                }

                Socket sock;
                // Complete this accept
                try
                {
                    sock = ima.acceptingSocket.EndAccept(result);
                }
                catch (ObjectDisposedException)
                {
                    // Socket was closed, but we're not shutting down; we need to open a new socket and start over...
                    // Close the old socket and open a new one
                    ima.log.Warn(ErrorCode.MessagingAcceptingSocketClosed, "Accepting socket was closed when not shutting down");
                    ima.RestartAcceptingSocket();
                    return;
                }
                catch (Exception ex)
                {
                    // There was a network error. We need to get a new accepting socket and re-issue an accept before we continue.
                    // Close the old socket and open a new one
                    ima.log.Warn(ErrorCode.MessagingEndAcceptSocketException, "Exception on accepting socket during EndAccept", ex);
                    ima.RestartAcceptingSocket();
                    return;
                }

                if (ima.log.IsVerbose3) ima.log.Verbose3("Received a connection from {0}", sock.RemoteEndPoint);

                // Finally, process the incoming request:
                // Prep the socket so it will reset on close
                sock.LingerState = new LingerOption(true, 0);

                // Add the socket to the open socket collection
                if (ima.RecordOpenedSocket(sock))
                {
                    // And set up the asynch receive
                    var rcc = new ReceiveCallbackContext(sock, ima);
                    try
                    {
                        rcc.BeginReceive(new AsyncCallback(ReceiveCallback));
                    }
                    catch (Exception exception)
                    {
                        SocketException socketException = exception as SocketException;
                        ima.log.Warn(ErrorCode.Messaging_IMA_NewBeginReceiveException,
                                String.Format("Exception on new socket during BeginReceive with RemoteEndPoint {0}: {1}",
                                socketException != null ? socketException.SocketErrorCode.ToString() : "", rcc.RemoteEndPoint), exception);
                        ima.SafeCloseSocket(sock);
                    }
                }
                else
                {
                    ima.SafeCloseSocket(sock);
                }
            }
            catch (Exception ex)
            {
                var logger = ima != null ? ima.log : Logger.GetLogger("IncomingMessageAcceptor", Logger.LoggerType.Runtime);
                logger.Error(ErrorCode.Messaging_IMA_ExceptionAccepting, "Unexpected exception in IncomingMessageAccepter.AcceptCallback", ex);
            }
        }

        private static void ReceiveCallback(IAsyncResult result)
        {
            ReceiveCallbackContext rcc = result.AsyncState as ReceiveCallbackContext;

            if (rcc == null)
            {
                // This should never happen. Trap it and drop it on the floor because allowing a null reference exception would
                // kill the process silently.
                return;
            }

            try
            {
                // First check to see if we're shutting down, in which case there's no point in doing anything other
                // than closing the accepting socket and returning.
                if (rcc.IMA.cts.IsCancellationRequested)
                {
                    // We're exiting, so close the socket and clean up
                    rcc.IMA.SafeCloseSocket(rcc.Sock);
                }

                int bytes = 0;
                // Complete the receive
                try
                {
                    bytes = rcc.Sock.EndReceive(result);
                }
                catch (ObjectDisposedException)
                {
                    // The socket is closed. Just clean up and return.
                    rcc.IMA.RecordClosedSocket(rcc.Sock);
                    return;
                }
                catch (Exception ex)
                {
                    rcc.IMA.log.Warn(ErrorCode.Messaging_ExceptionReceiving, "Exception while completing a receive from " + rcc.Sock.RemoteEndPoint, ex);
                    // Either there was a network error or the socket is being closed. Either way, just clean up and return.
                    rcc.IMA.SafeCloseSocket(rcc.Sock);
                    return;
                }

                //rcc.IMA.log.Verbose("Receive completed with " + bytes.ToString(CultureInfo.InvariantCulture) + " bytes");
                if (bytes == 0)
                {
                    // Socket was closed by the sender. so close our end
                    rcc.IMA.SafeCloseSocket(rcc.Sock);
                    // And we're done
                    return;
                }

                // Process the buffer we received
                try
                {
                    rcc.ProcessReceivedBuffer(bytes);
                }
                catch (Exception ex)
                {
                    rcc.IMA.log.Error(ErrorCode.Messaging_IMA_BadBufferReceived,
                                      String.Format("ProcessReceivedBuffer exception with RemoteEndPoint {0}: ",
                                                    rcc.RemoteEndPoint), ex);
                    // There was a problem with the buffer, presumably data corruption, so give up
                    rcc.IMA.SafeCloseSocket(rcc.Sock);
                    // And we're done
                    return;
                }

                // Start the next receive. Note that if this throws, the exception will be logged in the catch below.
                rcc.BeginReceive(ReceiveCallback);
            }
            catch (Exception ex)
            {
                rcc.IMA.log.Warn(ErrorCode.Messaging_IMA_DroppingConnection, "Exception receiving from end point " + rcc.RemoteEndPoint, ex);
                rcc.IMA.SafeCloseSocket(rcc.Sock);
            }
        }

        internal readonly static string PingHeader = Message.Header.ApplicationHeaderFlag + Message.Header.PingApplicationHeader;

        protected virtual void HandleMessage(Message wmsg, Socket receivedOnSocket)
        {
            wmsg.AddTimestamp(Message.LifecycleTag.ReceiveIncoming);

            // See it's a Ping message, and if so, short-circuit it
            if (wmsg.GetScalarHeader<bool>(PingHeader))
            {
                MessagingStatisticsGroup.OnPingReceive(wmsg.SendingSilo);

                if (log.IsVerbose2) log.Verbose2("Responding to Ping from {0}", wmsg.SendingSilo);

                if (!wmsg.TargetSilo.Equals(mc.MyAddress)) // got ping that is not destined to me. For example, got a ping to my older incarnation.
                {
                    MessagingStatisticsGroup.OnRejectedMessage(wmsg);
                    Message rejection = wmsg.CreateRejectionResponse(Message.RejectionTypes.FutureTransient,
                        string.Format("The target silo is no longer active: target was {0}, but this silo is {1}. The rejected ping message is {2}.",
                            wmsg.TargetSilo.ToLongString(), mc.MyAddress.ToLongString(), wmsg.ToString()));
                    mc.OutboundQueue.SendMessage(rejection);
                }else
                {
                    var response = wmsg.CreateResponseMessage();
                    response.BodyObject = OrleansResponse.Done;   
                    mc.SendMessage(response);
                }
                return;
            }

            // sniff message headers for directory cache management
            if (sniffIncomingMessageHandler != null)
                sniffIncomingMessageHandler(wmsg);

            // Don't process messages that have already timed out
            if (wmsg.IsExpired)
            {
                wmsg.DropExpiredMessage(MessagingStatisticsGroup.Phase.Receive);
                return;
            }

            // If we've stopped application message processing, then filter those out now
            // Note that if we identify or add other grains that are required for proper stopping, we will need to treat them as we do the membership table grain here.
            if (mc.IsBlockingApplicationMessages && (wmsg.Category == Message.Categories.Application) && (wmsg.SendingGrain != Constants.SystemMembershipTableId))
            {
                // We reject new requests, and drop all other messages
                if (wmsg.Direction == Message.Directions.Request)
                {
                    MessagingStatisticsGroup.OnRejectedMessage(wmsg);
                    var reject = wmsg.CreateRejectionResponse(Message.RejectionTypes.Unrecoverable, "Silo stopping");
                    mc.SendMessage(reject);
                }
                return;
            }

            // See if we're supposed to re-route the message
            if (wmsg.GetScalarHeader<bool>(Message.Header.ReroutingRequested))
            {
                // To re-route, we clear the rerouting and target silo headers, and just send the message
                if (log.IsVerbose2) log.Verbose2("Rerouting message from {0}", wmsg.SendingSilo);
                wmsg.RemoveHeader(Message.Header.ReroutingRequested);
                wmsg.RemoveHeader(Message.Header.TargetSilo);
                wmsg.AddTimestamp(Message.LifecycleTag.RerouteIncoming);

                MessagingStatisticsGroup.OnMessageReRoute(wmsg);
                mc.RerouteMessage(wmsg);
                return;
            }

            // Make sure the message is for us. Note that some control messages may have no target
            // information, so a null target silo is OK.
            if ((wmsg.TargetSilo == null) || wmsg.TargetSilo.Matches(mc.MyAddress))
            {
                // See if it's a message for a client we're proxying.
                if (mc.Gateway != null)
                {
                    if (mc.Gateway.TryDeliverToProxy(wmsg))
                    {
                        return;
                    }
                }

                // Nope, it's for us
                mc.InboundQueue.PostMessage(wmsg);
                return;
            }

            if (!wmsg.TargetSilo.Endpoint.Equals(mc.MyAddress.Endpoint))
            {
                // If the message is for some other silo altogether, then we need to forward it.
                if (log.IsVerbose2) log.Verbose2("Forwarding message {0} from {1} to silo {2}", wmsg.Id, wmsg.SendingSilo, wmsg.TargetSilo);
                wmsg.AddTimestamp(Message.LifecycleTag.EnqueueForForwarding);
                mc.OutboundQueue.SendMessage(wmsg);
                return;
            }

            // If the message was for this endpoint but an older epoch, then reject the message
            // (if it was a request), or drop it on the floor if it was a response or one-way.
            if (wmsg.Direction == Message.Directions.Request)
            {
                MessagingStatisticsGroup.OnRejectedMessage(wmsg);
                Message rejection = wmsg.CreateRejectionResponse(Message.RejectionTypes.FutureTransient,
                    string.Format("The target silo is no longer active: target was {0}, but this silo is {1}. The rejected message is {2}.", 
                        wmsg.TargetSilo.ToLongString(), mc.MyAddress.ToLongString(), wmsg.ToString()));
                mc.OutboundQueue.SendMessage(rejection);
                if (log.IsVerbose) log.Verbose("Rejecting an obsolete request; target was {0}, but this silo is {1}. The rejected message is {2}.",
                    wmsg.TargetSilo.ToLongString(), mc.MyAddress.ToLongString(), wmsg.ToString());
                return;
            }
        }

        private void RestartAcceptingSocket()
        {
            try
            {
                SocketManager.CloseSocket(acceptingSocket);
                acceptingSocket = SocketManager.GetAcceptingSocketForEndpoint(listenAddress);
                acceptingSocket.Listen(LISTEN_BACKLOG_SIZE);
                acceptingSocket.BeginAccept(new AsyncCallback(AcceptCallback), this);
            }
            catch (Exception ex)
            {
                log.Error(ErrorCode.Runtime_Error_100016, "Unable to create a new accepting socket", ex);
                throw;
            }
        }

        private void SafeCloseSocket(Socket sock)
        {
            RecordClosedSocket(sock);

            SocketManager.CloseSocket(sock);
        }
    }
}
