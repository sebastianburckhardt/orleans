using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using Orleans.Counters;

using Orleans.Messaging;
using System.Collections.Concurrent;

namespace Orleans.Runtime.Messaging
{
    internal class ClientState
    {
        internal Queue<Message> PendingToSend { get; private set; }
        internal Queue<List<Message>> PendingBatchesToSend { get; private set; }
        internal Socket Sock { get; private set; }
        internal DateTime DisconnectedSince { get; set; }
        internal Guid Id { get; set; }
        internal int GWSenderNumber { get; private set; }

        internal bool IsConnected { get { return Sock != null; } }

        internal ClientState(Guid id, int gWSenderNumber)
        {
            Id = id;
            GWSenderNumber = gWSenderNumber;
            PendingToSend = new Queue<Message>();
            PendingBatchesToSend = new Queue<List<Message>>();
        }

        internal void RecordDisconnection()
        {
            if (Sock != null)
            {
                DisconnectedSince = DateTime.UtcNow;
                Sock = null;
                NetworkingStatisticsGroup.OnClosedGWDuplexSocket();
            }
        }

        internal void RecordConnection(Socket sock)
        {
            Sock = sock;
            DisconnectedSince = DateTime.MaxValue;
        }

        internal bool ReadyToDrop()
        {
            return !IsConnected &&
                   (DateTime.UtcNow.Subtract(DisconnectedSince) >= Gateway.TIME_BEFORE_CLIENT_DROP);
        }
    }

    internal class GatewaySender : AsynchQueueAgent<OutgoingClientMessage>
    {
        private readonly Gateway gateway;
        private readonly CounterStatistic gatewaySends;

        internal GatewaySender(string name, Gateway gw)
            : base(name, gw.MessagingConfiguration)
        {
            gateway = gw;
            gatewaySends = CounterStatistic.FindOrCreate(StatNames.STAT_GATEWAY_SENT);
            onFault = FaultBehavior.RestartOnFault;
        }

        protected override void Process(OutgoingClientMessage request)
        {
            if (cts.IsCancellationRequested)
            {
                return;
            }

            var client = request.Item1;
            var msg = request.Item2;

            // Find the client state
            ClientState clientState;
            bool found;
            lock (gateway.lockable)
            {
                found = gateway.clients.TryGetValue(client, out clientState);
            }

            // This should never happen -- but make sure to handle it reasonably, just in case
            if (!found || (clientState == null))
            {
                if (msg == null)
                    return;
                log.Info(ErrorCode.GatewayTryingToSendToUnrecognizedClient, "Trying to send a message {0} to an unrecognized client {1}", msg.ToString(), client);
                MessagingStatisticsGroup.OnFailedSentMessage(msg);
                // Message for unrecognized client -- reject it
                if (msg.Direction == Message.Directions.Request)
                {
                    MessagingStatisticsGroup.OnRejectedMessage(msg);
                    Message error = msg.CreateRejectionResponse(Message.RejectionTypes.FutureTransient, "Unknown client " + client);
                    gateway.SendMessage(error);
                }
                else
                {
                    MessagingStatisticsGroup.OnDroppedSentMessage(msg);
                }
                return;
            }
            // if disconnected - queue for later.
            if (!clientState.IsConnected)
            {
                if (msg != null)
                {
                    if (log.IsVerbose3) log.Verbose3("Queued message {0} for client {1}", msg, client);
                    clientState.PendingToSend.Enqueue(msg);
                }
                return;
            }
            // if the queue is non empty - drain it first.
            if (clientState.PendingToSend.Count > 0)
            {
                if (msg != null)
                {
                    clientState.PendingToSend.Enqueue(msg);
                }
                // For now, drain in-line, although in the future this should happen in yet another asynch agent
                Drain(clientState);
                return;
            }
            // the queue was empty AND we are connected.

            // If the request includes a message to send, send it (or enqueue it for later)
            if (msg != null)
            {
                if (!Send(msg, clientState.Sock))
                {
                    if (log.IsVerbose3) log.Verbose3("Queued message {0} for client {1}", msg, client);
                    clientState.PendingToSend.Enqueue(msg);
                }
                else
                {
                    if (log.IsVerbose3) log.Verbose3("Sent message {0} to client {1}", msg, client);
                }
                return;
            }
        }

        protected override void ProcessBatch(List<OutgoingClientMessage> requests)
        {
            if (cts.IsCancellationRequested)
            {
                return;
            }

            if (requests == null || requests.Count == 0)
                return;

            // Every Tuple in requests are guaranteed to have the same client
            var client = requests[0].Item1;
            var msgs = requests.Where(r => r != null).Select(r => r.Item2).ToList();

            // Find the client state
            ClientState clientState;
            bool found;
            lock (gateway.lockable)
            {
                found = gateway.clients.TryGetValue(client, out clientState);
            }

            // This should never happen -- but make sure to handle it reasonably, just in case
            //if (!found || (clientState == null))
            //{
            //    if (msg == null)
            //        return;
            //    log.Info(ErrorCode.GatewayTryingToSendToUnrecognizedClient, "Trying to send a message {0} to an unrecognized client {1}", msg.ToString(), client);
            //    MessagingStatisticsGroup.OnFailedSentMessage(msg);
            //    // Message for unrecognized client -- reject it
            //    if (msg.Direction == Message.Directions.Request)
            //    {
            //            MessagingStatisticsGroup.OnRejectedMessage(msg);
            //            Message error = msg.CreateRejectionResponse(Message.RejectionTypes.FutureTransient, "Unknown client " + client);
            //            gateway.SendMessage(error);}
            //    }
            //    else
            //    {
            //        MessagingStatisticsGroup.OnDroppedSentMessage(msg);
            //    }
            //    return;
            //}
            // if disconnected - queue for later.
            if (!clientState.IsConnected)
            {
                if (msgs.Count != 0)
                {
                    if (log.IsVerbose3) log.Verbose3("Queued {0} messages for client {1}", msgs.Count, client);
                    clientState.PendingBatchesToSend.Enqueue(msgs);
                }
                return;
            }
            // if the queue is non empty - drain it first.
            if (clientState.PendingBatchesToSend.Count > 0)
            {
                if (msgs.Count != 0)
                {
                    clientState.PendingBatchesToSend.Enqueue(msgs);
                }
                // For now, drain in-line, although in the future this should happen in yet another asynch agent
                DrainBatch(clientState);
                return;
            }
            // the queue was empty AND we are connected.

            // If the request includes a message to send, send it (or enqueue it for later)
            if (msgs.Count != 0)
            {
                if (!SendBatch(msgs, clientState.Sock))
                {
                    if (log.IsVerbose3) log.Verbose3("Queued {0} messages for client {1}", msgs.Count, client);
                    clientState.PendingBatchesToSend.Enqueue(msgs);
                }
                else
                {
                    if (log.IsVerbose3) log.Verbose3("Sent {0} message to client {1}", msgs.Count, client);
                }
                return;
            }
        }

        private void Drain(ClientState clientState)
        {
            // For now, drain in-line, although in the future this should happen in yet another asynch agent
            while (clientState.PendingToSend.Count > 0)
            {
                var m = clientState.PendingToSend.Peek();
                if (Send(m, clientState.Sock))
                {
                    if (log.IsVerbose3) log.Verbose3("Sent queued message {0} to client {1}", m, clientState.Id);
                    clientState.PendingToSend.Dequeue();
                }
                else
                {
                    return;
                }
            }
        }

        private void DrainBatch(ClientState clientState)
        {
            // For now, drain in-line, although in the future this should happen in yet another asynch agent
            while (clientState.PendingBatchesToSend.Count > 0)
            {
                var m = clientState.PendingBatchesToSend.Peek();
                if (SendBatch(m, clientState.Sock))
                {
                    if (log.IsVerbose3) log.Verbose3("Sent {0} queued messages to client {1}", m.Count, clientState.Id);
                    clientState.PendingBatchesToSend.Dequeue();
                }
                else
                {
                    return;
                }
            }
        }


        private bool Send(Message msg, Socket sock)
        {
            if (cts.IsCancellationRequested)
            {
                return false;
            }
            if (sock == null)
            {
                return false;
            }

            // Send the message
            List<ArraySegment<byte>> data = null;
            int headerLength = 0;
            try
            {
                data = msg.Serialize(out headerLength);
            }
            catch (Exception exc)
            {
                OnMessageSerializationFailure(msg, exc);
                //throw;
                return true;
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
                    sendErrorStr = String.Format("Byte count mismatch on send: sent {0}, expected {1}", bytesSent, length);
                    log.Warn(ErrorCode.GatewayByteCountMismatch, sendErrorStr);
                }
            }
            catch (Exception exc)
            {
                exceptionSending = true;
                string remoteEndpoint = "";
                if (!(exc is ObjectDisposedException))
                {
                    remoteEndpoint = sock.RemoteEndPoint.ToString();
                }
                sendErrorStr = String.Format("Exception sending to client at {0}: {1}", remoteEndpoint, exc);
                log.Warn(ErrorCode.GatewayExceptionSendingToClient, sendErrorStr, exc);
            }
            MessagingStatisticsGroup.OnMessageSend(msg.TargetSilo, msg.Direction, bytesSent, headerLength, SocketDirection.GWToClient);
            bool sendError = exceptionSending || countMismatchSending;
            if (sendError)
            {
                gateway.RecordClosedSocket(sock);
                SocketManager.CloseSocket(sock);
            }
            gatewaySends.Increment();
            msg.ReleaseBodyAndHeaderBuffers();
            return !sendError;
        }
        
        private bool SendBatch(List<Message> msgs, Socket sock)
        {
            if (cts.IsCancellationRequested)
            {
                return false;
            }
            if (sock == null)
            {
                return false;
            }
            if (msgs == null || msgs.Count == 0)
            {
                return true;
            }
            // Send the message
            List<ArraySegment<byte>> data = null;
            int headerLengths = 0;
            bool continueSend = OutgoingMessageSender.SerializeMessages(msgs, out data, out headerLengths, OnMessageSerializationFailure);
            if (!continueSend)
                return false;

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
                    sendErrorStr = String.Format("Byte count mismatch on send: sent {0}, expected {1}", bytesSent, length);
                    log.Warn(ErrorCode.GatewayByteCountMismatch, sendErrorStr);
                }
            }
            catch (Exception exc)
            {
                exceptionSending = true;
                string remoteEndpoint = "";
                if (!(exc is ObjectDisposedException))
                {
                    remoteEndpoint = sock.RemoteEndPoint.ToString();
                }
                sendErrorStr = String.Format("Exception sending to client at {0}: {1}", remoteEndpoint, exc);
                log.Warn(ErrorCode.GatewayExceptionSendingToClient, sendErrorStr, exc);
            }
            MessagingStatisticsGroup.OnMessageBatchSend(msgs[0].TargetSilo, msgs[0].Direction, bytesSent, headerLengths, SocketDirection.GWToClient, msgs.Count);
            bool sendError = exceptionSending || countMismatchSending;
            if (sendError)
            {
                gateway.RecordClosedSocket(sock);
                SocketManager.CloseSocket(sock);
            }
            gatewaySends.Increment();
            foreach (Message msg in msgs)
            {
                msg.ReleaseBodyAndHeaderBuffers();
            }
            return !sendError;
        }

        private void OnMessageSerializationFailure(Message wmsg, Exception exc)
        {
            // we only get here if we failed to serialize the msg (or any other catastrophic failure).
            // Request msg fails to serialize on the sending silo, so we just enqueue a rejection msg.
            // Response msg fails to serialize on the responding silo, so we try to send an error response back.
            log.Warn(ErrorCode.Messaging_Gateway_SerializationError, String.Format("Unexpected error serializing message {0} on the gateway", wmsg.ToString()), exc);
            wmsg.ReleaseBodyAndHeaderBuffers();
            MessagingStatisticsGroup.OnFailedSentMessage(wmsg);
            MessagingStatisticsGroup.OnDroppedSentMessage(wmsg);
        }
    }

    internal class GatewayClientCleanupAgent : AsynchAgent
    {
        private readonly Gateway gateway;

        internal GatewayClientCleanupAgent(Gateway gw)
        {
            gateway = gw;
        }

        #region Overrides of AsynchAgent

        protected override void Run()
        {
            while (!cts.IsCancellationRequested)
            {
                gateway.DropDisconnectedClients();
                gateway.DropExpiredRoutingCachedEntries();
                Thread.Sleep(Gateway.TIME_BEFORE_CLIENT_DROP);
            }
        }

        #endregion
    }

    // this cache is used to record the addresses of GWs from which clients connected to.
    // it is used to route replies to clients from client addressable objects
    // without this cache this GW will not know how to route the reply back to the client 
    // (since clients are not registered in the directory and this GW may not be proxying for the client for whom the reply is destined).
    internal class ClientsReplyRoutingCache
    {
        // for every client: the GW to use to route repies back to it plus the last time that client connected via this GW.
        private readonly ConcurrentDictionary<GrainId, Tuple<SiloAddress, DateTime>> clientRoutes;
        private readonly TimeSpan TIME_BEFORE_ROUTE_CACHED_ENTRY_EXPIRES;

        internal ClientsReplyRoutingCache(IMessagingConfiguration messagingConfiguration)
        {
            clientRoutes = new ConcurrentDictionary<GrainId, Tuple<SiloAddress, DateTime>>();
            TIME_BEFORE_ROUTE_CACHED_ENTRY_EXPIRES = messagingConfiguration.ResponseTimeout.Multiply(5);
        }

        internal void RecordClientRoute(GrainId client, SiloAddress gateway)
        {
            DateTime now = DateTime.UtcNow;
            clientRoutes.AddOrUpdate(client, new Tuple<SiloAddress, DateTime>(gateway, now), (k, v) => new Tuple<SiloAddress, DateTime>(gateway, now));
        }

        internal bool TryFindClientRoute(GrainId client, out SiloAddress gateway)
        {
            gateway = null;
            Tuple<SiloAddress, DateTime> tuple;
            bool ret = clientRoutes.TryGetValue(client, out tuple);
            if (ret)
            {
                gateway = tuple.Item1;
            }
            return ret;
        }

        internal void DropExpiredEntries()
        {
            List<GrainId> clientsToDrop = clientRoutes.Where(route => Expired(route.Value.Item2)).Select(kv => kv.Key).ToList();
            foreach (GrainId client in clientsToDrop)
            {
                Tuple<SiloAddress, DateTime> tuple;
                clientRoutes.TryRemove(client, out tuple);
            }
        }

        private bool Expired(DateTime lastUsed)
        {
            return DateTime.UtcNow.Subtract(lastUsed) >= TIME_BEFORE_ROUTE_CACHED_ENTRY_EXPIRES;
        }
    }

    internal class Gateway
    {
        internal static readonly TimeSpan TIME_BEFORE_CLIENT_DROP = TimeSpan.FromSeconds(60);

        private readonly MessageCenter mc;
        private readonly GatewayAcceptor acceptor;
        private readonly Lazy<GatewaySender>[] senders;
        private readonly GatewayClientCleanupAgent dropper;

        // clients is the main authorative collection of all connected clients. 
        // Any client currently in the system appears in this collection. 
        // In addition, we use clientSockets and proxiedGrains collections for fast retrival of ClientState. 
        // Anything that appears in those 2 collections should also appear in the main clients collection.
        internal readonly ConcurrentDictionary<Guid, ClientState> clients;
        private readonly ConcurrentDictionary<Socket, ClientState> clientSockets;
        private readonly ConcurrentDictionary<GrainId, ClientState> proxiedGrains;
        private readonly SiloAddress gwAddress;
        internal IMessagingConfiguration MessagingConfiguration { get { return mc.MessagingConfiguration; } }
        private int nextGWSenderToUseForRoundRobin;
        private readonly ClientsReplyRoutingCache clientsReplyRoutingCache;

        internal readonly object lockable;
        private static Logger logger = Logger.GetLogger("Orleans.Messaging.Gateway");

        internal Gateway(MessageCenter msgCtr, IPEndPoint gatewayAddress)
        {
            mc = msgCtr;
            acceptor = new GatewayAcceptor(msgCtr, this, gatewayAddress);
            senders = new Lazy<GatewaySender>[mc.MessagingConfiguration.GatewaySenderQueues];
            nextGWSenderToUseForRoundRobin = 0;
            dropper = new GatewayClientCleanupAgent(this);
            clients = new ConcurrentDictionary<Guid, ClientState>();
            clientSockets = new ConcurrentDictionary<Socket, ClientState>();
            proxiedGrains = new ConcurrentDictionary<GrainId, ClientState>();
            clientsReplyRoutingCache = new ClientsReplyRoutingCache(mc.MessagingConfiguration);
            gwAddress = SiloAddress.New(gatewayAddress, 0);
            lockable = new object();
        }

        internal void Start()
        {
            acceptor.Start();
            for (int i = 0; i < senders.Length; i++)
            {
                int capture = i;
                senders[capture] = new Lazy<GatewaySender>(() =>
                {
                    var sender = new GatewaySender("GatewaySiloSender_" + capture, this);
                    sender.Start();
                    return sender;
                }, LazyThreadSafetyMode.ExecutionAndPublication);
            }
            dropper.Start();
        }

        internal void Stop()
        {
            dropper.Stop();
            foreach (var sender in senders)
            {
                if (sender != null && sender.IsValueCreated)
                {
                    sender.Value.Stop();
                }
            }
            acceptor.Stop();
        }

        internal void RecordOpenedSocket(Socket sock, Guid clientId)
        {
            lock (lockable)
            {
                logger.Info(ErrorCode.GatewayClientOpenedSocket, "Recorded opened socket from endpoint {0}, client ID {1}.", sock.RemoteEndPoint, clientId);
                ClientState clientState;
                if (clients.TryGetValue(clientId, out clientState))
                {
                    Socket oldSocket = clientState.Sock;
                    if (oldSocket != null)
                    {
                        // The old socket will be closed by itself later.
                        ClientState ignore;
                        clientSockets.TryRemove(oldSocket, out ignore);
                    }
                    clientState.RecordConnection(sock);
                    QueueRequest(clientState, null);
                }
                else
                {
                    int gwToUse = nextGWSenderToUseForRoundRobin % senders.Length;
                    nextGWSenderToUseForRoundRobin++; // under GW lock
                    clientState = new ClientState(clientId, gwToUse);
                    clients[clientId] = clientState;
                    clientState.RecordConnection(sock);
                    MessagingStatisticsGroup.ConnectedClientCount.Increment();
                }
                clientSockets[sock] = clientState;
                NetworkingStatisticsGroup.OnOpenedGWDuplexSocket();
            }
        }

        internal void RecordClosedSocket(Socket sock)
        {
            if (sock == null) return;
            lock (lockable)
            {
                ClientState cs = null;
                if (clientSockets.TryGetValue(sock, out cs))
                {
                    EndPoint endPoint = null;
                    try
                    {
                        endPoint = sock.RemoteEndPoint;
                    }
                    catch (Exception) { } // guard against ObjectDisposedExceptions
                    logger.Info(ErrorCode.GatewayClientClosedSocket, "Recorded closed socket from endpoint {0}, client ID {1}.", endPoint != null ? endPoint.ToString() : "null", cs.Id);

                    ClientState ignore;
                    clientSockets.TryRemove(sock, out ignore);
                    cs.RecordDisconnection();
                }
            }
        }

        internal void RecordProxiedGrain(GrainId id, Guid client)
        {
            lock (lockable)
            {
                ClientState cs;
                if (clients.TryGetValue(client, out cs))
                {
                    // TO DO done: what if we have an older proxiedGrain for this client?
                    // We now support many proxied grains per client, so there's no need to handle it specially here. -- ageller 6/21/2012
                    proxiedGrains.AddOrUpdate(id, cs, (k, v) => cs);
                }
            }
        }

        internal void RecordSendingProxiedGrain(GrainId senderGrainId, Socket clientSocket)
        {
            // not taking global lock on the crytical path!
            ClientState cs;
            if (clientSockets.TryGetValue(clientSocket, out cs))
            {
                // TO DO done: what if we have an older proxiedGrain for this client?
                // We now support many proxied grains per client, so there's no need to handle it specially here. -- ageller 6/21/2012
                proxiedGrains.AddOrUpdate(senderGrainId, cs, (k, v) => cs);
            }
        }

        internal SiloAddress TryToReroute(Message msg)
        {
            // for responses from ClientAddressableObject to ClientGrain try to use clientsReplyRoutingCache for sending replies directly back.
            if (msg.SendingGrain.IsClientAddressableObject && msg.TargetGrain.IsClientGrain)
            {
                if (msg.Direction == Message.Directions.Response)
                {
                    SiloAddress gateway = null;
                    if (clientsReplyRoutingCache.TryFindClientRoute(msg.TargetGrain, out gateway))
                    {
                        return gateway; // do not reroute, we have specified the silo address for this msg.
                    }
                }
            }
            return null;
        }

        internal void RecordUnproxiedGrain(GrainId id)
        {
            lock (lockable)
            {
                ClientState ignore;
                proxiedGrains.TryRemove(id, out ignore);
            }
        }

        internal void DropDisconnectedClients()
        {
            lock (lockable)
            {
                List<ClientState> clientsToDrop = clients.Values.Where(cs => cs.ReadyToDrop()).ToList();
                foreach (ClientState client in clientsToDrop)
                {    
                    DropClient(client);
                }
            }
        }

        internal void DropExpiredRoutingCachedEntries()
        {
            lock (lockable)
            {
                clientsReplyRoutingCache.DropExpiredEntries();
            }
        }
        

        // This function is run under global lock
        // There is NO need to acquire individual ClientState lock, since we only access client Id (immutable) and close an older socket.
        private void DropClient(ClientState client)
        {
            logger.Info(ErrorCode.GatewayDroppingClient, "Dropping client {0}, {1} after disconnect with no reconnect", 
                            client.Id, DateTime.UtcNow.Subtract(client.DisconnectedSince));

            ClientState ignore;
            clients.TryRemove(client.Id, out ignore);

            Socket oldSocket = client.Sock;
            if (oldSocket != null)
            {
                // this will not happen, since we drop only already disconnected clients, for socket is already null. But leave this code just to be sure.
                client.RecordDisconnection();
                clientSockets.TryRemove(oldSocket, out ignore);
                SocketManager.CloseSocket(oldSocket);
            }
            List<GrainId> proxies = proxiedGrains.Where((KeyValuePair<GrainId, ClientState> pair) => pair.Value.Id.Equals(client.Id)).Select(p => p.Key).ToList();
            foreach (GrainId proxy in proxies)
            {
                proxiedGrains.TryRemove(proxy, out ignore);
            }
            MessagingStatisticsGroup.ConnectedClientCount.DecrementBy(1);
            mc.RecordClientDrop(proxies);
        }

        /// <summary>
        /// See if this message is intended for a grain we're proxying, and queue it for delivery if so.
        /// </summary>
        /// <param name="msg"></param>
        /// <returns>true if the message should be delivered to a proxied grain, false if not.</returns>
        internal bool TryDeliverToProxy(Message msg)
        {
            // See if it's a grain we're proxying.
            ClientState client;
            
            // not taking global lock on the crytical path!
            if (!proxiedGrains.TryGetValue(msg.TargetGrain, out client))
            {
                return false;
            }
            if (!clients.ContainsKey(client.Id))
            {
                lock (lockable)
                {
                    if (!clients.ContainsKey(client.Id))
                    {
                        ClientState ignore;
                        // Lazy clean-up for dropped clients
                        proxiedGrains.TryRemove(msg.TargetGrain, out ignore);
                        // GK: I don't think this can ever happen. When we drop the client (the only place we remove the ClientState from clients collection)
                        // we also actively remove all proxiedGrains for this client. So the clean-up will be non lazy, right?
                        // Alan: I think you're right, this "if" can probably be dropped.
                        // GK: leaving it for now.
                        return false;
                    }
                }
            }

            // when this GW receives a message from client X to client addressale object Y
            // it needs to record the original GW address through which this message came from (the address of the GW that X is connected to)
            // it will use this GW to re-route the REPLY from Y back to X.
            if (msg.SendingGrain.IsClientGrain && msg.TargetGrain.IsClientAddressableObject)
            {
                clientsReplyRoutingCache.RecordClientRoute(msg.SendingGrain, msg.SendingSilo);
            }
            
            msg.TargetSilo = null;
            msg.PlacementStrategy = null;
            msg.SendingSilo = gwAddress; // This makes sure we don't expose wrong silo addresses to the client. Client will only see silo address of the GW it is connected to.
            QueueRequest(client, msg);
            return true;
        }

        private void QueueRequest(ClientState clientState, Message msg)
        {
            //int index = senders.Length == 1 ? 0 : Math.Abs(clientId.GetHashCode()) % senders.Length;
            int index = clientState.GWSenderNumber;
            senders[index].Value.QueueRequest(new OutgoingClientMessage(clientState.Id, msg));   
        }

        internal void SendMessage(Message msg)
        {
            mc.SendMessage(msg);
        }
    }
}
