using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Orleans.Counters;

using Orleans;
using Orleans.Messaging;

namespace Orleans.Runtime.Messaging
{
    internal class MessageCenter : ISiloMessageCenter, IDisposable, ISiloShutdownParticipant
    {
        internal IOutboundMessageQueue OutboundQueue { get; set; }
        internal IInboundMessageQueue InboundQueue { get; set; }
        internal Gateway Gateway { get; set; }
        internal ClusterConfiguration ClusterConfig { get; private set;  }
        internal ILocalGrainDirectory LocalGrainDirectory { get; private set; }

        public bool IsProxying { get { return Gateway != null; } }

        public bool TryDeliverToProxy(Message msg)
        {
            return Gateway != null && Gateway.TryDeliverToProxy(msg);
        }

        internal bool IsBlockingApplicationMessages { get; private set; }

        internal SocketManager socketManager;
        //private OutgoingMessageSender oms;
        private IncomingMessageAcceptor ima;

        private static Logger log = Logger.GetLogger("Orleans.Messaging.MessageCenter");

        private Action<Message> rerouteHandler;
        internal delegate Task DirectoryCacheFlushHandler(ActivationAddress addr);
        internal DirectoryCacheFlushHandler directoryCacheFlushHandler;
        private Action<List<GrainId>> clientDropHandler;

        internal ISiloPerformanceMetrics Metrics { get; private set; }

        // ReSharper disable UnaccessedField.Local
        private IntValueStatistic sendQueueLengthCounter;
        private IntValueStatistic receiveQueueLengthCounter;
        // ReSharper restore UnaccessedField.Local

        // This is determined by the IMA but needed by the OMS, and so is kept here in the message center itself.
        public SiloAddress MyAddress { get; private set; }

        public IMessagingConfiguration MessagingConfiguration { get; private set; }

        public MessageCenter(IPEndPoint here, int generation, IMessagingConfiguration config, ClusterConfiguration clusterConfig, ISiloPerformanceMetrics metrics = null)
        {
            Initialize(here, generation, config, clusterConfig, metrics);
        }

        private void Initialize(IPEndPoint here, int generation, IMessagingConfiguration config, ClusterConfiguration clusterConfig, 
            ISiloPerformanceMetrics metrics = null)           
        {
            if(log.IsVerbose3) log.Verbose3("Starting initialization.");
            
            socketManager = new SocketManager(config);
            ima = new IncomingMessageAcceptor(this, here, SocketDirection.SiloToSilo);
            MyAddress = SiloAddress.New((IPEndPoint) ima.acceptingSocket.LocalEndPoint, generation, config.ClusterId);
            ClusterConfig = clusterConfig;
            ClusterConfig.LocalHash = MyAddress.GetConsistentHashCode();
            MessagingConfiguration = config;
            InboundQueue = new InboundMessageQueue();
            OutboundQueue = new OutboundMessageQueue(this, config, clusterConfig);
            Gateway = null;
            Metrics = metrics;
            
            sendQueueLengthCounter = IntValueStatistic.FindOrCreate(StatNames.STAT_MESSAGE_CENTER_SEND_QUEUE_LENGTH, () => SendQueueLength);
            receiveQueueLengthCounter = IntValueStatistic.FindOrCreate(StatNames.STAT_MESSAGE_CENTER_RECEIVE_QUEUE_LENGTH, () => ReceiveQueueLength);

            if (log.IsVerbose3) log.Verbose3("Completed initialization.");
        }

        

        public void InstallLocalGrainDirectory(ILocalGrainDirectory localGrainDirectory)
        {
            this.LocalGrainDirectory = localGrainDirectory;
        }

        public void InstallGateway(IPEndPoint gatewayAddress)
        {
            Gateway = new Gateway(this, gatewayAddress);
        }

        public void RecordProxiedGrain(GrainId id, Guid client)
        {
            if (Gateway != null)
            {
                Gateway.RecordProxiedGrain(id, client);
            }
        }

        public void RecordUnproxiedGrain(GrainId id)
        {
            if (Gateway != null)
            {
                Gateway.RecordUnproxiedGrain(id);
            }
        }

        public void Start()
        {
            //Router.Start();
            IsBlockingApplicationMessages = false;
            ima.Start();
            OutboundQueue.Start();
        }

        public void StartGateway()
        {
            if (Gateway != null)
            {
                Gateway.Start();
            }            
        }

        public void PrepareToStop()
        {
        }

        public void Stop()
        {
            IsBlockingApplicationMessages = true;

            try
            {
                ima.Stop();
            }
            catch (Exception exc) { log.Error(ErrorCode.Runtime_Error_100108, "Stop failed.", exc); }

            StopAcceptingClientMessages();

            try
            {
                OutboundQueue.Stop();
            }
            catch (Exception exc) { log.Error(ErrorCode.Runtime_Error_100110, "Stop failed.", exc); }

            try
            {
                socketManager.Stop();
            }
            catch (Exception exc) { log.Error(ErrorCode.Runtime_Error_100111, "Stop failed.", exc); }
            //Router.Stop();
        }

        public void StopAcceptingClientMessages()
        {
            if (log.IsVerbose) log.Verbose("StopClientMessages");
            if (Gateway != null)
            {
                try
                {
                    Gateway.Stop();
                }
                catch (Exception exc) { log.Error(ErrorCode.Runtime_Error_100109, "Stop failed.", exc); }
                Gateway = null;
            }
        }

        public Action<Message> RerouteHandler
        {
            set
            {
                if (rerouteHandler != null)
                    throw new InvalidOperationException("MessageCenter RerouteHandler already set");
                rerouteHandler = value;
            }
        }

        public void RerouteMessage(Message message)
        {
            if (rerouteHandler != null)
                rerouteHandler(message);
            else
                SendMessage(message);
        }

        public Action<Message> SniffIncomingMessage
        {
            set
            {
                ima.SniffIncomingMessage = value;
            }
        }

        public Func<SiloAddress, bool> SiloDeadOracle { get; set; }

        public void SendMessage(Message msg)
        {
            // Note that if we identify or add other grains that are required for proper stopping, we will need to treat them as we do the membership table grain here.
            if (IsBlockingApplicationMessages && (msg.Category == Message.Categories.Application) && (msg.Result != Message.ResponseTypes.Rejection)
                && !Constants.SystemMembershipTableId.Equals(msg.TargetGrain))
            {
                // Drop the message on the floor if it's an application message that isn't a rejection
            }
            else
            {
                // The underlying assumption here is that we know which cluster a grain resides in apriori. We can identify a 
                // grain's cluster based on its primary key. This assumption is hard-coded in the lines below. How can we dynamically
                // allow grains to belong to any cluster? 
                // 
                // To allow a grain to belong to any cluster, we need to modify the directory protocol
                if (msg.SendingSilo == null)
                    msg.SendingSilo = MyAddress;

                // Hack: Route a request to a cluster based on the primary key.
                /*
                if (msg.TargetGrain.Category == UniqueKey.Category.Grain)
                {
                    long primary_key = msg.TargetGrain.GetPrimaryKeyLong();
                    long datacenterID = primary_key % ClusterConfig.NumClusters;
                    if (datacenterID != MyAddress.ClusterId)
                    {
                        msg.TargetSilo = SiloAddress.NewClusterRef((int)datacenterID);
                    }
                }
                 */
                OutboundQueue.SendMessage(msg).Ignore();
            }
        }

        public Action<List<GrainId>> ClientDropHandler
        {
            set
            {
                if (clientDropHandler != null)
                {
                    throw new InvalidOperationException("MessageCenter ClientDropHandler already set");
                }
                clientDropHandler = value;
            }
        }

        internal void RecordClientDrop(List<GrainId> client)
        {
            if (clientDropHandler != null && client != null)
            {
                clientDropHandler(client);
            }
        }

        internal void SendRejection(Message msg, Message.RejectionTypes rejectionType, string reason)
        {
            MessagingStatisticsGroup.OnRejectedMessage(msg);
            if (string.IsNullOrEmpty(reason)) reason = String.Format("Rejection from silo {0} - Unknown reason.", MyAddress);
            Message error = msg.CreateRejectionResponse(rejectionType, reason);
            // rejection msgs are always originated in the local silo, they are never remote.
            InboundQueue.PostMessage(error);
        }

        public Message WaitMessage(Message.Categories type, CancellationToken ct)
        {
            return InboundQueue.WaitMessage(type);
        }

        public void Dispose()
        {
            if (ima != null)
            {
                ima.Dispose();
                ima = null;
            }

            OutboundQueue.Dispose();

            GC.SuppressFinalize(this);
        }

        public int SendQueueLength { get { return OutboundQueue.Count; } }

        public int ReceiveQueueLength { get { return InboundQueue.Count; } }

        /// <summary>
        /// Indicates that application messages should be blocked from being sent or received.
        /// This method is used by the "fast stop" process.
        /// <para>
        /// Specifically, all outbound application messages are dropped, except for rejections and messages to the membership table grain.
        /// Inbound application requests are rejected, and other inbound application messages are dropped.
        /// </para>
        /// </summary>
        public void BlockApplicationMessages()
        {
            if(log.IsVerbose) log.Verbose("BlockApplicationMessages");
            IsBlockingApplicationMessages = true;
        }

        public void SwitchClusterMessaging(bool val)
        {
            ((OutboundMessageQueue)this.OutboundQueue).SwitchClusterMessaging(val);
        }

        #region Implementation of ISiloShutdownParticipant

        public void BeginShutdown(Action tryFinishShutdown)
        {
            // nothing
        }

        public bool CanFinishShutdown()
        {
            // todo: review - should also consider unresolved responses?
            if (log.IsVerbose) log.Verbose("CanFinishShutdown {0} {1}", InboundQueue.Count, OutboundQueue.Count);
            return InboundQueue.Count == 0 && OutboundQueue.Count == 0;
        }

        public void FinishShutdown()
        {
            Stop();
        }

        public SiloShutdownPhase Phase { get { return SiloShutdownPhase.Messaging; } }

        #endregion
    }

    /// <summary>
    /// For internal use only
    /// Used for controlling message delvery when UseChessScheduling/Messaging=true
    /// </summary>
    internal interface IOutboundMessageQueue : IDisposable
    {
        /// <summary>
        /// Start operation
        /// </summary>
        void Start();

        /// <summary>
        /// Stop operation
        /// </summary>
        void Stop();

        /// <summary>
        /// For internal use only
        /// </summary>
        /// <param name="message"></param>
        Task<bool> SendMessage(Message message);

        /// <summary>
        /// Current queue length
        /// </summary>
        int Count { get; }
    }
}
