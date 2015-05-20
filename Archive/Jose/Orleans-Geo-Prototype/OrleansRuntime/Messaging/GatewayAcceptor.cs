using System;
using System.Net;
using System.Net.Sockets;
using Orleans.Counters;

using Orleans.Messaging;

namespace Orleans.Runtime.Messaging
{
    internal class GatewayAcceptor : IncomingMessageAcceptor
    {
        private readonly Gateway gateway;
        private readonly CounterStatistic loadSheddingCounter;
        private readonly CounterStatistic gatewayTrafficCounter;

        internal GatewayAcceptor(MessageCenter msgCtr, Gateway gw, IPEndPoint gatewayAddress)
            : base(msgCtr, gatewayAddress, SocketDirection.GWToClient)
        {
            gateway = gw;
            loadSheddingCounter = CounterStatistic.FindOrCreate(StatNames.STAT_GATEWAY_LOAD_SHEDDING);
            gatewayTrafficCounter = CounterStatistic.FindOrCreate(StatNames.STAT_GATEWAY_RECEIVED);
        }
        
        protected override bool RecordOpenedSocket(Socket sock)
        {
            ThreadTrackingStatistic.FirstClientConnectedStartTracking();
            Guid client;
            if (ReceiveSocketPreample(sock, true, out client))
            {
                gateway.RecordOpenedSocket(sock, client);
                return true;
            }
            else
            {
                return false;
            }
        }
  
        // Always called under a lock
        protected override void RecordClosedSocket(Socket sock)
        {
            base.TryRemoveClosedSocket(sock); // don't count this closed socket in IMA, we count it in GW.
            gateway.RecordClosedSocket(sock);
        }

        /// <summary>
        /// Handles an incoming (proxied) message by rerouting it immediately and unconditionally,
        /// after some header massaging.
        /// </summary>
        /// <param name="wmsg"></param>
        /// <param name="receivedOnSocket"></param>
        protected override void HandleMessage(Message wmsg, Socket receivedOnSocket)
        {
            // Don't process messages that have already timed out
            if (wmsg.IsExpired)
            {
                wmsg.DropExpiredMessage(MessagingStatisticsGroup.Phase.Receive);
                return;
            }

            wmsg.AddTimestamp(Message.LifecycleTag.ReceiveIncoming);

            gatewayTrafficCounter.Increment();

            // Are we overloaded?
            if ((mc.Metrics != null) && mc.Metrics.IsOverloaded)
            {
                MessagingStatisticsGroup.OnRejectedMessage(wmsg);
                Message rejection = wmsg.CreateRejectionResponse(Message.RejectionTypes.GatewayTooBusy, "Shedding load");
                gateway.TryDeliverToProxy(rejection);
                if (log.IsVerbose) log.Verbose("Rejecting a request due to overloading: {0}", wmsg.ToString());
                //log.Info("Rejecting a request due to overloading: {0}", wmsg.ToString());
                loadSheddingCounter.Increment();
                return;
            }

            gateway.RecordSendingProxiedGrain(wmsg.SendingGrain, receivedOnSocket);
            SiloAddress targetAddress = gateway.TryToReroute(wmsg);
            wmsg.SendingSilo = mc.MyAddress;

            if (targetAddress == null)
            {
                // reroute via Dispatcher
                wmsg.RemoveHeader(Message.Header.TargetSilo);
                wmsg.RemoveHeader(Message.Header.TargetActivation);

                if (wmsg.TargetGrain.IsSystemTarget)
                {
                    wmsg.TargetSilo = mc.MyAddress;
                    wmsg.TargetActivation = ActivationId.GetSystemActivation(wmsg.TargetGrain, mc.MyAddress);
                }

                wmsg.AddTimestamp(Message.LifecycleTag.RerouteIncoming);
                MessagingStatisticsGroup.OnMessageReRoute(wmsg);
                mc.RerouteMessage(wmsg);
            }
            else
            {
                // send directly
                wmsg.TargetSilo = targetAddress;
                mc.SendMessage(wmsg);
            }
            return;
        }
    }
}
