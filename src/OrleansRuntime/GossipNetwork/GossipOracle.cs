using Orleans.Runtime.Configuration;
using Orleans.Runtime.Messaging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime.GossipNetwork
{
    internal class GossipOracle : SystemTarget, IGossipOracle
    {

        private readonly List<IGossipChannel> gossipChannels;

        private GatewayEntry lastinjected; // last local status injected to gossip network

        private readonly GossipOracleData gossipOracleData;
        private readonly TraceLogger logger;
        
        private GrainTimer timer;

        private string globalServiceId;

        //TODO : make these global parameters
        static TimeSpan ResendActiveStatusAfter = new TimeSpan(hours: 0, minutes: 10, seconds: 0);
        static TimeSpan TimerInterval = new TimeSpan(hours: 0, minutes: 0, seconds: 10);  

        public GossipOracle(SiloAddress silo, List<IGossipChannel> sources, GlobalConfiguration config)
            : base(Constants.GossipOracleId, silo)
        {
            Debug.Assert(sources != null && sources.Count > 0);
            logger = TraceLogger.GetLogger("GossipOracle");
            gossipChannels = sources;
            gossipOracleData = new GossipOracleData(logger);
            globalServiceId = config.GlobalServiceId; 
        }


        public bool IsFunctionalClusterGateway(SiloAddress siloAddress)
        {
            GatewayEntry g;
            return gossipOracleData.Current.Gateways.TryGetValue(siloAddress, out g) 
                && g.Status == GatewayStatus.Active;
        }

        public IEnumerable<string> GetActiveClusters()
        {
            var clusters = gossipOracleData.Current.Gateways.Values
                 .Where(g => g.Status == GatewayStatus.Active)
                 .Select(g => g.SiloAddress.ClusterId)
                 .Distinct();

            return clusters;
        }


        public SiloAddress GetClusterGateway(string cluster)
        {
            //TraceLogger.GetLogger("CMOD").Info("local data {0}", localTable);
            var gateways = gossipOracleData.Current.Gateways.Values
                .Where(g => g.SiloAddress.ClusterId.Equals(cluster) && g.Status == GatewayStatus.Active)
                .Select(g => g.SiloAddress);

            return gateways.FirstOrDefault();
        }


        public MultiClusterConfiguration GetMultiClusterConfiguration()
        {
            return gossipOracleData.Current.Configuration;
        }


        public bool SubscribeToGossipEvents(IGossipListener observer)
        {
            return gossipOracleData.SubscribeToGossipEvents(observer);
        }

        public bool UnSubscribeFromGossipEvents(IGossipListener observer)
        {
            return gossipOracleData.UnSubscribeFromGossipEvents(observer);
        }
    

        public Task Start()
        {
            logger.Info(ErrorCode.Gossip_Starting, "GossipOracle starting on {0} ", Silo);
            try
            {
                StartTimer();
            }
            catch (Exception exc)
            {
                logger.Error(ErrorCode.Gossip_FailedToStart, "GossipOracle failed to start {0}", exc);
                throw;
            }

            return TaskDone.Done;
        }

        private void StartTimer()
        {
            if (timer != null)
                timer.Dispose();

            timer = GrainTimer.FromTaskCallback(
                OnGetTimer, null, TimeSpan.Zero, TimerInterval, "Gossip.GatewayTimer");

            timer.Start();
        }

        private async Task OnGetTimer(object data)
        {
            if (logger.IsVerbose2)
                logger.Verbose2("-heartbeat");

            timer.CheckTimerDelay();

            InjectLocalStatus();

            await Gossip();
        }

        private async Task Gossip()
        {
            // gossip with sources
            var gossiptasks = gossipChannels.Select(s => GossipWith(s));

            await Task.WhenAll(gossiptasks);

            if (gossiptasks.All(t => ! t.Result))
                logger.Error(ErrorCode.Gossip_CommunicationFailure, "All Gossip channels failed");
        }


        private async Task<bool> GossipWith(IGossipChannel gossipsource)
        {
            GossipData answer = null;
            try
            {
                answer = await gossipsource.PushAndPull(gossipOracleData.Current);
            }
            catch (Exception exc)
            {
                logger.Error(ErrorCode.Gossip_CommunicationFailure, "Gossip channel failed", exc);
                return false;
            }

            // apply what we have learnt
            gossipOracleData.ApplyGossipDataAndNotify(answer);

            return true;
        }


        private bool InjectLocalStatus()
        {
            var currentstatus = DetermineLocalStatus();

            // send if status is changed, or we are active and haven't said so in a while
            if (lastinjected == null
                || currentstatus.Status != lastinjected.Status
                || (currentstatus.Status == GatewayStatus.Active 
                      && currentstatus.HeartbeatTimestamp - lastinjected.HeartbeatTimestamp > ResendActiveStatusAfter))
            {
                // update current gossip data with status
                gossipOracleData.ApplyGossipDataAndNotify(new GossipData(currentstatus));

                lastinjected = currentstatus;

                return true;
            }

            return false;
        }

        private GatewayEntry DetermineLocalStatus()
        {
            //TODO determine actual gateway status
            // for now we just assume EVERYBODY is an active gateway

            return new GatewayEntry()
            {
                SiloAddress = Silo,
                Status = GatewayStatus.Active,
                HeartbeatTimestamp = DateTime.UtcNow
            };
        }




    }
}
