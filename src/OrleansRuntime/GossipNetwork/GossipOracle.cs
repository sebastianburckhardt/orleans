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
    internal class GossipOracle : SystemTarget, IGossipOracle, ISiloStatusListener
    {

        private readonly List<IGossipChannel> gossipChannels;

        private readonly GossipOracleData gossipOracleData;
        private readonly TraceLogger logger;
        
        private GrainTimer timer;
        DateTime lastrefresh;

        private ISiloStatusOracle silostatusoracle;

        private string globalServiceId;

        BackgroundWorker worker;
 

        public GossipOracle(SiloAddress silo, List<IGossipChannel> sources, GlobalConfiguration config)
            : base(Constants.GossipOracleId, silo)
        {
            Debug.Assert(sources != null && sources.Count > 0);
            logger = TraceLogger.GetLogger("GossipOracle");
            gossipChannels = sources;
            gossipOracleData = new GossipOracleData(logger);
            globalServiceId = config.GlobalServiceId;
            worker = new BackgroundWorker(() => Work()); 
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

        Random randomgenerator = new Random();

        public SiloAddress GetRandomClusterGateway(string cluster)
        {
            //TraceLogger.GetLogger("CMOD").Info("local data {0}", localTable);
            var gateways = gossipOracleData.Current.Gateways.Values
                .Where(g => g.SiloAddress.ClusterId.Equals(cluster) && g.Status == GatewayStatus.Active)
                .Select(g => g.SiloAddress)
                .ToList();


            return gateways[randomgenerator.Next(gateways.Count)];
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
    

        public async Task Start(ISiloStatusOracle oracle)
        {
            logger.Info(ErrorCode.Gossip_Starting, "GossipOracle starting on {0} ", Silo);
            try
            {
                this.silostatusoracle = oracle;

                silostatusoracle.SubscribeToSiloStatusEvents(this);

                await Gossip();

                StartTimer();

                logger.Info(ErrorCode.Gossip_Starting, "GossipOracle started on {0} ", Silo);
            }
            catch (Exception exc)
            {
                logger.Error(ErrorCode.Gossip_FailedToStart, "GossipOracle failed to start {0}", exc);
                throw;
            }
        }

        private void StartTimer()
        {
            if (timer != null)
                timer.Dispose();

            timer = GrainTimer.FromTimerCallback(
                (object dummy) => {
                    if (logger.IsVerbose2)
                        logger.Verbose2("-timer");
                    worker.Notify();
                    }, null, TimeSpan.Zero, TimerInterval, "Gossip.GatewayTimer");

            timer.Start();
        }

        //TODO : make these global parameters
        static TimeSpan ResendActiveStatusAfter = new TimeSpan(hours: 0, minutes: 10, seconds: 0);
        static TimeSpan CleanupSilentGoneGatewaysAfter = new TimeSpan(hours: 0, minutes: 0, seconds: 30);
        static TimeSpan RefreshInterval = new TimeSpan(hours: 0, minutes: 0, seconds: 10);
        static TimeSpan TimerInterval = new TimeSpan(hours: 0, minutes: 0, seconds: 10);  

        // called in response to changed status, and periodically
        // only one call active at a time
        private async Task Work()
        {
            var activelocalgateways = silostatusoracle.GetApproximateGateways();

            var iamgateway = activelocalgateways.Contains(Silo);

            var localstatuschanged = InjectLocalStatus(iamgateway);

            var demotesomegateways = (iamgateway) ? DemoteLocalGateways(activelocalgateways) : false;

            if (localstatuschanged ||
                demotesomegateways ||
                (DateTime.UtcNow - lastrefresh) > RefreshInterval)
            {
                await Gossip();
            }
        }

        private async Task Gossip()
        {
            // gossip with sources
            var gossiptasks = new List<Task<bool>>();

            foreach(var c in gossipChannels)
                gossiptasks.Add(GossipWith(c));

            await Task.WhenAll(gossiptasks);

            lastrefresh = DateTime.UtcNow;

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


        private bool InjectLocalStatus(bool isgateway)
        {
            var currentstatus = new GatewayEntry()
            {
                SiloAddress = Silo,
                Status = isgateway ? GatewayStatus.Active : GatewayStatus.Inactive,
                HeartbeatTimestamp = DateTime.UtcNow
            };

            GatewayEntry whatsthere;

            // send if status is changed, or we are active and haven't said so in a while
            if (! gossipOracleData.Current.Gateways.TryGetValue(Silo, out whatsthere)
                || whatsthere.Status != currentstatus.Status
                || (currentstatus.Status == GatewayStatus.Active 
                      && currentstatus.HeartbeatTimestamp - whatsthere.HeartbeatTimestamp > ResendActiveStatusAfter))
            {
                // update current gossip data with status
                gossipOracleData.ApplyGossipDataAndNotify(new GossipData(currentstatus));

                return true;
            }

            return false;
        }

        private bool DemoteLocalGateways(IEnumerable<SiloAddress> activegateways)
        {
            var now = DateTime.UtcNow;

            // mark gateways as inactive if they have not recently advertised their existence,
            // and if they are not designated gateways as per membership table
            var tobeupdated = gossipOracleData.Current.Gateways
                .Where(g => g.Key.ClusterId == Silo.ClusterId
                       && g.Value.Status == GatewayStatus.Active
                       && (now - g.Value.HeartbeatTimestamp > CleanupSilentGoneGatewaysAfter)
                       && !activegateways.Contains(g.Key))
                .Select(g => new GatewayEntry()
                {
                    SiloAddress = g.Value.SiloAddress,
                    Status = GatewayStatus.Inactive,
                    HeartbeatTimestamp = now
                });

            var data = new GossipData(tobeupdated);

            if (data.IsEmpty)
                return false;
             
            gossipOracleData.ApplyGossipDataAndNotify(data);
            return true;            
        }

     

        public void SiloStatusChangeNotification(SiloAddress updatedSilo, SiloStatus status)
        {
            // any status change can cause changes in gateway list
            worker.Notify();
        }

    }
}
