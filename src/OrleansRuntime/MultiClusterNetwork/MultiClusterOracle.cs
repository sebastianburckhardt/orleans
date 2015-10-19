using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Orleans.Runtime.Configuration;
using Orleans.MultiCluster;

namespace Orleans.Runtime.MultiClusterNetwork
{
    internal class MultiClusterOracle : SystemTarget, IMultiClusterOracle, ISiloStatusListener
    {
        private static readonly Random random = new Random();

        private readonly List<IGossipChannel> gossipChannels;

        private readonly MultiClusterOracleData localData;
        private readonly TraceLogger logger;
        
        private GrainTimer timer;
        DateTime lastrefresh;

        private ISiloStatusOracle silostatusoracle;

        private string globalServiceId;

        private readonly BackgroundWorker gossipworker;

        private IReadOnlyList<string> defaultMultiCluster;

        public MultiClusterOracle(SiloAddress silo, List<IGossipChannel> sources, GlobalConfiguration config)
            : base(Constants.MultiClusterOracleId, silo)
        {
            Debug.Assert(sources != null);
            logger = TraceLogger.GetLogger("MultiClusterOracle");
            gossipChannels = sources;
            localData = new MultiClusterOracleData(logger);
            globalServiceId = config.GlobalServiceId;
            defaultMultiCluster = config.DefaultMultiCluster;
            gossipworker = new BackgroundWorker(() => GossipWork());
            RefreshInterval = config.GossipChannelRefreshTimeout;
        }


        // polling interval for gossip channels
        public TimeSpan RefreshInterval;


        public bool IsFunctionalClusterGateway(SiloAddress siloAddress)
        {
            GatewayEntry g;
            return localData.Current.Gateways.TryGetValue(siloAddress, out g) 
                && g.Status == GatewayStatus.Active;
        }

        public IEnumerable<string> GetActiveClusters()
        {
            var clusters = localData.Current.Gateways.Values
                 .Where(g => g.Status == GatewayStatus.Active)
                 .Select(g => g.SiloAddress.ClusterId)
                 .Distinct();

            return clusters;
        }

        public IEnumerable<GatewayEntry> GetGateways()
        {
            return localData.Current.Gateways.Values;
        }

        public SiloAddress GetRandomClusterGateway(string cluster)
        {
            //TraceLogger.GetLogger("CMOD").Info("local data {0}", localTable);
            var gateways = localData.Current.Gateways.Values
                .Where(g => g.SiloAddress.ClusterId.Equals(cluster) && g.Status == GatewayStatus.Active)
                .Select(g => g.SiloAddress)
                .ToList();

            if (gateways.Count == 0)
                return null;

            return gateways[random.Next(gateways.Count)];
        }

        public MultiClusterConfiguration GetMultiClusterConfiguration()
        {
            return localData.Current.Configuration;
        }

        public Task InjectMultiClusterConfiguration(MultiClusterConfiguration config)
        {
            injectedconfig = config;
            return gossipworker.NotifyAndWait();
        }

        private MultiClusterConfiguration injectedconfig;


        public void SiloStatusChangeNotification(SiloAddress updatedSilo, SiloStatus status)
        {
            // any status change can cause changes in gateway list
            gossipworker.Notify();
        }

        public bool SubscribeToMultiClusterConfigurationEvents(GrainReference observer)
        {
            return localData.SubscribeToMultiClusterConfigurationEvents(observer);
        }

        public bool UnSubscribeFromMultiClusterConfigurationEvents(GrainReference observer)
        {
            return localData.UnSubscribeFromMultiClusterConfigurationEvents(observer);
        }

        public async Task Start(ISiloStatusOracle oracle)
        {
            logger.Info(ErrorCode.MultiClusterNetwork_Starting, "MultiClusterOracle starting on {0} ", Silo);
            try
            {
                this.silostatusoracle = oracle;

                silostatusoracle.SubscribeToSiloStatusEvents(this);

                await Gossip();

                // use default multi cluster if none found
                if (GetMultiClusterConfiguration() == null && defaultMultiCluster != null)
                {
                    injectedconfig = new MultiClusterConfiguration(DateTime.UtcNow, defaultMultiCluster, "DefaultMultiCluster from GlobalConfig");
                    InjectConfiguration();
                    await Gossip();
                }

                StartTimer();

                logger.Info(ErrorCode.MultiClusterNetwork_Starting, "MultiClusterOracle started on {0} ", Silo);
            }
            catch (Exception exc)
            {
                logger.Error(ErrorCode.MultiClusterNetwork_FailedToStart, "MultiClusterOracle failed to start {0}", exc);
                throw;
            }
        }

        private void StartTimer()
        {
            if (timer != null)
                timer.Dispose();

            timer = GrainTimer.FromTimerCallback(
                (object dummy) => {
                    if (logger.IsVerbose3)
                        logger.Verbose3("-timer");
                    its_time_for_periodic_gossip = true;
                    gossipworker.Notify();
                }, null, TimeSpan.Zero, RefreshInterval, "MultiCluster.GossipTimer");

            timer.Start();
        }

        private bool its_time_for_periodic_gossip;


        //TODO : make these global parameters?
        
        // as a backup measure, current local active status is sent occasionally
        public static TimeSpan ResendActiveStatusAfter = new TimeSpan(hours: 0, minutes: 10, seconds: 0);

        // time after which this gateway removes other gateways in this same cluster that are known to be gone 
        public static TimeSpan CleanupSilentGoneGatewaysAfter = new TimeSpan(hours: 0, minutes: 0, seconds: 30);



        // called in response to changed status, and periodically
        // only one call active at a time
        private async Task GossipWork()
        {
            var activelocalgateways = silostatusoracle.GetApproximateMultiClusterGateways();

            var iamgateway = activelocalgateways.Contains(Silo);

            var localstatuschanged = InjectLocalStatus(iamgateway);

            var configchanged = InjectConfiguration();

            var demotesomegateways = (iamgateway) ? DemoteLocalGateways(activelocalgateways) : false;

            if (localstatuschanged ||
                configchanged ||
                demotesomegateways ||
                its_time_for_periodic_gossip)
            {
                its_time_for_periodic_gossip = false;
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
                logger.Warn(ErrorCode.MultiClusterNetwork_CommunicationFailure, "All Gossip channels failed");
        }


        private async Task<bool> GossipWith(IGossipChannel gossipsource)
        {
            MultiClusterData answer = null;
            try
            {
                answer = await gossipsource.PushAndPull(localData.Current);
            }
            catch (Exception exc)
            {
                logger.Warn(ErrorCode.MultiClusterNetwork_CommunicationFailure, "Gossip channel failed", exc);
                return false;
            }

            // apply what we have learnt
            localData.ApplyIncomingDataAndNotify(answer);

            return true;
        }

        private bool InjectConfiguration()
        {
            if (injectedconfig == null)
                return false;

            var data = new MultiClusterData(injectedconfig);
            injectedconfig = null;

            if (logger.IsVerbose)
                logger.Verbose("-InjectConfiguration {0}", data.Configuration.ToString());

            return localData.ApplyIncomingDataAndNotify(data);
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
            if (! localData.Current.Gateways.TryGetValue(Silo, out whatsthere)
                || whatsthere.Status != currentstatus.Status
                || (currentstatus.Status == GatewayStatus.Active 
                      && currentstatus.HeartbeatTimestamp - whatsthere.HeartbeatTimestamp > ResendActiveStatusAfter))
            {
                if (logger.IsVerbose2)
                    logger.Verbose2("-InjectLocalStatus {0}", currentstatus.ToString());

                // update current data with status
                return localData.ApplyIncomingDataAndNotify(new MultiClusterData(currentstatus));
            }

            return false;
        }

        private bool DemoteLocalGateways(IEnumerable<SiloAddress> activegateways)
        {
            var now = DateTime.UtcNow;

            // mark gateways as inactive if they have not recently advertised their existence,
            // and if they are not designated gateways as per membership table
            var tobeupdated = localData.Current.Gateways
                .Where(g => g.Key.ClusterId == Silo.ClusterId
                       && g.Value.Status == GatewayStatus.Active
                       && (now - g.Value.HeartbeatTimestamp > CleanupSilentGoneGatewaysAfter)
                       && !activegateways.Contains(g.Key))
                .Select(g => new GatewayEntry()
                {
                    SiloAddress = g.Value.SiloAddress,
                    Status = GatewayStatus.Inactive,
                    HeartbeatTimestamp = g.Value.HeartbeatTimestamp + CleanupSilentGoneGatewaysAfter
                });

            if (tobeupdated.Count() == 0)
                return false;

            var data = new MultiClusterData(tobeupdated);

            if (logger.IsVerbose)
                logger.Verbose("-DemoteLocalGateways {0}", data.ToString());
 
            return localData.ApplyIncomingDataAndNotify(data);
        }

     


    }
}
