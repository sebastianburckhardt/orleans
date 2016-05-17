using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans.Runtime.Configuration;
using Orleans.MultiCluster;

namespace Orleans.Runtime.MultiClusterNetwork
{
    internal class MultiClusterOracle : SystemTarget, IMultiClusterOracle, ISiloStatusListener, IMultiClusterGossipService
    {
        // as a backup measure, current local active status is sent occasionally
        public static readonly TimeSpan ResendActiveStatusAfter = TimeSpan.FromMinutes(10);

        // time after which this gateway removes other gateways in this same cluster that are known to be gone 
        public static readonly TimeSpan CleanupSilentGoneGatewaysAfter = TimeSpan.FromSeconds(30);

        private readonly List<IGossipChannel> gossipChannels;
        private readonly MultiClusterOracleData localData;
        private readonly TraceLogger logger;
        private readonly SafeRandom random;
        private readonly string clusterId;
        private readonly IReadOnlyList<string> defaultMultiCluster;

        // to avoid convoying, each silo randomizes these period intervals
        private readonly TimeSpan randomizedBackgroundGossipInterval;
        private TimeSpan randomizedResendActiveStatusAfter;

        private GrainTimer timer;
        private ISiloStatusOracle siloStatusOracle;
        private MultiClusterConfiguration injectedConfig;

        public MultiClusterOracle(SiloAddress silo, List<IGossipChannel> sources, GlobalConfiguration config)
            : base(Constants.MultiClusterOracleId, silo)
        {
            if (sources == null) throw new ArgumentNullException("sources");
            if (silo == null) throw new ArgumentNullException("silo");

            logger = TraceLogger.GetLogger("MultiClusterOracle");
            gossipChannels = sources;
            localData = new MultiClusterOracleData(logger);
            clusterId = config.ClusterId;
            defaultMultiCluster = config.DefaultMultiCluster;  
            random = new SafeRandom();
            randomizedBackgroundGossipInterval = RandomizeTimespan(config.BackgroundGossipInterval);
            randomizedResendActiveStatusAfter = RandomizeTimespan(ResendActiveStatusAfter);
        }
   
        // randomize a timespan by up to 10%
        private TimeSpan RandomizeTimespan(TimeSpan value)
        {
            return TimeSpan.FromMilliseconds(value.TotalMilliseconds * (0.9 + (random.NextDouble() * 0.1)));
        }

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
                 .Select(g => g.ClusterId)
                 .Distinct();

            return clusters;
        }

        public IEnumerable<GatewayEntry> GetGateways()
        {
            return localData.Current.Gateways.Values;
        }

        public SiloAddress GetRandomClusterGateway(string cluster)
        {
            var activeGateways = this.localData.Current.Gateways.Values
                .Where(gw => gw.ClusterId == cluster && gw.Status == GatewayStatus.Active)
                .Select(gw => gw.SiloAddress)
                .ToList();

            if (activeGateways.Count == 0)
                return null;

            return activeGateways[random.Next(activeGateways.Count)];
        }

        public MultiClusterConfiguration GetMultiClusterConfiguration()
        {
            return localData.Current.Configuration;
        }

        public async Task InjectMultiClusterConfiguration(MultiClusterConfiguration config)
        {
            this.injectedConfig = config;

            logger.Info("Starting MultiClusterConfiguration Injection, configuration={0} ", config);

            PublishChanges();

            // wait for the gossip channel tasks and aggregate exceptions
            var currentChannelTasks = this.channelTasks.Values.ToList();
            await Task.WhenAll(currentChannelTasks.Select(ct => ct.Task));

            var exceptions = currentChannelTasks
                .Where(ct => ct.LastException != null)
                .Select(ct => ct.LastException)
                .ToList();

            logger.Info("Completed MultiClusterConfiguration Injection, {0} exceptions", exceptions.Count);

            if (exceptions.Count > 0)
                throw new AggregateException(exceptions);
        }

        public void SiloStatusChangeNotification(SiloAddress updatedSilo, SiloStatus status)
        {
            // any status change can cause changes in gateway list
            PublishChanges();
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
            logger.Info(ErrorCode.MultiClusterNetwork_Starting, "MultiClusterOracle starting on {0}, Severity={1} ", Silo, logger.SeverityLevel);
            try
            {
                if (string.IsNullOrEmpty(clusterId))
                    throw new OrleansException("Internal Error: missing cluster id");

                this.siloStatusOracle = oracle;

                // startup: pull all the info from the tables, then inject default multi cluster if none found
                foreach (var ch in gossipChannels)
                {
                    SynchronizeWithChannel(ch);
                }

                await Task.WhenAll(this.channelTasks.Select(kvp => kvp.Value.Task));
                if (GetMultiClusterConfiguration() == null && defaultMultiCluster != null)
                {
                    this.injectedConfig = new MultiClusterConfiguration(DateTime.UtcNow, defaultMultiCluster, "DefaultMultiCluster");
                    logger.Info("No configuration found. Using default configuration {0} ", this.injectedConfig);
                }

                this.siloStatusOracle.SubscribeToSiloStatusEvents(this);

                PublishChanges();

                StartTimer(); // for periodic full bulk gossip

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
                this.OnGossipTimerTick,
                null,
                this.randomizedBackgroundGossipInterval,
                this.randomizedBackgroundGossipInterval,
                "MultiCluster.GossipTimer");

            timer.Start();
        }

        private void OnGossipTimerTick(object _)
        {
            logger.Verbose3("-timer");
            PublishChanges();
            PeriodicBackgroundGossip();
        }

        // called in response to changed status, and periodically
        private void PublishChanges()
        {
             logger.Verbose("--- PublishChanges: assess");

            var activeLocalGateways = this.siloStatusOracle.GetApproximateMultiClusterGateways();

            var iAmGateway = activeLocalGateways.Contains(Silo);

            // collect deltas that need to be published to all other gateways. 
            // Most of the time, this will contain just zero or one change.
            var deltas = new MultiClusterData();

            // Determine local status, and add to deltas if it changed
            InjectLocalStatus(iAmGateway, ref deltas);

            // Determine if admin has injected a new configuration, and add to deltas if that is the case
            InjectConfiguration(ref deltas);

            // Determine if there are some stale gateway entries of this cluster that should be demoted, 
            // and add those demotions to deltas
            if (iAmGateway)
                DemoteLocalGateways(activeLocalGateways, ref deltas);

            if (logger.IsVerbose)
                logger.Verbose("--- PublishChanges: found activeGateways={0} iAmGateway={1} publish={2}",
                   string.Join(",", activeLocalGateways), iAmGateway, deltas);

            if (!deltas.IsEmpty)
            {
                // Now we do the actual publishing. Note that we publish deltas only once and 
                // simply log any errors without retrying. To handle problems 
                // caused by lost messages we rely instead on the periodic background gossip: 
                // each node periodically does full two-way gossip (Synchronize) with 
                // some random other node or channel. This ensures all information 
                // eventually gets everywhere.

                // publish deltas to all remote clusters 
                foreach (var x in this.AllClusters().Where(x => x != this.clusterId))
                {
                    PublishGossipToCluster(x, deltas);
                }

                // publish deltas to all local silos
                var activeLocalClusterSilos = this.GetApproximateOtherActiveSilos();

                foreach (var activeLocalClusterSilo in activeLocalClusterSilos)
                {
                    PublishGossipToSilo(activeLocalClusterSilo, deltas);
                }

                // publish deltas to all gossip channels
                foreach (var ch in gossipChannels)
                {
                    PublishGossipToChannel(ch, deltas);
                }
            }

            if (deltas.Gateways.ContainsKey(this.Silo) && deltas.Gateways[this.Silo].Status == GatewayStatus.Active)
            {
                // Fully synchronize with channels if we just went active, which helps with initial startup time.
                // Note: doing a partial publish just before this full synchronize is by design, so that it reduces stabilization
                // time when several Silos are starting up at the same time, and there already is information about each other
                // before they attempt the full gossip
                foreach (var ch in gossipChannels)
                {
                    SynchronizeWithChannel(ch);
                }
            }

            logger.Verbose("--- PublishChanges: done");
        }

        private IEnumerable<SiloAddress> GetApproximateOtherActiveSilos()
        {
            return this.siloStatusOracle.GetApproximateSiloStatuses()
                .Where(kvp => !kvp.Key.Equals(this.Silo) && kvp.Value == SiloStatus.Active)
                .Select(kvp => kvp.Key);
        }

        private void PeriodicBackgroundGossip()
        {
            logger.Verbose("--- PeriodicBackgroundGossip");
            // pick random target for full gossip
            var gateways = localData.Current.Gateways.Values
                           .Where(gw => !gw.SiloAddress.Equals(this.Silo) && gw.Status == GatewayStatus.Active)
                           .ToList();
            var pick = random.Next(gateways.Count + gossipChannels.Count);
            if (pick < gateways.Count)
            {
                var address = gateways[pick].SiloAddress;
                var cluster = gateways[pick].ClusterId;
                FullGossipWithSilo(address, cluster);
            }
            else
            {
                var address = gossipChannels[pick - gateways.Count];
                SynchronizeWithChannel(address);
            }

            // report summary of encountered communication problems in log
            var unreachableClusters = string.Join(",", this.clusterTasks
                .Where(kvp => kvp.Value.LastException != null)
                .Select(kvp => string.Format("{0}({1})", kvp.Key, kvp.Value.LastException.GetType().Name)));
            if (!string.IsNullOrEmpty(unreachableClusters))
                logger.Info(ErrorCode.MultiClusterNetwork_GossipCommunicationFailure, "Gossip Communication: cannot reach clusters {0}", unreachableClusters);

            var unreachableSilos = string.Join(",", this.siloTasks
                .Where(kvp => kvp.Value.LastException != null)
                .Select(kvp => string.Format("{0}({1})", kvp.Key, kvp.Value.LastException.GetType().Name)));
            if (!string.IsNullOrEmpty(unreachableSilos))
                logger.Info(ErrorCode.MultiClusterNetwork_GossipCommunicationFailure, "Gossip Communication: cannot reach silos {0}", unreachableSilos);

            var unreachableChannels = string.Join(",", this.channelTasks
                  .Where(kvp => kvp.Value.LastException != null)
                  .Select(kvp => string.Format("{0}({1})", kvp.Key, kvp.Value.LastException.GetType().Name)));
            if (!string.IsNullOrEmpty(unreachableChannels))
                logger.Info(ErrorCode.MultiClusterNetwork_GossipCommunicationFailure, "Gossip Communication: cannot reach channels {0}", unreachableChannels);

            // discard old status information
            RemoveStaleTaskStatusEntries(this.clusterTasks);
            RemoveStaleTaskStatusEntries(this.siloTasks);

            logger.Verbose("--- PeriodicBackgroundGossip: done");
        }

        // the set of all known clusters
        private IEnumerable<string> AllClusters()
        {
            var allClusters = localData.Current.Gateways.Values.Select(gw => gw.ClusterId);
            if (localData.Current.Configuration != null)
            {
                allClusters = allClusters.Union(localData.Current.Configuration.Clusters);
            }

            return new HashSet<string>(allClusters);
        }

        private void RemoveStaleTaskStatusEntries<K>(Dictionary<K, GossipStatus> dict)
        {
            var now = DateTime.UtcNow;
            var toRemove = dict
                .Where(kvp => (now - kvp.Value.LastUse).TotalMilliseconds > 2.5 * this.randomizedResendActiveStatusAfter.TotalMilliseconds)
                .Select(kvp => kvp.Key)
                .ToList();

            foreach (var key in toRemove)
                dict.Remove(key);
        }
      
        // called by remote nodes that publish changes
        public Task Publish(IMultiClusterGossipData gossipData, bool forwardLocally)
        {
            logger.Verbose("--- Publish: receive {0} data {1}", forwardLocally ? "remote" : "local", gossipData);

            var data = (MultiClusterData)gossipData;

            var delta = localData.ApplyDataAndNotify(data);

            // forward changes to all local silos
            if (forwardLocally)
            {
                foreach (var activeSilo in this.GetApproximateOtherActiveSilos())
                    PublishGossipToSilo(activeSilo, delta);
            }

            PublishMyStatusToNewDestinations(delta);

            logger.Verbose("--- Publish: done");

            return TaskDone.Done;
        }

        // called by remote nodes' full background gossip
        public Task<IMultiClusterGossipData> Synchronize(IMultiClusterGossipData gossipData)
        {
            logger.Verbose("--- Synchronize: gossip {0}", gossipData);

            var data = (MultiClusterData)gossipData;

            var delta = this.localData.ApplyDataAndNotify(data);

            PublishMyStatusToNewDestinations(delta);

            logger.Verbose("--- Synchronize: done, answer={0}", delta);

            return Task.FromResult((IMultiClusterGossipData)delta);
        }

        // initiate a search for lagging silos, contacting other silos and clusters
        public async Task<List<SiloAddress>> FindLaggingSilos(MultiClusterConfiguration expected)
        {
            var tasks = new List<Task<List<SiloAddress>>>();

            // check this cluster for lagging silos
            tasks.Add(FindLaggingSilos(expected, true));

            // check all other clusters for lagging silos
            foreach (var cluster in GetActiveClusters())
            {
                if (cluster != this.clusterId)
                {
                    var silo = GetRandomClusterGateway(cluster);
                    if (silo == null)
                        throw new OrleansException("no gateway for cluster " + cluster);
                    var remoteOracle = InsideRuntimeClient.Current.InternalGrainFactory.GetSystemTarget<IMultiClusterGossipService>(Constants.MultiClusterOracleId, silo);
                    tasks.Add(remoteOracle.FindLaggingSilos(expected, true));
                }
            }

            // This function is called  during manual admin operations through 
            // IManagementGrain (change configuration, or check stability).
            // Users are going to want to see the exception details to figure out 
            // what is going on.
            await Task.WhenAll(tasks);

            return tasks.SelectMany(t => t.Result).ToList();
        }

        // receive a remote request for finding lagging silos in this cluster or on this silo
        public async Task<List<SiloAddress>> FindLaggingSilos(MultiClusterConfiguration expected, bool forwardLocally)
        {
            logger.Verbose("--- FindLaggingSilos: {0}, {1}", forwardLocally ? "remote" : "local", expected);

            var result = new List<SiloAddress>();

            // check if this silo is lagging
            if (!MultiClusterConfiguration.Equals(localData.Current.Configuration, expected))
                result.Add(this.Silo);

            if (forwardLocally)
            {
                // contact all other active silos in this cluster

                var tasks = new List<Task<List<SiloAddress>>>();

                foreach (var activeSilo in this.GetApproximateOtherActiveSilos())
                {
                    var remoteOracle = InsideRuntimeClient.Current.InternalGrainFactory.GetSystemTarget<IMultiClusterGossipService>(Constants.MultiClusterOracleId, activeSilo);
                    tasks.Add(remoteOracle.FindLaggingSilos(expected, false));
                }
 
                await Task.WhenAll(tasks);

                foreach (var silo in tasks.SelectMany(t => t.Result))
                {
                    result.Add(silo);
                }
            }

            logger.Verbose("--- FindLaggingSilos: done, found {0}", result.Count);

            return result;
        }

        private void PublishMyStatusToNewDestinations(MultiClusterData delta)
        {
            // for quicker convergence, we publish active local status information
            // immediately when we learn about a new destination

            GatewayEntry myEntry;

            if (!localData.Current.Gateways.TryGetValue(this.Silo, out myEntry)
                || myEntry.Status != GatewayStatus.Active)
                return;

            foreach (var gateway in delta.Gateways.Values)
            {
                GossipStatus gossipStatus;
                var destinationCluster = gateway.ClusterId;

                if (destinationCluster == this.clusterId)
                {
                    // local cluster
                    this.siloTasks.TryGetValue(gateway.SiloAddress, out gossipStatus);
                    if (!this.siloTasks.TryGetValue(gateway.SiloAddress, out gossipStatus) || !gossipStatus.KnowsMe)
                        PublishGossipToSilo(gateway.SiloAddress, new MultiClusterData(myEntry));
                }
                else
                {
                    // remote cluster
                    if (!this.clusterTasks.TryGetValue(destinationCluster, out gossipStatus) || !gossipStatus.KnowsMe)
                        PublishGossipToCluster(destinationCluster, new MultiClusterData(myEntry));
                }
            }
        }

        private class GossipStatus
        {
            public SiloAddress Address;
            public Task Task = TaskDone.Done;
            public Exception LastException;
            public bool KnowsMe;
            public bool Pending;
            public DateTime LastUse = DateTime.UtcNow;
        }

        // tasks for gossip
        private readonly Dictionary<SiloAddress, GossipStatus> siloTasks = new Dictionary<SiloAddress, GossipStatus>();
        private readonly Dictionary<string, GossipStatus> clusterTasks = new Dictionary<string, GossipStatus>();
        private readonly Dictionary<IGossipChannel, GossipStatus> channelTasks = new Dictionary<IGossipChannel, GossipStatus>();
 
        // numbering for tasks (helps when analyzing logs)
        private int idCounter;

        private void PublishGossipToSilo(SiloAddress destinationSilo, MultiClusterData delta)
        {
            GossipStatus status;
            if (!this.siloTasks.TryGetValue(destinationSilo, out status))
                this.siloTasks[destinationSilo] = status = new GossipStatus();

            int id = ++this.idCounter;
            logger.Verbose("-{0} PublishGossipToSilo {1} {2}", id, destinationSilo, delta);

            // the task that actually publishes
            Func<Task, Task> publishAsync = async (Task prev) =>
            {
                await prev; // wait for previous publish to same silo
                status.LastUse = DateTime.UtcNow;
                status.Pending = true;
                status.Address = destinationSilo;
                try
                {
                    // publish to the remote system target
                    var remoteOracle = InsideRuntimeClient.Current.InternalGrainFactory.GetSystemTarget<IMultiClusterGossipService>(Constants.MultiClusterOracleId, destinationSilo);
                    await remoteOracle.Publish(delta, false);

                    status.LastException = null;
                    if (delta.Gateways.ContainsKey(this.Silo))
                        status.KnowsMe = delta.Gateways[this.Silo].Status == GatewayStatus.Active;
                    logger.Verbose("-{0} PublishGossipToSilo successful", id);
                }
                catch (Exception e)
                {
                    logger.Warn(ErrorCode.MultiClusterNetwork_GossipCommunicationFailure,
                        string.Format("-{0} PublishGossipToSilo {1} failed", id, destinationSilo), e);
                    status.LastException = e;
                }
                status.LastUse = DateTime.UtcNow;
                status.Pending = false;
            };

            // queue the publish - we are not awaiting it!
            status.Task = publishAsync(status.Task);
        }

        private void PublishGossipToCluster(string destinationCluster, MultiClusterData delta)
        {
            GossipStatus status;
            if (!this.clusterTasks.TryGetValue(destinationCluster, out status))
                this.clusterTasks[destinationCluster] = status = new GossipStatus();

            int id = ++this.idCounter;
            logger.Verbose("-{0} PublishGossipToCluster {1} {2}", id, destinationCluster, delta);

            // the task that actually publishes
            Func<Task, Task> publishAsync = async (Task prev) =>
            {
                await prev; // wait for previous publish to same cluster
                status.LastUse = DateTime.UtcNow;
                status.Pending = true;
                try
                {
                    // pick a random gateway if we don't already have one or it is not active anymore
                    if (status.Address == null 
                        || !this.localData.Current.IsActiveGatewayForCluster(status.Address, destinationCluster))
                    {
                        status.Address = GetRandomClusterGateway(destinationCluster);
                    }

                    if (status.Address == null)
                        throw new OrleansException("could not notify cluster: no gateway found");

                    // publish to the remote system target
                    var remoteOracle = InsideRuntimeClient.Current.InternalGrainFactory.GetSystemTarget<IMultiClusterGossipService>(Constants.MultiClusterOracleId, status.Address);
                    await remoteOracle.Publish(delta, true);

                    status.LastException = null;
                    if (delta.Gateways.ContainsKey(this.Silo))
                        status.KnowsMe = delta.Gateways[this.Silo].Status == GatewayStatus.Active;
                    logger.Verbose("-{0} PublishGossipToCluster successful", id);
                }
                catch (Exception e)
                {
                    logger.Warn(ErrorCode.MultiClusterNetwork_GossipCommunicationFailure,
                        string.Format("-{0} PublishGossipToCluster {1} failed", id, destinationCluster), e);
                    status.LastException = e;
                    status.Address = null; // this gateway was no good... pick random gateway again next time
                }
                status.LastUse = DateTime.UtcNow;
                status.Pending = false;
            };

            // queue the publish - we are not awaiting it!
            status.Task = publishAsync(status.Task);
        }

        private void PublishGossipToChannel(IGossipChannel channel, MultiClusterData delta)
        {
            GossipStatus status;
            if (!this.channelTasks.TryGetValue(channel, out status))
                this.channelTasks[channel] = status = new GossipStatus();

            int id = ++this.idCounter;
            logger.Verbose("-{0} PublishGossipToChannel {1} {2}", id, channel.Name, delta);

            // the task that actually publishes
            Func<Task, Task> publishAsync = async (Task prev) =>
            {
                await prev; // wait for previous publish to same cluster
                status.LastUse = DateTime.UtcNow;
                status.Pending = true;
                try
                {
                    await channel.Publish(delta);

                    status.LastException = null;
                    logger.Verbose("-{0} PublishGossipToChannel successful, answer={1}", id, delta);
                }
                catch (Exception e)
                {
                    logger.Warn(ErrorCode.MultiClusterNetwork_GossipCommunicationFailure,
                        string.Format("-{0} PublishGossipToChannel {1} failed", id, channel.Name), e);
                    status.LastException = e;
                }
                status.LastUse = DateTime.UtcNow;
                status.Pending = false;
            };

            // queue the publish - we are not awaiting it!
            status.Task = publishAsync(status.Task);
        }

        private void SynchronizeWithChannel(IGossipChannel channel)
        {
            GossipStatus status;
            if (!this.channelTasks.TryGetValue(channel, out status))
                this.channelTasks[channel] = status = new GossipStatus();

            int id = ++this.idCounter;
            logger.Verbose("-{0} FullGossipWithChannel {1}", id, channel.Name);

            // the task that actually gossips
            Func<Task, Task> gossipAsync = async (Task prev) =>
            {
                await prev; // wait for previous gossip with same channel
                status.LastUse = DateTime.UtcNow;
                status.Pending = true;
                try
                {
                    var answer = await channel.Synchronize(this.localData.Current);

                    // apply what we have learnt
                    var delta = this.localData.ApplyDataAndNotify(answer);

                    status.LastException = null;
                    logger.Verbose("-{0} FullGossipWithChannel successful", id);

                    PublishMyStatusToNewDestinations(delta);
                }
                catch (Exception e)
                {
                    logger.Warn(ErrorCode.MultiClusterNetwork_GossipCommunicationFailure,
                        string.Format("-{0} FullGossipWithChannel {1} failed", id, channel.Name), e);
                    status.LastException = e;
                }
                status.LastUse = DateTime.UtcNow;
                status.Pending = false;
            };

            // queue the gossip - we are not awaiting it!
            status.Task = gossipAsync(status.Task);
        }

        private void FullGossipWithSilo(SiloAddress destinationSilo, string destinationCluster)
        {
            GossipStatus status;

            if (destinationCluster != this.clusterId)
            {
                if (!this.clusterTasks.TryGetValue(destinationCluster, out status))
                    this.clusterTasks[destinationCluster] = status = new GossipStatus();
            }
            else
            {
                if (!this.siloTasks.TryGetValue(destinationSilo, out status))
                    this.siloTasks[destinationSilo] = status = new GossipStatus();
            }

            int id = ++this.idCounter;
            logger.Verbose("-{0} FullGossipWithSilo {1} in cluster {2}", id, destinationSilo, destinationCluster);

            // the task that actually gossips
            Func<Task, Task> publishasync = async (Task prev) =>
            {
               await prev; // wait for previous gossip to same silo
               status.LastUse = DateTime.UtcNow;
               status.Pending = true;
               status.Address = destinationSilo;
               try
                {
                    var remoteOracle = InsideRuntimeClient.Current.InternalGrainFactory.GetSystemTarget<IMultiClusterGossipService>(Constants.MultiClusterOracleId, destinationSilo);
                    var answer = (MultiClusterData) await remoteOracle.Synchronize(localData.Current);

                    // apply what we have learnt
                    var delta = localData.ApplyDataAndNotify(answer);
                    
                    status.LastException = null;
                    logger.Verbose("-{0} FullGossipWithSilo successful, answer={1}", id, answer);

                    PublishMyStatusToNewDestinations(delta);
                }
                catch (Exception e)
                {
                    logger.Warn(ErrorCode.MultiClusterNetwork_GossipCommunicationFailure,
                        string.Format("-{0} FullGossipWithSilo {1} in cluster {2} failed", id, destinationSilo, destinationCluster), e);
                    status.LastException = e;
                }
                status.LastUse = DateTime.UtcNow;
                status.Pending = false;
            };

            // queue the publish - we are not awaiting it!
            status.Task = publishasync(status.Task);
        }


        private void InjectConfiguration(ref MultiClusterData deltas)
        {
            if (this.injectedConfig == null)
                return;

            var data = new MultiClusterData(this.injectedConfig);
            this.injectedConfig = null;

            if (logger.IsVerbose)
                logger.Verbose("-InjectConfiguration {0}", data.Configuration.ToString());

            var delta = this.localData.ApplyDataAndNotify(data);

            if (!delta.IsEmpty)
                deltas = deltas.Merge(delta);
        }

        private void InjectLocalStatus(bool isGateway, ref MultiClusterData deltas)
        {
            var myStatus = new GatewayEntry()
            {
                ClusterId = clusterId,
                SiloAddress = Silo,
                Status = isGateway ? GatewayStatus.Active : GatewayStatus.Inactive,
                HeartbeatTimestamp = DateTime.UtcNow,
            };

            GatewayEntry existingEntry;

            // do not update if we are reporting inactive status and entry is not already there
            if (!this.localData.Current.Gateways.TryGetValue(Silo, out existingEntry) && !isGateway)
                return;

            // send if status is changed, or we are active and haven't said so in a while
            if (existingEntry == null
                || existingEntry.Status != myStatus.Status
                || (myStatus.Status == GatewayStatus.Active
                      && myStatus.HeartbeatTimestamp - existingEntry.HeartbeatTimestamp > this.randomizedResendActiveStatusAfter))
            {
                logger.Verbose2("-InjectLocalStatus {0}", myStatus);

                // update current data with status
                var delta = this.localData.ApplyDataAndNotify(new MultiClusterData(myStatus));

                if (!delta.IsEmpty)
                    deltas = deltas.Merge(delta);
            }
        }

        private void DemoteLocalGateways(IReadOnlyList<SiloAddress> activeGateways, ref MultiClusterData deltas)
        {
            var now = DateTime.UtcNow;

            // mark gateways as inactive if they have not recently advertised their existence,
            // and if they are not designated gateways as per membership table
            var toBeUpdated = this.localData.Current.Gateways.Values
                .Where(g => g.ClusterId == clusterId
                       && g.Status == GatewayStatus.Active
                       && (now - g.HeartbeatTimestamp > CleanupSilentGoneGatewaysAfter)
                       && !activeGateways.Contains(g.SiloAddress))
                .Select(g => new GatewayEntry()
                {
                    ClusterId = g.ClusterId,
                    SiloAddress = g.SiloAddress,
                    Status = GatewayStatus.Inactive,
                    HeartbeatTimestamp = g.HeartbeatTimestamp + CleanupSilentGoneGatewaysAfter,
                }).ToList();

            if (toBeUpdated.Count == 0)
                return;

            var data = new MultiClusterData(toBeUpdated);

            if (logger.IsVerbose)
                logger.Verbose("-DemoteLocalGateways {0}", data.ToString());
 
            var delta = this.localData.ApplyDataAndNotify(data);

            if (!delta.IsEmpty)
            {
                deltas = deltas.Merge(delta);
            }
        }
    }
}
