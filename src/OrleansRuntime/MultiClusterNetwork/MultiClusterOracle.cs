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
        private readonly List<IGossipChannel> gossipChannels;
        private readonly MultiClusterOracleData localData;
        private readonly TraceLogger logger;
        private readonly Random random;

        private GrainTimer timer;

        private ISiloStatusOracle siloStatusOracle;
        private MultiClusterConfiguration injectedConfig;

        private string clusterId;

        private IReadOnlyList<string> defaultMultiCluster;

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
            random = new Random(silo.GetHashCode());
            RandomizedBackgroundGossipInterval = RandomizeTimespan(config.BackgroundGossipInterval);
            RandomizedResendActiveStatusAfter = RandomizeTimespan(ResendActiveStatusAfter);
        }
   
        // as a backup measure, current local active status is sent occasionally
        public static TimeSpan ResendActiveStatusAfter = new TimeSpan(hours: 0, minutes: 10, seconds: 0);

        // time after which this gateway removes other gateways in this same cluster that are known to be gone 
        public static TimeSpan CleanupSilentGoneGatewaysAfter = new TimeSpan(hours: 0, minutes: 0, seconds: 30);

        // to avoid convoying, each silo randomizes these period intervals
        private TimeSpan RandomizedBackgroundGossipInterval;
        private TimeSpan RandomizedResendActiveStatusAfter;

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
            var activegateways = new List<SiloAddress>();

            foreach(var gw in localData.Current.Gateways)
            {
                var cur = gw.Value;
                if (cur.ClusterId != cluster)
                    continue;
                if (cur.Status != GatewayStatus.Active)
                    continue;
                activegateways.Add(cur.SiloAddress);
            }

            if (activegateways.Count == 0)
                return null;

            return activegateways[random.Next(activegateways.Count)];
        }

        public MultiClusterConfiguration GetMultiClusterConfiguration()
        {
            return localData.Current.Configuration;
        }

        public async Task InjectMultiClusterConfiguration(MultiClusterConfiguration config)
        {
            this.injectedConfig = config;
            PushChanges();

            // wait for the gossip channel tasks and aggregate exceptions
            var exceptions = new List<Exception>();
            foreach (var ct in this.channelTasks.Values)
            {
                await ct.Task;
                if (ct.LastException != null)
                    exceptions.Add(ct.LastException);
            }

            if (exceptions.Count > 0)
                throw new AggregateException(exceptions);
        }

        public void SiloStatusChangeNotification(SiloAddress updatedSilo, SiloStatus status)
        {
            // any status change can cause changes in gateway list
            PushChanges();
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

                this.siloStatusOracle.SubscribeToSiloStatusEvents(this);

                // startup: pull all the info from the tables, then inject default multi cluster if none found
                foreach (var ch in gossipChannels)
                    FullGossipWithChannel(ch);

                await Task.WhenAll(this.channelTasks.Select(kvp => kvp.Value.Task));
                if (GetMultiClusterConfiguration() == null && defaultMultiCluster != null)
                {
                    this.injectedConfig = new MultiClusterConfiguration(DateTime.UtcNow, defaultMultiCluster, "DefaultMultiCluster");
                    logger.Info("No configuration found. Using default configuration {0} ", this.injectedConfig);
                }

                PushChanges();

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
                (object dummy) => {
                    if (logger.IsVerbose3)
                        logger.Verbose3("-timer");

                    PushChanges();
                    PeriodicBackgroundGossip();

                }, null, RandomizedBackgroundGossipInterval, RandomizedBackgroundGossipInterval, "MultiCluster.GossipTimer");

            timer.Start();
        }


        // called in response to changed status, and periodically
        private void PushChanges()
        {
             logger.Verbose("--- PushChanges: assess");

            var activeLocalGateways = this.siloStatusOracle.GetApproximateMultiClusterGateways();

            var iAmGateway = activeLocalGateways.Contains(Silo);

            var deltas = new MultiClusterData();

            InjectLocalStatus(iAmGateway, ref deltas);

            InjectConfiguration(ref deltas);

            if (iAmGateway)
                DemoteLocalGateways(activeLocalGateways, ref deltas);

            if (logger.IsVerbose)
                logger.Verbose("--- PushChanges: found activeGateways={0} iAmGateway={1} push={2}",
                   string.Join(",", activeLocalGateways), iAmGateway, deltas);

            if (!deltas.IsEmpty)
            {
                // push deltas to all remote clusters 
                foreach (var x in AllClusters())
                    if (x != clusterId)
                        PushGossipToCluster(x, deltas);

                // push deltas to all local silos
                foreach (var kvp in this.siloStatusOracle.GetApproximateSiloStatuses())
                    if (!kvp.Key.Equals(Silo) && kvp.Value == SiloStatus.Active)
                        PushGossipToSilo(kvp.Key, deltas);

                // push deltas to all gossip channels
                foreach (var c in gossipChannels)
                    PushGossipToChannel(c, deltas);
            }

            // fully synchronize with channels if we just went active
            // this helps with initial startup time
            if (deltas.Gateways.ContainsKey(this.Silo) && deltas.Gateways[this.Silo].Status == GatewayStatus.Active)
            {
                foreach (var ch in gossipChannels)
                    FullGossipWithChannel(ch);
            }

            logger.Verbose("--- PushChanges: done");
        }

        private void PeriodicBackgroundGossip()
        {
            logger.Verbose("--- PeriodicBackgroundGossip");
            // pick random target for full gossip
            var gateways = localData.Current.Gateways
                           .Where(kvp => !kvp.Key.Equals(this.Silo) && kvp.Value.Status == GatewayStatus.Active)
                           .ToList();
            var pick = random.Next(gateways.Count + gossipChannels.Count);
            if (pick < gateways.Count)
            {
                var address = gateways[pick].Key;
                var cluster = gateways[pick].Value.ClusterId;
                FullGossipWithSilo(address, cluster);
            }
            else
            {
                var address = gossipChannels[pick - gateways.Count];
                FullGossipWithChannel(address);
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
            var clusters = new HashSet<string>();
            if (localData.Current.Configuration != null)
                foreach (var c in localData.Current.Configuration.Clusters)
                    clusters.Add(c);
            foreach (var g in localData.Current.Gateways)
                clusters.Add(g.Value.ClusterId);
            return clusters;
        }

        private void RemoveStaleTaskStatusEntries<K>(Dictionary<K, GossipStatus> d)
        {
            var tbr = new List<K>();
            foreach (var kvp in d)
                if ((DateTime.UtcNow - kvp.Value.LastUse).TotalMilliseconds > 2.5 * RandomizedResendActiveStatusAfter.TotalMilliseconds)
                    tbr.Add(kvp.Key);
            foreach (var k in tbr)
                d.Remove(k);
        }
      
        // called by remote nodes that push changes
        public Task Push(IMultiClusterGossipData gossipData, bool forwardLocally)
        {
            logger.Verbose("--- Push: receive {0} data {1}", forwardLocally ? "remote" : "local", gossipData);

            var data = (MultiClusterData)gossipData;

            var delta = localData.ApplyDataAndNotify(data);

            // forward changes to all local silos
            if (forwardLocally)
                foreach (var kvp in this.siloStatusOracle.GetApproximateSiloStatuses())
                    if (!kvp.Key.Equals(Silo) && kvp.Value == SiloStatus.Active)
                        PushGossipToSilo(kvp.Key, delta);

            PushMyStatusToNewDestinations(delta);

            logger.Verbose("--- Push: done");

            return TaskDone.Done;
        }

        // called by remote nodes' full background gossip
        public Task<IMultiClusterGossipData> PushAndPull(IMultiClusterGossipData gossipData)
        {
            logger.Verbose("--- PushAndPull: gossip {0}", gossipData);

            var data = (MultiClusterData)gossipData;

            var delta = localData.ApplyDataAndNotify(data);

            PushMyStatusToNewDestinations(delta);

            logger.Verbose("--- PushAndPull: done, answer={0}", delta);

            return Task.FromResult((IMultiClusterGossipData)delta);
        }

        public async Task<Dictionary<SiloAddress, MultiClusterConfiguration>> CheckMultiClusterStability(MultiClusterConfiguration expected)
        {
            var localTask = FindUnstableSilos(expected, true);

            var remoteTasks = new List<Task<Dictionary<SiloAddress, MultiClusterConfiguration>>>();
            foreach (var cluster in GetActiveClusters())
            {
                if (cluster != clusterId)
                {
                    var silo = GetRandomClusterGateway(cluster);
                    if (silo == null)
                        throw new OrleansException("no gateway for cluster " + cluster);
                    var remoteOracle = InsideRuntimeClient.Current.InternalGrainFactory.GetSystemTarget<IMultiClusterGossipService>(Constants.MultiClusterOracleId, silo);
                    remoteTasks.Add(remoteOracle.FindUnstableSilos(expected, true));
                }
            }

            var result = await localTask;

            await Task.WhenAll(remoteTasks);

            foreach (var kvp in remoteTasks.SelectMany(t => t.Result))
            {
                result.Add(kvp.Key, kvp.Value);
            }

            return result;
        }

        public async Task<Dictionary<SiloAddress, MultiClusterConfiguration>> FindUnstableSilos(MultiClusterConfiguration expected, bool forwardLocally)
        {
            logger.Verbose("--- FindUnstableSilos: {0}, {1}", forwardLocally ? "remote" : "local", expected);

            var result = new Dictionary<SiloAddress, MultiClusterConfiguration>();

            if (! MultiClusterConfiguration.Equals(localData.Current.Configuration, expected))
                result.Add(this.Silo, localData.Current.Configuration);

            if (forwardLocally)
            {
                var tasks = new List<Task<Dictionary<SiloAddress, MultiClusterConfiguration>>>();

                  foreach (var kvp in this.siloStatusOracle.GetApproximateSiloStatuses())
                       if (!kvp.Key.Equals(Silo) && kvp.Value == SiloStatus.Active)
                       {
                           var silo = kvp.Key;
                           var remoteoracle = InsideRuntimeClient.Current.InternalGrainFactory.GetSystemTarget<IMultiClusterGossipService>(Constants.MultiClusterOracleId, silo);
                           tasks.Add(remoteoracle.FindUnstableSilos(expected, false));
                       }
 
                await Task.WhenAll(tasks);

                foreach (var t in tasks)
                    foreach (var kvp in t.Result)
                        result.Add(kvp.Key, kvp.Value);
            }

            logger.Verbose("--- FindUnstableSilos: done, found {0}", result.Count);

            return result;
        }

        private void PushMyStatusToNewDestinations(MultiClusterData delta)
        {
            // for quicker convergence, we push active local status information
            // immediately when we learn about a new destination

            GatewayEntry myentry;

            if (!localData.Current.Gateways.TryGetValue(this.Silo, out myentry)
                || myentry.Status != GatewayStatus.Active)
                return;

            foreach (var kvp in delta.Gateways)
            {
                var destinationcluster = kvp.Value.ClusterId;

                if (destinationcluster == clusterId) // local cluster
                {
                    if (!this.siloTasks.ContainsKey(kvp.Key) || !this.siloTasks[kvp.Key].KnowsMe)
                        PushGossipToSilo(kvp.Key, new MultiClusterData(myentry));
                }
                else // remote cluster
                {
                    if (!this.clusterTasks.ContainsKey(destinationcluster) || !this.clusterTasks[destinationcluster].KnowsMe)
                        PushGossipToCluster(destinationcluster, new MultiClusterData(myentry));
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
        private Dictionary<SiloAddress, GossipStatus> siloTasks = new Dictionary<SiloAddress, GossipStatus>();
        private Dictionary<string, GossipStatus> clusterTasks = new Dictionary<string, GossipStatus>();
        private Dictionary<IGossipChannel, GossipStatus> channelTasks = new Dictionary<IGossipChannel,GossipStatus>();
 
        // numbering for tasks (helps when analyzing logs)
        private int idCounter;

        private void PushGossipToSilo(SiloAddress destinationSilo, MultiClusterData delta)
        {
            GossipStatus status;
            if (!this.siloTasks.TryGetValue(destinationSilo, out status))
                this.siloTasks[destinationSilo] = status = new GossipStatus();

            int id = ++this.idCounter;
            logger.Verbose("-{0} PushGossipToSilo {1} {2}", id, destinationSilo, delta);

            // the task that actually pushes
            Func<Task, Task> pushAsync = async (Task prev) =>
            {
                await prev; // wait for previous push to same silo
                status.LastUse = DateTime.UtcNow;
                status.Pending = true;
                status.Address = destinationSilo;
                try
                {
                    // push to the remote system target
                    var remoteOracle = InsideRuntimeClient.Current.InternalGrainFactory.GetSystemTarget<IMultiClusterGossipService>(Constants.MultiClusterOracleId, destinationSilo);
                    await remoteOracle.Push(delta, false);

                    status.LastException = null;
                    if (delta.Gateways.ContainsKey(this.Silo))
                        status.KnowsMe = delta.Gateways[this.Silo].Status == GatewayStatus.Active;
                    logger.Verbose("-{0} PushGossipToSilo successful", id);
                }
                catch (Exception e)
                {
                    logger.Warn(ErrorCode.MultiClusterNetwork_GossipCommunicationFailure,
                        string.Format("-{0} PushGossipToSilo {1} failed", id, destinationSilo), e);
                    status.LastException = e;
                }
                status.LastUse = DateTime.UtcNow;
                status.Pending = false;
            };

            // queue the push - we are not awaiting it!
            status.Task = pushAsync(status.Task);
        }

        private void PushGossipToCluster(string destinationCluster, MultiClusterData delta)
        {
            GossipStatus status;
            if (!this.clusterTasks.TryGetValue(destinationCluster, out status))
                this.clusterTasks[destinationCluster] = status = new GossipStatus();

            int id = ++this.idCounter;
            logger.Verbose("-{0} PushGossipToCluster {1} {2}", id, destinationCluster, delta);

            // the task that actually pushes
            Func<Task, Task> pushAsync = async (Task prev) =>
            {
                await prev; // wait for previous push to same cluster
                status.LastUse = DateTime.UtcNow;
                status.Pending = true;
                try
                {
                    // pick a random gateway if we don't already have one or it is not active anymore
                    if (status.Address == null 
                        || !localData.Current.IsActiveGatewayForCluster(status.Address, destinationCluster))
                    {
                        status.Address = GetRandomClusterGateway(destinationCluster);
                    }

                    if (status.Address == null)
                        throw new OrleansException("could not notify cluster: no gateway found");

                    // push to the remote system target
                    var remoteoracle = InsideRuntimeClient.Current.InternalGrainFactory.GetSystemTarget<IMultiClusterGossipService>(Constants.MultiClusterOracleId, status.Address);
                    await remoteoracle.Push(delta, true);

                    status.LastException = null;
                    if (delta.Gateways.ContainsKey(this.Silo))
                        status.KnowsMe = delta.Gateways[this.Silo].Status == GatewayStatus.Active;
                    logger.Verbose("-{0} PushGossipToCluster successful", id);
                }
                catch (Exception e)
                {
                    logger.Warn(ErrorCode.MultiClusterNetwork_GossipCommunicationFailure,
                        string.Format("-{0} PushGossipToCluster {1} failed", id, destinationCluster), e);
                    status.LastException = e;
                    status.Address = null; // this gateway was no good... pick random gateway again next time
                }
                status.LastUse = DateTime.UtcNow;
                status.Pending = false;
            };

            // queue the push - we are not awaiting it!
            status.Task = pushAsync(status.Task);
        }

        private void PushGossipToChannel(IGossipChannel channel, MultiClusterData delta)
        {
            GossipStatus status;
            if (!this.channelTasks.TryGetValue(channel, out status))
                this.channelTasks[channel] = status = new GossipStatus();

            int id = ++this.idCounter;
            logger.Verbose("-{0} PushGossipToChannel {1} {2}", id, channel.Name, delta);

            // the task that actually pushes
            Func<Task, Task> pushAsync = async (Task prev) =>
            {
                await prev; // wait for previous push to same cluster
                status.LastUse = DateTime.UtcNow;
                status.Pending = true;
                try
                {
                    await channel.Push(delta);

                    status.LastException = null;
                    logger.Verbose("-{0} PushGossipToChannel successful, answer={1}", id, delta);
                }
                catch (Exception e)
                {
                    logger.Warn(ErrorCode.MultiClusterNetwork_GossipCommunicationFailure,
                        string.Format("-{0} PushGossipToChannel {1} failed", id, channel.Name), e);
                    status.LastException = e;
                }
                status.LastUse = DateTime.UtcNow;
                status.Pending = false;
            };

            // queue the push - we are not awaiting it!
            status.Task = pushAsync(status.Task);
        }

        private void FullGossipWithChannel(IGossipChannel channel)
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
                    var answer = await channel.PushAndPull(localData.Current);

                    // apply what we have learnt
                    var delta = localData.ApplyDataAndNotify(answer);

                    status.LastException = null;
                    logger.Verbose("-{0} FullGossipWithChannel successful", id);

                    PushMyStatusToNewDestinations(delta);
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

            if (destinationCluster != clusterId)
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
            Func<Task, Task> pushasync = async (Task prev) =>
            {
               await prev; // wait for previous gossip to same silo
               status.LastUse = DateTime.UtcNow;
               status.Pending = true;
               status.Address = destinationSilo;
               try
                {
                    var remoteOracle = InsideRuntimeClient.Current.InternalGrainFactory.GetSystemTarget<IMultiClusterGossipService>(Constants.MultiClusterOracleId, destinationSilo);
                    var answer = (MultiClusterData) await remoteOracle.PushAndPull(localData.Current);

                    // apply what we have learnt
                    var delta = localData.ApplyDataAndNotify(answer);
                    
                    status.LastException = null;
                    logger.Verbose("-{0} FullGossipWithSilo successful, answer={1}", id, answer);

                    PushMyStatusToNewDestinations(delta);
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

            // queue the push - we are not awaiting it!
            status.Task = pushasync(status.Task);
        }


        private void InjectConfiguration(ref MultiClusterData deltas)
        {
            if (this.injectedConfig == null)
                return;

            var data = new MultiClusterData(this.injectedConfig);
            this.injectedConfig = null;

            if (logger.IsVerbose)
                logger.Verbose("-InjectConfiguration {0}", data.Configuration.ToString());

            var delta = localData.ApplyDataAndNotify(data);

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

            GatewayEntry whatsThere;

            // do not update if we are reporting inactive status and entry is not already there
            if (!localData.Current.Gateways.TryGetValue(Silo, out whatsThere) && !isGateway)
                return;

            // send if status is changed, or we are active and haven't said so in a while
            if (whatsThere == null
                || whatsThere.Status != myStatus.Status
                || (myStatus.Status == GatewayStatus.Active
                      && myStatus.HeartbeatTimestamp - whatsThere.HeartbeatTimestamp > RandomizedResendActiveStatusAfter))
            {
                logger.Verbose2("-InjectLocalStatus {0}", myStatus);

                // update current data with status
                var delta = localData.ApplyDataAndNotify(new MultiClusterData(myStatus));

                if (!delta.IsEmpty)
                    deltas = deltas.Merge(delta);

                return;
            }

            return;
        }

        private void DemoteLocalGateways(IEnumerable<SiloAddress> activeGateways, ref MultiClusterData deltas)
        {
            var now = DateTime.UtcNow;

            // mark gateways as inactive if they have not recently advertised their existence,
            // and if they are not designated gateways as per membership table
            var toBeUpdated = localData.Current.Gateways
                .Where(g => g.Value.ClusterId == clusterId
                       && g.Value.Status == GatewayStatus.Active
                       && (now - g.Value.HeartbeatTimestamp > CleanupSilentGoneGatewaysAfter)
                       && !activeGateways.Contains(g.Key))
                .Select(g => new GatewayEntry()
                {
                    ClusterId = g.Value.ClusterId,
                    SiloAddress = g.Key,
                    Status = GatewayStatus.Inactive,
                    HeartbeatTimestamp = g.Value.HeartbeatTimestamp + CleanupSilentGoneGatewaysAfter,
                }).ToList();

            if (toBeUpdated.Count == 0)
                return;

            var data = new MultiClusterData(toBeUpdated);

            if (logger.IsVerbose)
                logger.Verbose("-DemoteLocalGateways {0}", data.ToString());
 
            var delta = localData.ApplyDataAndNotify(data);

            if (!delta.IsEmpty)
            {
                deltas = deltas.Merge(delta);
            }
        }
    }
}
