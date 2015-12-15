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

        private ISiloStatusOracle silostatusoracle;

        private string globalServiceId;
        private string clusterId;

        private IReadOnlyList<string> defaultMultiCluster;

        public MultiClusterOracle(SiloAddress silo, string clusterid, List<IGossipChannel> sources, GlobalConfiguration config)
            : base(Constants.MultiClusterOracleId, silo)
        {
            if (sources == null) throw new ArgumentNullException("sources");
            if (silo == null) throw new ArgumentNullException("silo");

            logger = TraceLogger.GetLogger("MultiClusterOracle");
            gossipChannels = sources;
            localData = new MultiClusterOracleData(logger);
            globalServiceId = config.GlobalServiceId;
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
            injectedconfig = config;
            PushChanges();

            // wait for the gossip channel tasks and reproduce any exceptions
            foreach (var ct in channeltasks.Values)
            {
                await ct.task;
                if (ct.lastexception != null)
                    throw ct.lastexception;
            }
        }

        private MultiClusterConfiguration injectedconfig;


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

                this.silostatusoracle = oracle;

                silostatusoracle.SubscribeToSiloStatusEvents(this);

                // startup: pull all the info from the tables, then inject default multi cluster if none found
                foreach (var ch in gossipChannels)
                    FullGossipWithChannel(ch);
                await Task.WhenAll(channeltasks.Select(kvp => kvp.Value.task));
                if (GetMultiClusterConfiguration() == null && defaultMultiCluster != null)
                {
                    injectedconfig = new MultiClusterConfiguration(DateTime.UtcNow, defaultMultiCluster, "DefaultMultiCluster");
                    logger.Info("No configuration found. Using default configuration {0} ", injectedconfig);
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

            var activelocalgateways = silostatusoracle.GetApproximateMultiClusterGateways();

            var iamgateway = activelocalgateways.Contains(Silo);

            var deltas = new MultiClusterData();

            InjectLocalStatus(iamgateway, ref deltas);

            InjectConfiguration(ref deltas);

            if (iamgateway)
                DemoteLocalGateways(activelocalgateways, ref deltas);

            if (logger.IsVerbose)
                logger.Verbose("--- PushChanges: found activegateways={0} iamgateway={1} push={2}",
                   string.Join(",", activelocalgateways), iamgateway, deltas);

            if (!deltas.IsEmpty)
            {
                // push deltas to all remote clusters 
                foreach (var x in AllClusters())
                    if (x != clusterId)
                        PushGossipToCluster(x, deltas);

                // push deltas to all local silos
                foreach (var kvp in silostatusoracle.GetApproximateSiloStatuses())
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
                FullGossipWithSilo(gateways[pick].Key, gateways[pick].Value.ClusterId);
            }
            else
            {
                var address = gossipChannels[pick - gateways.Count];
                FullGossipWithChannel(address);
            }

            // report summary of encountered communication problems in log
            var unreachableclusters = string.Join(",", clustertasks
                .Where(kvp => kvp.Value.lastexception != null)
                .Select(kvp => string.Format("{0}({1})", kvp.Key, kvp.Value.lastexception.GetType().Name)));
            if (!string.IsNullOrEmpty(unreachableclusters))
                logger.Info(ErrorCode.MultiClusterNetwork_GossipCommunicationFailure, "Gossip Communication: cannot reach  reach clusters {0}", unreachableclusters);
            var unreachablesilos = string.Join(",", silotasks
                .Where(kvp => kvp.Value.lastexception != null)
                .Select(kvp => string.Format("{0}({1})", kvp.Key, kvp.Value.lastexception.GetType().Name)));
            if (!string.IsNullOrEmpty(unreachablesilos))
                logger.Info(ErrorCode.MultiClusterNetwork_GossipCommunicationFailure, "Gossip Communication: cannot reach  reach silos {0}", unreachablesilos);
            var unreachablechannels = string.Join(",", channeltasks
                  .Where(kvp => kvp.Value.lastexception != null)
                  .Select(kvp => string.Format("{0}({1})", kvp.Key, kvp.Value.lastexception.GetType().Name)));

            if (!string.IsNullOrEmpty(unreachablechannels))
                logger.Info(ErrorCode.MultiClusterNetwork_GossipCommunicationFailure, "Gossip Communication: cannot reach channels {0}", unreachablechannels);

            // discard old status information
            RemoveStaleTaskStatusEntries(clustertasks);
            RemoveStaleTaskStatusEntries(silotasks);

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
                if ((DateTime.UtcNow - kvp.Value.lastuse).TotalMilliseconds > 2.5 * RandomizedResendActiveStatusAfter.TotalMilliseconds)
                    tbr.Add(kvp.Key);
            foreach (var k in tbr)
                d.Remove(k);
        }
      
        // called by remote nodes that push changes
        public Task Push(IMultiClusterGossipData multiclusterdata, bool forwardlocally)
        {
            logger.Verbose("--- Push: receive {0} data {1}", forwardlocally ? "remote" : "local", multiclusterdata);

            var data = (MultiClusterData)multiclusterdata;

            var delta = localData.ApplyDataAndNotify(data);

            // forward changes to all local silos
            if (forwardlocally)
                foreach (var kvp in silostatusoracle.GetApproximateSiloStatuses())
                    if (!kvp.Key.Equals(Silo) && kvp.Value == SiloStatus.Active)
                        PushGossipToSilo(kvp.Key, delta);

            PushMyStatusToNewDestinations(delta);

            logger.Verbose("--- Push: done");

            return TaskDone.Done;
        }

        // called by remote nodes' full background gossip
        public Task<IMultiClusterGossipData> PushAndPull(IMultiClusterGossipData multiclusterdata)
        {
            logger.Verbose("--- PushAndPull: gossip {0}", multiclusterdata);

            var data = (MultiClusterData)multiclusterdata;

            var delta = localData.ApplyDataAndNotify(data);

            PushMyStatusToNewDestinations(delta);

            logger.Verbose("--- PushAndPull: done, answer={0}", delta);

            return Task.FromResult((IMultiClusterGossipData)delta);
        }

        public async Task<Dictionary<SiloAddress, MultiClusterConfiguration>> StabilityCheck(MultiClusterConfiguration expected)
        {
            var localtask = FindUnstableSilos(expected, true);

            var remotetasks = new List<Task<Dictionary<SiloAddress, MultiClusterConfiguration>>>();
            foreach (var cluster in GetActiveClusters())
                if (cluster != clusterId)
                {
                    var silo = GetRandomClusterGateway(cluster);
                    if (silo == null)
                        throw new OrleansException("no gateway for cluster " + cluster);
                    var remoteoracle = InsideRuntimeClient.Current.InternalGrainFactory.GetSystemTarget<IMultiClusterGossipService>(Constants.MultiClusterOracleId, silo);
                    remotetasks.Add(remoteoracle.FindUnstableSilos(expected, true));
                }

            var result = await localtask;

            await Task.WhenAll(remotetasks);

            foreach (var t in remotetasks)
                foreach (var kvp in t.Result)
                    result.Add(kvp.Key, kvp.Value);

            return result;
        }


        public async Task<Dictionary<SiloAddress, MultiClusterConfiguration>> FindUnstableSilos(MultiClusterConfiguration expected, bool forwardlocally)
        {
            logger.Verbose("--- FindUnstableSilos: {0}, {1}", forwardlocally ? "remote" : "local", expected);

            var result = new Dictionary<SiloAddress, MultiClusterConfiguration>();

            if (! MultiClusterConfiguration.SameAs(localData.Current.Configuration, expected))
                result.Add(this.Silo, localData.Current.Configuration);

            if (forwardlocally)
            {
                var tasks = new List<Task<Dictionary<SiloAddress, MultiClusterConfiguration>>>();

                  foreach (var kvp in silostatusoracle.GetApproximateSiloStatuses())
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
                    if (!silotasks.ContainsKey(kvp.Key) || !silotasks[kvp.Key].knowsme)
                        PushGossipToSilo(kvp.Key, new MultiClusterData(myentry));
                }
                else // remote cluster
                {
                    if (!clustertasks.ContainsKey(destinationcluster) || !clustertasks[destinationcluster].knowsme)
                        PushGossipToCluster(destinationcluster, new MultiClusterData(myentry));
                }
            }
        }

        private class GossipStatus
        {
            public SiloAddress address;
            public Task task = TaskDone.Done;
            public Exception lastexception;
            public bool knowsme;
            public bool pending;
            public DateTime lastuse = DateTime.UtcNow;
        }

        // tasks for gossip
        private Dictionary<SiloAddress, GossipStatus> silotasks = new Dictionary<SiloAddress, GossipStatus>();
        private Dictionary<string, GossipStatus> clustertasks = new Dictionary<string, GossipStatus>();
        private Dictionary<IGossipChannel, GossipStatus> channeltasks = new Dictionary<IGossipChannel,GossipStatus>();
 
        // numbering for tasks (helps when analyzing logs)
        private int idcounter;


        private void PushGossipToSilo(SiloAddress destinationsilo, MultiClusterData delta)
        {
            GossipStatus status;
            if (!silotasks.TryGetValue(destinationsilo, out status))
                silotasks[destinationsilo] = status = new GossipStatus();

            int id = ++idcounter;
            logger.Verbose("-{0} PushGossipToSilo {1} {2}", id, destinationsilo, delta);

            // the task that actually pushes
            Func<Task, Task> pushasync = async (Task prev) =>
            {
                await prev; // wait for previous push to same silo
                status.lastuse = DateTime.UtcNow;
                status.pending = true;
                status.address = destinationsilo;
                try
                {
                    // push to the remote system target
                    var remoteoracle = InsideRuntimeClient.Current.InternalGrainFactory.GetSystemTarget<IMultiClusterGossipService>(Constants.MultiClusterOracleId, destinationsilo);
                    await remoteoracle.Push(delta, false);

                    status.lastexception = null;
                    if (delta.Gateways.ContainsKey(this.Silo))
                        status.knowsme = delta.Gateways[this.Silo].Status == GatewayStatus.Active;
                    logger.Verbose("-{0} PushGossipToSilo successful", id);
                }
                catch (Exception e)
                {
                    logger.Warn(ErrorCode.MultiClusterNetwork_GossipCommunicationFailure,
                        string.Format("-{0} PushGossipToSilo {1} failed", id, destinationsilo), e);
                    status.lastexception = e;
                }
                status.lastuse = DateTime.UtcNow;
                status.pending = false;
            };

            // queue the push - we are not awaiting it!
            status.task = pushasync(status.task);
        }

     

        private void PushGossipToCluster(string destinationcluster, MultiClusterData delta)
        {
            GossipStatus status;
            if (!clustertasks.TryGetValue(destinationcluster, out status))
                clustertasks[destinationcluster] = status = new GossipStatus();

            int id = ++idcounter;
            logger.Verbose("-{0} PushGossipToCluster {1} {2}", id, destinationcluster, delta);

            // the task that actually pushes
            Func<Task, Task> pushasync = async (Task prev) =>
            {
                await prev; // wait for previous push to same cluster
                status.lastuse = DateTime.UtcNow;
                status.pending = true;
                try
                {
                    // pick a random gateway if we don't already have one or it is not active anymore
                    if (status.address == null 
                        || !localData.Current.IsActiveGatewayForCluster(status.address, destinationcluster))
                    {
                        status.address = GetRandomClusterGateway(destinationcluster);
                    }

                    if (status.address == null)
                        throw new OrleansException("could not notify cluster: no gateway found");

                    // push to the remote system target
                    var remoteoracle = InsideRuntimeClient.Current.InternalGrainFactory.GetSystemTarget<IMultiClusterGossipService>(Constants.MultiClusterOracleId, status.address);
                    await remoteoracle.Push(delta, true);

                    status.lastexception = null;
                    if (delta.Gateways.ContainsKey(this.Silo))
                        status.knowsme = delta.Gateways[this.Silo].Status == GatewayStatus.Active;
                    logger.Verbose("-{0} PushGossipToCluster successful", id);
                }
                catch (Exception e)
                {
                    logger.Warn(ErrorCode.MultiClusterNetwork_GossipCommunicationFailure,
                        string.Format("-{0} PushGossipToCluster {1} failed", id, destinationcluster), e);
                    status.lastexception = e;
                    status.address = null; // this gateway was no good... pick random gateway again next time
                }
                status.lastuse = DateTime.UtcNow;
                status.pending = false;
            };

            // queue the push - we are not awaiting it!
            status.task = pushasync(status.task);
        }

        private void PushGossipToChannel(IGossipChannel channel, MultiClusterData delta)
        {
            GossipStatus status;
            if (!channeltasks.TryGetValue(channel, out status))
                channeltasks[channel] = status = new GossipStatus();

            int id = ++idcounter;
            logger.Verbose("-{0} PushGossipToChannel {1} {2}", id, channel.Name, delta);

            // the task that actually pushes
            Func<Task, Task> pushasync = async (Task prev) =>
            {
                await prev; // wait for previous push to same cluster
                status.lastuse = DateTime.UtcNow;
                status.pending = true;
                try
                {
                    await channel.Push(delta);

                    status.lastexception = null;
                    logger.Verbose("-{0} PushGossipToChannel successful, answer={1}", id, delta);
                }
                catch (Exception e)
                {
                    logger.Warn(ErrorCode.MultiClusterNetwork_GossipCommunicationFailure,
                        string.Format("-{0} PushGossipToChannel {1} failed", id, channel.Name), e);
                    status.lastexception = e;
                }
                status.lastuse = DateTime.UtcNow;
                status.pending = false;
            };

            // queue the push - we are not awaiting it!
            status.task = pushasync(status.task);
        }

        private void FullGossipWithChannel(IGossipChannel channel)
        {
            GossipStatus status;
            if (!channeltasks.TryGetValue(channel, out status))
                channeltasks[channel] = status = new GossipStatus();

            int id = ++idcounter;
            logger.Verbose("-{0} FullGossipWithChannel {1}", id, channel.Name);

            // the task that actually gossips
            Func<Task, Task> gossipasync = async (Task prev) =>
            {
                await prev; // wait for previous gossip with same channel
                status.lastuse = DateTime.UtcNow;
                status.pending = true;
                try
                {
                    var answer = await channel.PushAndPull(localData.Current);

                    // apply what we have learnt
                    var delta = localData.ApplyDataAndNotify(answer);

                    status.lastexception = null;
                    logger.Verbose("-{0} FullGossipWithChannel successful", id);

                    PushMyStatusToNewDestinations(delta);
                }
                catch (Exception e)
                {
                    logger.Warn(ErrorCode.MultiClusterNetwork_GossipCommunicationFailure,
                        string.Format("-{0} FullGossipWithChannel {1} failed", id, channel.Name), e);
                    status.lastexception = e;
                }
                status.lastuse = DateTime.UtcNow;
                status.pending = false;
            };

            // queue the gossip - we are not awaiting it!
            status.task = gossipasync(status.task);
        }

        private void FullGossipWithSilo(SiloAddress destinationsilo, string destinationcluster)
        {
            GossipStatus status;

            if (destinationcluster != clusterId)
            {
                if (!clustertasks.TryGetValue(destinationcluster, out status))
                    clustertasks[destinationcluster] = status = new GossipStatus();
            }
            else
            {
                if (!silotasks.TryGetValue(destinationsilo, out status))
                    silotasks[destinationsilo] = status = new GossipStatus();
            }

            int id = ++idcounter;
            logger.Verbose("-{0} FullGossipWithSilo {1} in cluster {2}", id, destinationsilo, destinationcluster);

            // the task that actually gossips
            Func<Task, Task> pushasync = async (Task prev) =>
            {
               await prev; // wait for previous gossip to same silo
               status.lastuse = DateTime.UtcNow;
               status.pending = true;
               status.address = destinationsilo;
               try
                {
                    var remoteoracle = InsideRuntimeClient.Current.InternalGrainFactory.GetSystemTarget<IMultiClusterGossipService>(Constants.MultiClusterOracleId, destinationsilo);
                    var answer = (MultiClusterData) await remoteoracle.PushAndPull(localData.Current);

                    // apply what we have learnt
                    var delta = localData.ApplyDataAndNotify(answer);
                    
                    status.lastexception = null;
                    logger.Verbose("-{0} FullGossipWithSilo successful, answer={1}", id, answer);

                    PushMyStatusToNewDestinations(delta);
                }
                catch (Exception e)
                {
                    logger.Warn(ErrorCode.MultiClusterNetwork_GossipCommunicationFailure,
                        string.Format("-{0} FullGossipWithSilo {1} in cluster {2} failed", id, destinationsilo, destinationcluster), e);
                    status.lastexception = e;
                }
                status.lastuse = DateTime.UtcNow;
                status.pending = false;
            };

            // queue the push - we are not awaiting it!
            status.task = pushasync(status.task);
        }


        private void InjectConfiguration(ref MultiClusterData deltas)
        {
            if (injectedconfig == null)
                return;

            var data = new MultiClusterData(injectedconfig);
            injectedconfig = null;

            if (logger.IsVerbose)
                logger.Verbose("-InjectConfiguration {0}", data.Configuration.ToString());

            var delta = localData.ApplyDataAndNotify(data);

            if (!delta.IsEmpty)
                deltas = deltas.Merge(delta);
        }


        private void InjectLocalStatus(bool isgateway, ref MultiClusterData deltas)
        {
            var mystatus = new GatewayEntry()
            {
                ClusterId = clusterId,
                SiloAddress = Silo,
                Status = isgateway ? GatewayStatus.Active : GatewayStatus.Inactive,
                HeartbeatTimestamp = DateTime.UtcNow,
            };

            GatewayEntry whatsthere;

            // do not update if we are reporting inactive status and entry is not already there
            if (!localData.Current.Gateways.TryGetValue(Silo, out whatsthere) && !isgateway)
                return;

            // send if status is changed, or we are active and haven't said so in a while
            if (whatsthere == null
                || whatsthere.Status != mystatus.Status
                || (mystatus.Status == GatewayStatus.Active
                      && mystatus.HeartbeatTimestamp - whatsthere.HeartbeatTimestamp > RandomizedResendActiveStatusAfter))
            {
                logger.Verbose2("-InjectLocalStatus {0}", mystatus);

                // update current data with status
                var delta = localData.ApplyDataAndNotify(new MultiClusterData(mystatus));

                if (!delta.IsEmpty)
                    deltas = deltas.Merge(delta);

                return;
            }

            return;
        }

        private void DemoteLocalGateways(IEnumerable<SiloAddress> activegateways, ref MultiClusterData deltas)
        {
            var now = DateTime.UtcNow;

            // mark gateways as inactive if they have not recently advertised their existence,
            // and if they are not designated gateways as per membership table
            var tobeupdated = localData.Current.Gateways
                .Where(g => g.Value.ClusterId == clusterId
                       && g.Value.Status == GatewayStatus.Active
                       && (now - g.Value.HeartbeatTimestamp > CleanupSilentGoneGatewaysAfter)
                       && !activegateways.Contains(g.Key))
                .Select(g => new GatewayEntry()
                {
                    ClusterId = g.Value.ClusterId,
                    SiloAddress = g.Key,
                    Status = GatewayStatus.Inactive,
                    HeartbeatTimestamp = g.Value.HeartbeatTimestamp + CleanupSilentGoneGatewaysAfter,
                });

            if (tobeupdated.Count() == 0)
                return;

            var data = new MultiClusterData(tobeupdated);

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
