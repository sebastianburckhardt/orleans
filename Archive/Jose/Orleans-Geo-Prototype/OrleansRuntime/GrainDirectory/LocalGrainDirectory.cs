using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Orleans.Scheduler;
using Orleans.Counters;

using Orleans.Runtime.Scheduler;

namespace Orleans.Runtime.GrainDirectory
{
    internal class LocalGrainDirectory : MarshalByRefObject, ILocalGrainDirectory, ISiloStatusListener, ISiloShutdownParticipant
    {
        /// <summary>
        /// list of silo members sorted by the hash value of their address
        /// </summary>
        private readonly List<SiloAddress> membershipRingList;
        private readonly HashSet<SiloAddress> membershipCache;

        
        private readonly AsynchAgent maintainer;

        private readonly Logger log;
        private readonly SiloAddress seed;
        private Action tryFinishShutdown;
        internal ISiloStatusOracle membership;

        // TODO: move these constants into an apropriate place
        // number of retries to redirect a wrong request (which can get wrong beacuse of membership changes)
        internal const int NUM_RETRIES = 3;
        // if no replication is needed, set this parameter to 0
        internal int ReplicationFactor { get; private set; }

        protected SiloAddress Seed { get { return seed; } }

        internal bool running;

        internal SiloAddress MyAddress { get; private set; }

        internal IGrainDirectoryCache<List<Tuple<SiloAddress, ActivationId>>> DirectoryCache { get; private set; }
        internal GrainDirectoryPartition DirectoryPartition { get; private set; }

        public RemoteGrainDirectory RemGrainDirectory { get; private set; }
        public RemoteGrainDirectory CacheValidator { get; private set; }

        private TaskCompletionSource<bool> stopPreparationResolver;
        public Task StopPreparationCompletion { get { return stopPreparationResolver.Task; } }

        internal OrleansTaskScheduler Scheduler { get; private set; }

        internal GrainDirectoryReplicationAgent ReplicationAgent { get; private set; }

        internal ISiloStatusListener CatalogSiloStatusListener { get; set; }

        private readonly CounterStatistic localLookups;
        private readonly CounterStatistic localSuccesses;
        private readonly CounterStatistic fullLookups;
        internal readonly CounterStatistic remoteLookupsSent;
        internal readonly CounterStatistic remoteLookupsReceived;
        internal readonly CounterStatistic localDirectoryLookups;
        internal readonly CounterStatistic localDirectorySuccesses;
        private readonly CounterStatistic cacheLookups;
        private readonly CounterStatistic cacheSuccesses;
        internal readonly CounterStatistic cacheValidationsSent;
        internal readonly CounterStatistic cacheValidationsReceived;

        private readonly CounterStatistic registrationsIssued;
        internal readonly CounterStatistic registrationsLocal;
        internal readonly CounterStatistic registrationsRemoteSent;
        internal readonly CounterStatistic registrationsRemoteReceived;
        private readonly CounterStatistic registrationsSingleActIssued;
        internal readonly CounterStatistic registrationsSingleActLocal;
        internal readonly CounterStatistic registrationsSingleActRemoteSent;
        internal readonly CounterStatistic registrationsSingleActRemoteReceived;

        private readonly CounterStatistic unregistrationsIssued;
        internal readonly CounterStatistic unregistrationsLocal;
        internal readonly CounterStatistic unregistrationsRemoteSent;
        internal readonly CounterStatistic unregistrationsRemoteReceived;

        private readonly CounterStatistic unregistrationsManyIssued;
        internal readonly CounterStatistic unregistrationsManyRemoteSent;
        internal readonly CounterStatistic unregistrationsManyRemoteReceived;

        private readonly IntValueStatistic directoryPartitionCount;

        public LocalGrainDirectory(Silo silo)
        {
            log = Logger.GetLogger("Orleans.GrainDirectory.LocalGrainDirectory");

            MyAddress = silo.LocalMessageCenter.MyAddress;
            Scheduler = silo.LocalScheduler;
            membershipRingList = new List<SiloAddress>();
            membershipCache = new HashSet<SiloAddress>();

            silo.OrleansConfig.OnConfigChange("Globals/Caching", () =>
                { lock (membershipCache) { DirectoryCache = GrainDirectoryCacheFactory<List<Tuple<SiloAddress, ActivationId>>>.CreateGrainDirectoryCache(silo.GlobalConfig); } });
            maintainer = GrainDirectoryCacheFactory<List<Tuple<SiloAddress, ActivationId>>>.CreateGrainDirectoryCacheMaintainer(this, DirectoryCache);

            if (silo.GlobalConfig.SeedNodes.Count > 0)
            {
                seed = silo.GlobalConfig.SeedNodes.Contains(MyAddress.Endpoint) ? MyAddress : SiloAddress.New(silo.GlobalConfig.SeedNodes[0], 0);
            }
            ReplicationFactor = silo.GlobalConfig.DirectoryReplicationFactor;

            stopPreparationResolver = new TaskCompletionSource<bool>();
            DirectoryPartition = new GrainDirectoryPartition();
            ReplicationAgent = new GrainDirectoryReplicationAgent(this, silo.GlobalConfig);

            RemGrainDirectory = new RemoteGrainDirectory(this, Constants.DirectoryServiceId);
            CacheValidator = new RemoteGrainDirectory(this, Constants.DirectoryCacheValidatorId);

            // add myself to the list of members
            AddServer(MyAddress);

            Func<SiloAddress, string> siloAddressPrint = (SiloAddress addr) => 
            {
                return String.Format("{0}/{1:X}", addr.ToLongString(), addr.GetConsistentHashCode());
            };
            
            localLookups = CounterStatistic.FindOrCreate(StatNames.STAT_DIRECTORY_LOOKUPS_LOCAL_ISSUED);
            localSuccesses = CounterStatistic.FindOrCreate(StatNames.STAT_DIRECTORY_LOOKUPS_LOCAL_SUCCESSES);
            fullLookups = CounterStatistic.FindOrCreate(StatNames.STAT_DIRECTORY_LOOKUPS_FULL_ISSUED);

            remoteLookupsSent = CounterStatistic.FindOrCreate(StatNames.STAT_DIRECTORY_LOOKUPS_REMOTE_SENT);
            remoteLookupsReceived = CounterStatistic.FindOrCreate(StatNames.STAT_DIRECTORY_LOOKUPS_REMOTE_RECEIVED);

            localDirectoryLookups = CounterStatistic.FindOrCreate(StatNames.STAT_DIRECTORY_LOOKUPS_LOCALDIRECTORY_ISSUED);
            localDirectorySuccesses = CounterStatistic.FindOrCreate(StatNames.STAT_DIRECTORY_LOOKUPS_LOCALDIRECTORY_SUCCESSES);

            cacheLookups = CounterStatistic.FindOrCreate(StatNames.STAT_DIRECTORY_LOOKUPS_CACHE_ISSUED);
            cacheSuccesses = CounterStatistic.FindOrCreate(StatNames.STAT_DIRECTORY_LOOKUPS_CACHE_SUCCESSES);
            StringValueStatistic.FindOrCreate(StatNames.STAT_DIRECTORY_LOOKUPS_CACHE_HITRATIO, () =>
                {
                    long delta1 = 0, delta2 = 0;
                    long curr1 = cacheSuccesses.GetCurrentValueAndDelta(out delta1);
                    long curr2 = cacheLookups.GetCurrentValueAndDelta(out delta2);
                    return String.Format("{0}, Delta={1}", 
                        (curr2 != 0 ? (float)curr1 / (float)curr2 : 0)
                        ,(delta2 !=0 ? (float)delta1 / (float)delta2 : 0));
                });

            cacheValidationsSent = CounterStatistic.FindOrCreate(StatNames.STAT_DIRECTORY_VALIDATIONS_CACHE_SENT);
            cacheValidationsReceived = CounterStatistic.FindOrCreate(StatNames.STAT_DIRECTORY_VALIDATIONS_CACHE_RECEIVED);

            registrationsIssued = CounterStatistic.FindOrCreate(StatNames.STAT_DIRECTORY_REGISTRATIONS_ISSUED);
            registrationsLocal = CounterStatistic.FindOrCreate(StatNames.STAT_DIRECTORY_REGISTRATIONS_LOCAL);
            registrationsRemoteSent = CounterStatistic.FindOrCreate(StatNames.STAT_DIRECTORY_REGISTRATIONS_REMOTE_SENT);
            registrationsRemoteReceived = CounterStatistic.FindOrCreate(StatNames.STAT_DIRECTORY_REGISTRATIONS_REMOTE_RECEIVED);
            registrationsSingleActIssued = CounterStatistic.FindOrCreate(StatNames.STAT_DIRECTORY_REGISTRATIONS_SINGLE_ACT_ISSUED);
            registrationsSingleActLocal = CounterStatistic.FindOrCreate(StatNames.STAT_DIRECTORY_REGISTRATIONS_SINGLE_ACT_LOCAL);
            registrationsSingleActRemoteSent = CounterStatistic.FindOrCreate(StatNames.STAT_DIRECTORY_REGISTRATIONS_SINGLE_ACT_REMOTE_SENT);
            registrationsSingleActRemoteReceived = CounterStatistic.FindOrCreate(StatNames.STAT_DIRECTORY_REGISTRATIONS_SINGLE_ACT_REMOTE_RECEIVED);
            unregistrationsIssued = CounterStatistic.FindOrCreate(StatNames.STAT_DIRECTORY_UNREGISTRATIONS_ISSUED);
            unregistrationsLocal = CounterStatistic.FindOrCreate(StatNames.STAT_DIRECTORY_UNREGISTRATIONS_LOCAL);
            unregistrationsRemoteSent = CounterStatistic.FindOrCreate(StatNames.STAT_DIRECTORY_UNREGISTRATIONS_REMOTE_SENT);
            unregistrationsRemoteReceived = CounterStatistic.FindOrCreate(StatNames.STAT_DIRECTORY_UNREGISTRATIONS_REMOTE_RECEIVED);
            unregistrationsManyIssued = CounterStatistic.FindOrCreate(StatNames.STAT_DIRECTORY_UNREGISTRATIONS_MANY_ISSUED);
            unregistrationsManyRemoteSent = CounterStatistic.FindOrCreate(StatNames.STAT_DIRECTORY_UNREGISTRATIONS_MANY_REMOTE_SENT);
            unregistrationsManyRemoteReceived = CounterStatistic.FindOrCreate(StatNames.STAT_DIRECTORY_UNREGISTRATIONS_MANY_REMOTE_RECEIVED);

            directoryPartitionCount = IntValueStatistic.FindOrCreate(StatNames.STAT_DIRECTORY_PARTITION_SIZE, () => DirectoryPartition.Count);
            IntValueStatistic.FindOrCreate(StatNames.STAT_DIRECTORY_RING_MYPORTION_RINGDISTANCE, () => RingDistanceToSuccessor());
            FloatValueStatistic.FindOrCreate(StatNames.STAT_DIRECTORY_RING_MYPORTION_RINGPERCENTAGE, () => (((float)RingDistanceToSuccessor()) / ((float)(int.MaxValue * 2L))) * 100);
            FloatValueStatistic.FindOrCreate(StatNames.STAT_DIRECTORY_RING_MYPORTION_AVERAGERINGPERCENTAGE, () => membershipRingList.Count == 0 ? 0 : ((float)100 / (float)membershipRingList.Count));
            IntValueStatistic.FindOrCreate(StatNames.STAT_DIRECTORY_RING_RINGSIZE, () => membershipRingList.Count);
            StringValueStatistic.FindOrCreate(StatNames.STAT_DIRECTORY_RING, () =>
                {
                    lock (membershipCache)
                    {
                        return Utils.IEnumerableToString(membershipRingList, siloAddressPrint);
                    }
                });
            StringValueStatistic.FindOrCreate(StatNames.STAT_DIRECTORY_RING_PREDECESSORS, () => Utils.IEnumerableToString(FindPredecessors(MyAddress, Math.Max(ReplicationFactor, 1)), siloAddressPrint));
            StringValueStatistic.FindOrCreate(StatNames.STAT_DIRECTORY_RING_SUCCESSORS, () => Utils.IEnumerableToString(FindSuccessors(MyAddress, Math.Max(ReplicationFactor, 1)), siloAddressPrint));
        }

        public void Start()
        {
            running = true;
            if (maintainer != null)
            {
                maintainer.Start();
            }
            if (ReplicationFactor > 0)
            {
                ReplicationAgent.Start();
            }
        }

        // Note that this implementation stops processing directory change requests (Register, Unregister, etc.) when the Stop event is raised. 
        // This means that there may be a short period during which no silo believes that it is the owner of directory information for a set of 
        // grains (for update purposes), which could cause application requests that require a new activation to be created to time out. 
        // The alternative would be to allow the silo to process requests after it has handed off its partition, in which case those changes 
        // would receive successful responses but would not be reflected in the eventual state of the directory. 
        // It's easy to change this, if we think the trade-off is better the other way.
        public void Stop(bool doOnStopReplication)
        {
            // This will cause remote write requests to be forwarded to the silo that will become the new owner.
            // Requests might bounce back and forth for a while as membership stabilizes, but they will either be served by the
            // new owner of the grain, or will wind up failing. In either case, we avoid requests succeeding at this silo after we've
            // begun stopping, which could cause them to not get handed off to the new owner.
            running = false;

            if (doOnStopReplication)
            {
                ReplicationAgent.ProcessSiloStoppingEvent();
            }
            else
            {
                MarkStopPreparationCompleted();
            }
            if (maintainer != null)
            {
                maintainer.Stop();
            }
            if (ReplicationFactor > 0)
            {
                ReplicationAgent.Stop(); // Note that this only stops the agent thread for the replication agent, and so doesn't interrupt the silo stop processing
            }
            DirectoryCache.Clear();
        }

        internal void MarkStopPreparationCompleted()
        {
            stopPreparationResolver.TrySetResult(true);
        }

        internal void MarkStopPreparationFailed(Exception ex)
        {
            stopPreparationResolver.TrySetException(ex);
        }

        #region Handling the membership
        protected void AddServer(SiloAddress silo)
        {
            lock (membershipCache)
            {
                if (membershipCache.Contains(silo))
                {
                    // we have already cached this silo
                    return;
                }

                membershipCache.Add(silo);

                // insert new silo in the sorted order
                long hash = silo.GetConsistentHashCode();

                // Find the last silo with hash smaller than the new silo, and insert the latter after (this is why we have +1 here) the former.
                // Notice that FindLastIndex might return -1 if this should be the first silo in the list, but then
                // 'index' will get 0, as needed.
                int index = membershipRingList.FindLastIndex(siloAddr => siloAddr.GetConsistentHashCode() < hash) + 1;
                membershipRingList.Insert(index, silo);

                ReplicationAgent.ProcessSiloAddEvent(silo);

                if (log.IsVerbose) log.Verbose("Silo {0} added silo {1}", MyAddress, silo);
            }
        }

        protected void RemoveServer(SiloAddress silo, SiloStatus status)
        {
            lock (membershipCache)
            {
                if (!membershipCache.Contains(silo))
                {
                    // we have already removed this silo
                    return;
                }

                if (CatalogSiloStatusListener != null)
                {
                    try
                    {
                        // only call SiloStatusChangeNotification once on the catalog and the order is important: call BEFORE updating membershipRingList.
                        CatalogSiloStatusListener.SiloStatusChangeNotification(silo, status);
                    }
                    catch (Exception exc)
                    {
                        log.Error(ErrorCode.Directory_SiloStatusChangeNotification_Exception,
                            String.Format("CatalogSiloStatusListener.SiloStatusChangeNotification has thrown an exception when notified about removed silo {0}.", silo.ToStringWithHashCode()), exc);
                    }
                }

                // the call order is important
                ReplicationAgent.ProcessSiloRemoveEvent(silo);

                membershipCache.Remove(silo);

                membershipRingList.Remove(silo);

                AdjustLocalDirectory(silo);
                AdjustLocalCache(silo);

                if (log.IsVerbose) log.Verbose("Silo {0} removed silo {1}", MyAddress, silo);
            }
        }

        /// adjust local directory following the removal of a silo by droping all activations located on the removed silo
        protected void AdjustLocalDirectory(SiloAddress removedSilo)
        {
            var activationsToRemove = (from pair in DirectoryPartition.GetItems()
                                       from pair2 in pair.Value.Instances.Where(pair3 => pair3.Value.SiloAddress.Equals(removedSilo))
                                       select new Tuple<GrainId, ActivationId>(pair.Key, pair2.Key)).ToList();
            // drop all records of activations located on the removed silo
            foreach (var activation in activationsToRemove)
            {
                DirectoryPartition.RemoveActivation(activation.Item1, activation.Item2, true);
            }
        }

        /// Adjust local cache following the removal of a silo by droping:
        /// 1) entries that point to activations located on the removed silo 
        /// 2) entries for grains that are now owned by this silo (me)
        /// 3) entries for grains that were owned by this removed silo - we currently do NOT do that.
        ///     If we did 3, we need to do that BEFORE we change the membershipRingList (based on old mbr).
        ///     We don't do that since first cache refresh handles that. 
        ///     Second, since MBR events are not guaranteed to be ordered, we may remove a cache entry that does not really point to a failed silo.
        ///     To do that properly, we need to store for each cache entry who was the directory owner that registered this activation (the original partition owner). 
        protected void AdjustLocalCache(SiloAddress removedSilo)
        {
            // remove all records of activations located on the removed silo
            foreach (Tuple<GrainId, List<Tuple<SiloAddress, ActivationId>>, int> tuple in DirectoryCache.KeyValues)
            {
                // 2) remove entries owned by me (they should be retrieved from my directory partition)
                if (MyAddress.Equals(CalculateTargetSilo(tuple.Item1)))
                {
                    DirectoryCache.Remove(tuple.Item1);
                }

                // 1) remove entries that point to activations located on the removed silo
                if (tuple.Item2.RemoveAll(tuple2 => tuple2.Item1.Equals(removedSilo)) > 0)
                {
                    if (tuple.Item2.Count > 0)
                    {
                        DirectoryCache.AddOrUpdate(tuple.Item1, tuple.Item2, tuple.Item3);
                    }
                    else
                    {
                        DirectoryCache.Remove(tuple.Item1);
                    }
                }
            }
        }

        internal List<SiloAddress> FindPredecessors(SiloAddress silo, int count)
        {
            lock (membershipCache)
            {
                int index = membershipRingList.FindIndex(elem => elem.Equals(silo));
                if (index == -1)
                {
                    log.Warn(ErrorCode.Runtime_Error_100201, "Got request to find predecessors of silo " + silo + ", which is not in the list of members");
                    return null;
                }

                var result = new List<SiloAddress>();
                int numMembers = membershipRingList.Count;
                for (int i = index - 1; ((i + numMembers) % numMembers) != index && result.Count < count; i--)
                {
                    result.Add(membershipRingList[(i + numMembers) % numMembers]);
                }

                return result;
            }
        }

        internal List<SiloAddress> FindSuccessors(SiloAddress silo, int count)
        {
            lock (membershipCache)
            {
                int index = membershipRingList.FindIndex(elem => elem.Equals(silo));
                if (index == -1)
                {
                    log.Warn(ErrorCode.Runtime_Error_100203, "Got request to find successors of silo " + silo + ", which is not in the list of members");
                    return null;
                }

                var result = new List<SiloAddress>();
                int numMembers = membershipRingList.Count;
                for (int i = index + 1; i % numMembers != index && result.Count < count; i++)
                {
                    result.Add(membershipRingList[i % numMembers]);
                }

                return result;
            }
        }

        public void SiloStatusChangeNotification(SiloAddress updatedSilo, SiloStatus status)
        {
            // This silo's status has changed
            if (updatedSilo == MyAddress)
            {
                if (status == SiloStatus.Stopping || status.Equals(SiloStatus.ShuttingDown))
                {
                    // QueueAction up the "Stop" to run on a system turn
                    Scheduler.QueueAction(() => Stop(true), null).Ignore();
                }
                else if (status == SiloStatus.Dead)
                {
                    // QueueAction up the "Stop" to run on a system turn
                    Scheduler.QueueAction(() => Stop(false), null).Ignore();
                }
            }
            else // Status change for some other silo
            {
                if (status.Equals(SiloStatus.Dead) || status.Equals(SiloStatus.ShuttingDown) || status.Equals(SiloStatus.Stopping))
                {
                    // QueueAction up the "Remove" to run on a system turn
                    Scheduler.QueueAction(() => RemoveServer(updatedSilo, status), null).Ignore();
                }
                else if (status.Equals(SiloStatus.Active))      // do not do anything with SiloStatus.Starting -- wait until it actually becomes active
                {
                    // QueueAction up the "Remove" to run on a system turn
                    Scheduler.QueueAction(() => AddServer(updatedSilo), null).Ignore();
                }
            }
        }

        private bool IsValidSilo(SiloAddress silo)
        {
            if (membership == null)
            {
                membership = Silo.CurrentSilo.LocalSiloStatusOracle;
            }
            return membership.IsValidSilo(silo);
        }

        #endregion

        /// <summary>
        /// Finds the silo that owns the directory information for the given grain ID.
        /// This routine will always return a non-null silo address unless the excludeThisSiloIfStopping parameter is true,
        /// this is the only silo known, and this silo is stopping.
        /// </summary>
        /// <param name="grain"></param>
        /// <param name="excludeThisSiloIfStopping"></param>
        /// <returns></returns>
        public SiloAddress CalculateTargetSilo(GrainId grain, bool excludeThisSiloIfStopping = true)
        {
            // give a special treatment for special grains
            if (grain.IsSystemTarget)
            {
                if (log.IsVerbose2) log.Verbose2("Silo {0} looked for a system target {1}, returned {2}", MyAddress, grain, MyAddress);
                // every silo owns its system targets
                return MyAddress;
            }

            if (Constants.SystemMembershipTableId.Equals(grain)
                || Constants.SystemReminderTableId.Equals(grain))
            {
                if (Seed == null)
                {
                    string grainName = "";
                    if (!Constants.TryGetSystemGrainName(grain, out grainName))
                    {
                        grainName = "Membership/Reminder table grain";
                    }
                    string errorMsg = grainName + " cannot run without Seed node - please check your silo configuration file and make sure it specifies a SeedNode element. " +
                        " Alternatively, you may want to use AzureTable for LivenessType and for Reminders.";
                    throw new ArgumentException(errorMsg, "grain = " + grain.ToString());
                }
                // Directory info for the membership table grain has to be located on the primary (seed) node, for bootstrapping
                if (log.IsVerbose2) log.Verbose2("Silo {0} looked for a special grain {1}, returned {2}", MyAddress, grain, Seed);
                return Seed;
            }

            SiloAddress s;
            int hash = grain.GetUniformHashCode();

            lock (membershipCache)
            {
                if (membershipRingList.Count == 0)
                {
                    // If the membership ring is empty, then we're the owner by default unless we're stopping.
                    if (excludeThisSiloIfStopping && !running)
                    {
                        return null;
                    }
                    return MyAddress;
                }

                // excludeMySelf from being a TargetSilo if we're not running and the excludeThisSIloIfStopping flag is true. see the comment in the Stop method.
                bool excludeMySelf = !running && excludeThisSiloIfStopping; 

                // need to implement a binary search, but for now simply traverse the list of silos sorted by their hashes
                s = membershipRingList.FindLast(siloAddr => (siloAddr.GetConsistentHashCode() <= hash) &&
                                    (!siloAddr.Equals(MyAddress) || !excludeMySelf));
                if (s == null)
                {
                    // If not found in the traversal, last silo will do (we are on a ring).
                    // We checked above to make sure that the list isn't empty, so this should always be safe.
                    s = membershipRingList[membershipRingList.Count - 1];
                    // Make sure it's not us...
                    if (s.Equals(MyAddress) && excludeMySelf)
                    {
                        if (membershipRingList.Count > 1)
                        {
                            s = membershipRingList[membershipRingList.Count - 2];
                        }
                        else
                        {
                            s = null;
                        }
                    }
                }
            }
            if (log.IsVerbose2) log.Verbose2("Silo {0} calculated directory partition owner silo {1} for grain {2}: {3} --> {4}", MyAddress, s, grain, hash, s.GetConsistentHashCode());
            return s;
        }

        #region Implementation of ILocalGrainDirectory

        /// <summary>
        /// Registers a new activation, in single activation mode, with the directory service.
        /// If there is already an activation registered for this grain, then the new activation will
        /// not be registered and the address of the existing activation will be returned.
        /// Otherwise, the passed-in address will be returned.
        /// <para>This method must be called from a scheduler thread.</para>
        /// </summary>
        /// <param name="address">The address of the potential new activation.</param>
        /// <returns>The address registered for the grain's single activation.</returns>
        public Task<ActivationAddress> RegisterSingleActivationAsync(ActivationAddress address)
        {
            registrationsSingleActIssued.Increment();
            SiloAddress owner = CalculateTargetSilo(address.Grain);
            if (owner == null)
            {
                // We don't know about any other silos, and we're stopping, so throw
                throw new InvalidOperationException("Grain directory is stopping");
            }
            if (owner.Equals(MyAddress))
            {
                registrationsSingleActLocal.Increment();
                // if I am the owner, store the new activation locally
                return Task.FromResult(DirectoryPartition.AddSingleActivation(address.Grain, address.Activation, address.Silo));
            }
            else
            {
                registrationsSingleActRemoteSent.Increment();
                // otherwise, notify the owner
                return GetDirectoryReference(owner).RegisterSingleActivation(address, NUM_RETRIES);
            }
        }

        public Task RegisterAsync(ActivationAddress address)
        {
            registrationsIssued.Increment();
            SiloAddress owner = CalculateTargetSilo(address.Grain);
            if (owner == null)
            {
                // We don't know about any other silos, and we're stopping, so throw
                throw new InvalidOperationException("Grain directory is stopping");
            }
            if (owner.Equals(MyAddress))
            {
                registrationsLocal.Increment();
                // if I am the owner, store the new activation locally
                DirectoryPartition.AddActivation(address.Grain, address.Activation, address.Silo);
                return TaskDone.Done;
            }
            else
            {
                 registrationsRemoteSent.Increment();
                // otherwise, notify the owner
                 return GetDirectoryReference(owner).Register(address, NUM_RETRIES);
            }
        }

        public Task UnregisterAsync(ActivationAddress addr)
        {
            return UnregisterAsyncImpl(addr, true);
        }

        public Task UnregisterConditionallyAsync(ActivationAddress addr)
        {
            // This is a no-op if the lazy registration delay is zero or negative
            if (Silo.CurrentSilo.OrleansConfig.Globals.DirectoryLazyDeregistrationDelay <= TimeSpan.Zero)
            {
                return TaskDone.Done;
            }
            return UnregisterAsyncImpl(addr, false);
        }

        private Task UnregisterAsyncImpl(ActivationAddress addr, bool force)
        {
            unregistrationsIssued.Increment();
            SiloAddress owner = CalculateTargetSilo(addr.Grain);
            if (owner == null)
            {
                // We don't know about any other silos, and we're stopping, so throw
                throw new InvalidOperationException("Grain directory is stopping");
            }

            if (log.IsVerbose) log.Verbose("Silo {0} is going to unregister grain {1}-->{2} ({3}-->{4})", MyAddress, addr.Grain, owner, addr.Grain.GetUniformHashCode(), owner.GetConsistentHashCode());

            InvalidateCacheEntry(addr.Grain);
            if (owner.Equals(MyAddress))
            {
                unregistrationsLocal.Increment();
                // if I am the owner, remove the old activation locally
                DirectoryPartition.RemoveActivation(addr.Grain, addr.Activation, force);
                return TaskDone.Done;
            }
            else
            {
                unregistrationsRemoteSent.Increment();
                // otherwise, notify the owner
                return GetDirectoryReference(owner).Unregister(addr, force, NUM_RETRIES);
            }
        }

        public Task UnregisterManyAsync(List<ActivationAddress> addresses)
        {
            unregistrationsManyIssued.Increment();
            return Task.WhenAll(
                addresses.GroupBy(a => CalculateTargetSilo(a.Grain))
                    .Select(g =>
                    {
                        if (g.Key == null)
                        {
                            // We don't know about any other silos, and we're stopping, so throw
                            throw new InvalidOperationException("Grain directory is stopping");
                        }
                        else
                        {
                            foreach (var addr in g)
                            {
                                InvalidateCacheEntry(addr.Grain);
                            }
                            if (MyAddress.Equals(g.Key))
                            {
                                // if I am the owner, remove the old activation locally
                                foreach (var addr in g)
                                {
                                    unregistrationsLocal.Increment();
                                    DirectoryPartition.RemoveActivation(addr.Grain, addr.Activation, true);
                                }
                                return TaskDone.Done;
                            }
                            else
                            {
                                unregistrationsManyRemoteSent.Increment();
                                // otherwise, notify the owner
                                return GetDirectoryReference(g.Key).UnregisterMany(g.ToList(), NUM_RETRIES);
                            }
                        }
                    }));
        }

        public bool LocalLookup(GrainId grain, out List<ActivationAddress> addresses)
        {
            localLookups.Increment();

            SiloAddress silo = CalculateTargetSilo(grain, false);
            // No need to check that silo != null since we're passing excludeThisSiloIfStopping = false

            if (log.IsVerbose) log.Verbose("Silo {0} tries to lookup for {1}-->{2} ({3}-->{4})", MyAddress, grain, silo, grain.GetUniformHashCode(), silo.GetConsistentHashCode());

            // check if we own the grain
            if (silo.Equals(MyAddress))
            {
                localDirectoryLookups.Increment();
                addresses = GetLocalDirectoryData(grain);
                if (addresses == null)
                {
                    // it can happen that we cannot find the grain in our partition if there were 
                    // some recent changes in the membership
                    if (log.IsVerbose2) log.Verbose2("LocalLookup mine {0}=null", grain);
                    return false;
                }
                if (log.IsVerbose2) log.Verbose2("LocalLookup mine {0}={1}", grain, addresses.ToStrings());
                localDirectorySuccesses.Increment();
                localSuccesses.Increment();
                return true;
            }

            // handle cache
            cacheLookups.Increment();
            addresses = GetLocalCacheData(grain);
            if (addresses == null)
            {
                if (log.IsVerbose2) log.Verbose2("TryFullLookup else {0}=null", grain);
                return false;
            }
            if (log.IsVerbose2) log.Verbose2("LocalLookup cache {0}={1}", grain, addresses.ToStrings());
            cacheSuccesses.Increment();
            localSuccesses.Increment();
            return true;
        }

        public List<ActivationAddress> GetLocalDirectoryData(GrainId grain)
        {
            var result = DirectoryPartition.LookUpGrain(grain);
            if (result == null)
            {
                return null;
            }
            return result.Item1.Select(t => ActivationAddress.GetAddress(t.Item1, grain, t.Item2)).Where(addr => IsValidSilo(addr.Silo)).ToList();
        }

        public List<ActivationAddress> GetLocalCacheData(GrainId grain)
        {
            List<Tuple<SiloAddress, ActivationId>> cached = null;
            if (DirectoryCache.LookUp(grain, out cached))
            {
                return cached.Select(elem => ActivationAddress.GetAddress(elem.Item1, grain, elem.Item2)).Where(addr => IsValidSilo(addr.Silo)).ToList();
            }
            return null;
        }

        public async Task<List<ActivationAddress>> FullLookup(GrainId grain)
        {
            fullLookups.Increment();

            SiloAddress silo = CalculateTargetSilo(grain, false);
            // No need to check that silo != null since we're passing excludeThisSiloIfStopping = false

            if (log.IsVerbose) log.Verbose("Silo {0} fully lookups for {1}-->{2} ({3}-->{4})", MyAddress, grain, silo, grain.GetUniformHashCode(), silo.GetConsistentHashCode());

            // We assyme that getting here means the grain was not found locally (i.e., in TryFullLookup()).
            // We still check if we own the grain locally to avoid races between the time TryFullLookup() and FullLookup() were called.
            if (silo.Equals(MyAddress))
            {
                localDirectoryLookups.Increment();
                var localResult = DirectoryPartition.LookUpGrain(grain);
                if (localResult == null)
                {
                    // it can happen that we cannot find the grain in our partition if there were 
                    // some recent changes in the membership
                    if (log.IsVerbose2) log.Verbose2("FullLookup mine {0}=none", grain);
                    return new List<ActivationAddress>();
                }
                var a = localResult.Item1.Select(t => ActivationAddress.GetAddress(t.Item1, grain, t.Item2)).Where(addr => IsValidSilo(addr.Silo)).ToList();
                if (log.IsVerbose2) log.Verbose2("FullLookup mine {0}={1}", grain, a.ToStrings());
                localDirectorySuccesses.Increment();
                return a;
            }

            // Just a optimization. Why sending a message to someone we know is not valid.
            if (!IsValidSilo(silo))
            {
                throw new OrleansException(String.Format("Current directory at {0} is not stable to perform the lookup for grain {1} (it maps to {2}, which is not a valid silo). Retry later.", MyAddress, grain, silo));
            }

            remoteLookupsSent.Increment();
            var result = await GetDirectoryReference(silo).LookUp(grain, NUM_RETRIES);

            // update the cache
            var entries = result.Item1.Where(t => IsValidSilo(t.Item1)).ToList();
            if (entries.Count > 0)
            {
                DirectoryCache.AddOrUpdate(grain, entries, result.Item2);
            }
            List<ActivationAddress> addresses = entries.Select(t => ActivationAddress.GetAddress(t.Item1, grain, t.Item2)).ToList();
            if (log.IsVerbose2) log.Verbose2("FullLookup remote {0}={1}", grain, addresses.ToStrings());
            return addresses;
        }

        public Task DeleteGrain(GrainId grain)
        {
            SiloAddress silo = CalculateTargetSilo(grain);
            if (silo == null)
            {
                // We don't know about any other silos, and we're stopping, so throw
                throw new InvalidOperationException("Grain directory is stopping");
            }

            if (log.IsVerbose) log.Verbose("Silo {0} tries to lookup for {1}-->{2} ({3}-->{4})", MyAddress, grain, silo, grain.GetUniformHashCode(), silo.GetConsistentHashCode());

            if (silo.Equals(MyAddress))
            {
                // remove from our partition
                DirectoryPartition.RemoveGrain(grain);
                return TaskDone.Done;
            }

            // remove from the cache
            DirectoryCache.Remove(grain);

            // send request to the owner
            return GetDirectoryReference(silo).DeleteGrain(grain, NUM_RETRIES);
        }

        public void InvalidateCacheEntry(GrainId grain)
        {
            bool removed = DirectoryCache.Remove(grain);
            if (removed)
            {
                if(log.IsVerbose2) log.Verbose2("InvalidateCacheEntry for {0}", grain);
            }
        }

        public void InvalidateCacheEntryPartly(GrainId grain, ActivationId activation)
        {
            List<Tuple<SiloAddress, ActivationId>> list;
            if (DirectoryCache.LookUp(grain, out list))
            {
                list.RemoveAll(tuple => tuple.Item2.Equals(activation));
            }
        }

        //public void AddCacheEntries(IEnumerable<ActivationAddress> entries)
        //{
        //    foreach (var activationAddress in entries)
        //    {
        //        AddCacheEntry(activationAddress);
        //    }
        //}

        //public void AddCacheEntry(ActivationAddress activationAddress)
        //{
        //    var grain = activationAddress.Grain;
        //    var activation = activationAddress.Activation;
        //    var silo = activationAddress.Silo;
        //    if (IsValidSilo(silo))
        //    {
        //        SiloAddress owner = CalculateTargetSilo(grain);
        //        // Make sure not to cache anything you are the owner for it, since if you are the owner, it should be in your partition.
        //        if (owner == null || !owner.Equals(MyAddress))
        //        {
        //            List<Tuple<SiloAddress, ActivationId>> list;
        //            if (DirectoryCache.LookUp(grain, out list))
        //            {
        //                if (!list.Exists(tuple => tuple.Item2.Equals(activation)))
        //                {
        //                    list.Add(new Tuple<SiloAddress, ActivationId>(silo, activation));
        //                }
        //            }
        //            else
        //            {
        //                DirectoryCache.AddOrUpdate(grain, new List<Tuple<SiloAddress, ActivationId>> { new Tuple<SiloAddress, ActivationId>(silo, activation) }, 0);
        //            }
        //        }
        //    }
        //}

        /// <summary>
        /// For testing purposes only.
        /// Returns the silo that this silo thinks is the primary owner of directory information for
        /// the provided grain ID.
        /// </summary>
        /// <param name="grain"></param>
        /// <returns></returns>
        public SiloAddress GetPrimaryForGrain(GrainId grain)
        {
            return CalculateTargetSilo(grain);
        }

        /// <summary>
        /// For testing purposes only.
        /// Returns the silos that this silo thinks hold replicated directory information for
        /// the provided grain ID.
        /// </summary>
        /// <param name="grain"></param>
        /// <returns></returns>
        public List<SiloAddress> GetReplicasForGrain(GrainId grain)
        {
            var primary = CalculateTargetSilo(grain);
            return FindPredecessors(primary, ReplicationFactor);
        }

        /// <summary>
        /// For testing purposes only.
        /// Returns the directory information held by the local silo for the provided grain ID.
        /// The result will be null if no information is held.
        /// </summary>
        /// <param name="grain"></param>
        /// <param name="isPrimary"></param>
        /// <returns></returns>
        public List<ActivationAddress> GetLocalDataForGrain(GrainId grain, out bool isPrimary)
        {
            var primary = CalculateTargetSilo(grain);
            var backupData = ReplicationAgent.GetReplicatedInfo(grain);
            if (MyAddress.Equals(primary))
            {
                log.Assert(ErrorCode.DirectoryBothPrimaryAndBackupForGrain, backupData == null,
                    "Silo contains both primary and backup directory data for grain " + grain);
                isPrimary = true;
                return GetLocalDirectoryData(grain);
            }

            isPrimary = false;
            return backupData;
        }

        #endregion

        #region Implementation of ISiloShutdownParticipant

        public void BeginShutdown(Action tryFinish)
        {
            if (tryFinishShutdown != null)
                return;
            tryFinishShutdown = tryFinish;

            if (CanFinishShutdown())
                tryFinish();
        }

        public bool CanFinishShutdown()
        {
            return true;
        }

        public void FinishShutdown()
        {
            Stop(true);
        }

        public SiloShutdownPhase Phase
        {
            get { return SiloShutdownPhase.Messaging; }
        }

        #endregion

        public override string ToString()
        {
            var sb = new StringBuilder();

            long localLookupsDelta;
            long localLookupsCurrent = localLookups.GetCurrentValueAndDelta(out localLookupsDelta);
            long localLookupsSucceededDelta;
            long localLookupsSucceededCurrent = localSuccesses.GetCurrentValueAndDelta(out localLookupsSucceededDelta);
            long fullLookupsDelta;
            long fullLookupsCurrent = fullLookups.GetCurrentValueAndDelta(out fullLookupsDelta);
            long directoryPartitionSize = directoryPartitionCount.GetCurrentValue();

            sb.AppendLine("Local Grain Directory:");
            sb.AppendFormat("   Local partition: {0} entries", directoryPartitionSize).AppendLine();
            sb.AppendLine("   Since last call:");
            sb.AppendFormat("      Local lookups: {0}", localLookupsDelta).AppendLine();
            sb.AppendFormat("      Local found: {0}", localLookupsSucceededDelta).AppendLine();
            if (localLookupsCurrent > 0)
            {
                sb.AppendFormat("      Hit rate: {0:F1}%", (100.0 * localLookupsSucceededDelta) / localLookupsDelta).AppendLine();
            }
            sb.AppendFormat("      Full lookups: {0}", fullLookupsDelta).AppendLine();

            sb.AppendLine("   Since start:");
            sb.AppendFormat("      Local lookups: {0}", localLookupsCurrent).AppendLine();
            sb.AppendFormat("      Local found: {0}", localLookupsSucceededCurrent).AppendLine();
            if (localLookupsCurrent > 0)
            {
                sb.AppendFormat("      Hit rate: {0:F1}%", (100.0 * localLookupsSucceededCurrent) / localLookupsCurrent).AppendLine();
            }
            sb.AppendFormat("      Full lookups: {0}", fullLookupsCurrent).AppendLine();

            sb.Append(DirectoryCache.ToString());

            return sb.ToString();
        }

        private long RingDistanceToSuccessor()
        {
            long distance;
            List<SiloAddress> successorList = FindSuccessors(MyAddress, 1);
            if (successorList == null || successorList.Count == 0)
            {
                distance = 0;
            }
            else
            {
                SiloAddress successor = successorList.First();
                if (successor == null)
                {
                    distance = 0; // Only me here!
                }
                else
                {
                    distance = CalcRingDistance(MyAddress, successor);
                }
            }
            return distance;
        }

        private string RingDistanceToSuccessor_2()
        {
            const long ringSize = int.MaxValue * 2L;
            long distance;
            List<SiloAddress> successorList = FindSuccessors(MyAddress, 1);
            if (successorList == null || successorList.Count == 0)
            {
                distance = 0;
            }
            else
            {
                SiloAddress successor = successorList.First();
                if (successor == null)
                {
                    distance = 0; // Only me here!
                }
                else
                {
                    distance = CalcRingDistance(MyAddress, successor);
                }
            }
            double averageRingSpace = membershipRingList.Count == 0 ? 0 : (1.0 / (double)membershipRingList.Count);
            return string.Format("RingDistance={0:X}, %Ring Space {1:0.00000}%, Average %Ring Space {2:0.00000}%", distance, ((double)distance / (double)ringSize) * 100.0, averageRingSpace * 100.0);
        }

        private static long CalcRingDistance(SiloAddress silo1, SiloAddress silo2)
        {
            const long ringSize = int.MaxValue * 2L;
            long hash1 = silo1.GetConsistentHashCode();
            long hash2 = silo2.GetConsistentHashCode();

            if (hash2 > hash1) return hash2 - hash1;
            if (hash2 < hash1) return ringSize - (hash1 - hash2);
            else return 0;
        }

        public string RingStatusToString()
        {
            var sb = new StringBuilder();

            sb.AppendFormat("Silo address is {0}, silo consistent hash is {1:X}, replication factor is {2}", MyAddress, MyAddress.GetConsistentHashCode(), ReplicationFactor).AppendLine();
            sb.AppendLine("Ring is:");
            lock (membershipCache)
            {
                foreach (var silo in membershipRingList)
                {
                    sb.AppendFormat("    Silo {0}, consistent hash is {1:X}", silo, silo.GetConsistentHashCode()).AppendLine();
                }
            }
            sb.AppendFormat("My predecessors: {0}", FindPredecessors(MyAddress, Math.Max(ReplicationFactor, 1)).ToStrings(addr => String.Format("{0}/{1:X}---", addr, addr.GetConsistentHashCode()), " -- ")).AppendLine();
            sb.AppendFormat("My successors: {0}", FindSuccessors(MyAddress, Math.Max(ReplicationFactor, 1)).ToStrings(addr => String.Format("{0}/{1:X}---", addr, addr.GetConsistentHashCode()), " -- "));
            return sb.ToString();
        }

        internal IRemoteGrainDirectory GetDirectoryReference(SiloAddress silo)
        {
            return RemoteGrainDirectoryFactory.GetSystemTarget(Constants.DirectoryServiceId, silo);
        }
    }
}
