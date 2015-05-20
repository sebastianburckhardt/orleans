using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;


using Orleans.Counters;

namespace Orleans.Runtime.GrainDirectory
{
    internal class RemoteGrainDirectory : SystemTarget, IRemoteGrainDirectory
    {
        private readonly LocalGrainDirectory router;
        private readonly GrainDirectoryPartition partition;
        private readonly Logger logger;
        private OrleansTimerInsideGrain grainResolutionTimer;


        private static readonly TimeSpan RETRY_DELAY = TimeSpan.FromSeconds(5); // Pause 5 seconds between forwards to let the membership directory settle down
        private static readonly TimeSpan ACTIVATION_RACE_DELAY = TimeSpan.FromSeconds(5);

        internal RemoteGrainDirectory(LocalGrainDirectory r, GrainId id)
            : base(id, r.MyAddress)
        {
            router = r;
            partition = r.DirectoryPartition;
            logger = Logger.GetLogger("Orleans.GrainDirectory.CacheValidator", Logger.LoggerType.Runtime);
        }

        public Task BecomeActive()
        {
            SiloAddress myGateway = Silo.CurrentSilo.OrleansConfig.Cluster.GetGateway(router.MyAddress.ClusterId);
            if (myGateway.Matches(router.MyAddress))
            {
                grainResolutionTimer = OrleansTimerInsideGrain.FromTimerCallback(ReconciliationFunc, null,
                    TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5), name: "Directory.GrainResolutionTimer",
                    options: OrleansTimerInsideGrain.OptionFlags.CountTicks);
                grainResolutionTimer.Start();
            }
            return Task.FromResult<object>(null);
        }

        private void ReconciliationFunc(object arg)
        {
            this.ReconcileDoubtfulActivations().Ignore();
            if (logger.IsVerbose2) logger.Verbose2("Grain resolution timer fired!");
        }

        public async Task Register(ActivationAddress address, int retries)
        {
            // validate that this request arrived correctly
            //logger.Assert(ErrorCode.Runtime_Error_100135, destination.Matches(router.MyAddress), "destination address != my address");
            router.registrationsRemoteReceived.Increment();
            await RegisterSingleActivation(address, retries);
            return;
        }

        public Task RegisterMany(List<ActivationAddress> addresses, int retries)
        {
            return Task.WhenAll(addresses.Select(addr => Register(addr, retries)));
        }

        // Use this function to register non-client activations. We assume that all these activations can be registered without going
        // to a remote cluster. 
        public async Task<ActivationAddress> RegisterSingleActivationLocal(ActivationAddress address, int retries)
        {
            SiloAddress owner = router.CalculateTargetSilo(address.Grain); 

            // This silo is the owner of the grain.
            if (router.MyAddress.Equals(owner))
            {
                return partition.AddSystemActivation(address.Grain, address.Activation, address.Silo);
            }
            else if (retries > 0)
            {
                await Task.Delay(RETRY_DELAY);
                owner = router.CalculateTargetSilo(address.Grain);
                if (router.MyAddress.Equals(owner))
                {
                    return partition.AddSystemActivation(address.Grain, address.Activation, address.Silo);
                }
                else
                {
                    return await GetDirectoryReference(owner).RegisterSingleActivationLocal(address, retries - 1);
                }
            }
            else
            {
                throw new OrleansException("RegisterSingleActivationLocal ran out of retries!");
            }
        }

        public async Task FlushCachedActivation(ActivationAddress address, int retries)
        {
            SiloAddress owner = router.CalculateTargetSilo(address.Grain);
            if (owner == null)
            {
                // We don't know about any other silos, and we're stopping, so throw
                throw new InvalidOperationException("Grain directory is stopping");
            }

            if (router.MyAddress.Equals(owner))
            {
                partition.RemoveCachedActivation(address);
            }

            if (retries > 0)
            {
                await Task.Delay(RETRY_DELAY);

                SiloAddress o = router.CalculateTargetSilo(address.Grain);
                if (o == null)
                {
                    // We don't know about any other silos, and we're stopping, so throw
                    throw new InvalidOperationException("Grain directory is stopping");
                }
                if (o.Equals(router.MyAddress))
                {
                    await FlushCachedActivation(address, retries - 1);
                }
                router.registrationsSingleActRemoteSent.Increment();
                await GetDirectoryReference(o).FlushCachedActivation(address, retries - 1);
            }
        }

        /// <summary>
        /// Returns the set of activations in state DOUBTFUL.
        /// </summary>
        /// <returns></returns>
        public Task<Dictionary<ActivationId, GrainId>> GetDoubtfulActivations()
        {
            var ret = partition.GetDoubtfulActivations();
            return Task.FromResult(ret);
        }

        // This method will fan out requests to each silo in this cluster, and ask it to send over its set of DOUBTFUL activations.
        private async Task<Dictionary<ActivationId, GrainId>> GetLocalDoubtfulActivations()
        {
            // First get the list of DOUBTFUL activations in this cluster. We ask silo in the cluster to send us its set of
            // DOUBTFUL activations.
            var doubtfulActivations = new Dictionary<ActivationId, GrainId>();

            // Ask every other silo in the cluster to send this silo its set of DOUBTFUL activations.
            Dictionary<SiloAddress, SiloStatus> activeSilos =
                Silo.CurrentSilo.LocalSiloStatusOracle.GetApproximateSiloStatuses(true);
            var doubtfulActivationPromises = new List<Task<Dictionary<ActivationId, GrainId>>>();
            foreach (var keyValue in activeSilos)
            {
                var address = keyValue.Key;
                if (address.Equals(router.MyAddress))
                {
                    // We don't need to make an RPC to our own silo. Just call the "GetDoubtfulActivations()" method directly.
                    doubtfulActivationPromises.Add(this.GetDoubtfulActivations());
                }
                else
                {
                    // Make an RPC to remote silos.
                    doubtfulActivationPromises.Add(GetDirectoryReference(address).GetDoubtfulActivations());
                }
            }

            // Wait for each silo in the cluster to send over its set of doubtful activations.
            foreach (var promise in doubtfulActivationPromises)
            {
                Dictionary<ActivationId, GrainId> doubtfuls;
                try
                {
                    doubtfuls = await promise; // This could throw an exception.
                }
                catch (Exception e)
                {
                    // Catch all errors with a remote cluster message at this point. For the moment, we silently ignore these
                    // errors and let execution continue. 
                    if (logger.IsInfo) logger.Info("Got an exception: " + e.Message);
                    continue;
                }
                foreach (var keyValue in doubtfuls)
                {
                    doubtfulActivations.Add(keyValue.Key, keyValue.Value);
                }
            }
            return doubtfulActivations;
        }

        private async Task<Tuple<Dictionary<ActivationId, GrainId>, bool>> RunAntiEntropy(
            Dictionary<ActivationId, GrainId> doubtfulActivations)
        {
            // Forward the list of doubtful activations to all other clusters.
            var loserPromises = new List<Task<Tuple<Dictionary<ActivationId, GrainId>, bool>>>();
            foreach (var siloAddr in Silo.CurrentSilo.OrleansConfig.Cluster.GetAllGateways())
            {
                if (!siloAddr.IsSameCluster(router.MyAddress))
                {
                    var remoteClusterPromise =
                        GetDirectoryReference(siloAddr).ProcessRemoteDoubtfulActivations(doubtfulActivations,
                        router.MyAddress.ClusterId);
                    loserPromises.Add(remoteClusterPromise);
                }
            }

            
            Dictionary<ActivationId, GrainId> loserActivations = new Dictionary<Orleans.ActivationId, GrainId>();
            var gotAllResponses = true;

            foreach (var promise in loserPromises)
            {
                Tuple<Dictionary<ActivationId, GrainId>, bool> retVal = null;
                // This call may throw an exception.
                try
                {
                    retVal = await promise;
                }
                catch (Exception)
                {
                    gotAllResponses = false;
                    continue;
                }
                gotAllResponses &= retVal.Item2;

                
                foreach (var keyVal in retVal.Item1)
                {
                    loserActivations.Add(keyVal.Key, keyVal.Value);
                }
            }
            return Tuple.Create(loserActivations, gotAllResponses);
        }

        // This function partitions the set of activations in "acts", so that each activation in acts is mapped to the silo which hosts
        // the appropriate grain directory partition.
        private Dictionary<SiloAddress, Dictionary<ActivationId, GrainId>> PartitionActivations(Dictionary<ActivationId, GrainId> acts)
        {
            Dictionary<SiloAddress, Dictionary<ActivationId, GrainId>> partitionedDict = 
                new Dictionary<SiloAddress, Dictionary<ActivationId, GrainId>>();
            foreach (var keyvalue in acts)
            {
                ActivationId actId = keyvalue.Key;
                GrainId grainId = keyvalue.Value;

                // Calculate the silo which in charge of the appropriate grain directory partition.
                var owner = router.CalculateTargetSilo(grainId);
                Dictionary<ActivationId, GrainId> actDict = null;
                if (!partitionedDict.TryGetValue(owner, out actDict))
                {
                    actDict = new Dictionary<ActivationId, GrainId>();
                    partitionedDict.Add(owner, actDict);
                }
                actDict.Add(actId, grainId);
            }
            return partitionedDict;
        }

        // DistributeAntiEntropyResults takes the set of winner and loser activations as input, as distributes them to the appropriate
        // grain directory partitions in the cluster.
        private void DistributeAntiEntropyResults(Dictionary<ActivationId, GrainId> losers, 
            Dictionary<ActivationId, GrainId> winners)
        {
            // Partition the set of loser activations according to the silo which actually maps it.
            Dictionary<SiloAddress, Dictionary<ActivationId, GrainId>> partitionedLosers = PartitionActivations(losers);
            Dictionary<SiloAddress, Dictionary<ActivationId, GrainId>> partitionedWinners = null;

            // Partition the set of winner activations, if there are any.
            if (winners != null)
            {
                partitionedWinners = PartitionActivations(winners);
            }

            // For every silo in partitionedLosers, send over the set of loser and winner activations of the grains that map to the silo's
            // grain directory partition.
            foreach (var keyvalue in partitionedLosers)
            {
                SiloAddress addr = keyvalue.Key;
                Dictionary<ActivationId, GrainId> loserPartition = keyvalue.Value;
                Dictionary<ActivationId, GrainId> winnerPartition = null;

                if (partitionedWinners != null && !partitionedWinners.TryGetValue(addr, out winnerPartition))
                {
                    winnerPartition = null;
                }

                if (addr.Equals(router.MyAddress))
                {
                    // If the silo is the current one, just call the method directly, we don't need to do perform an RPC.
                    this.ProcessAntiEntropyResults(loserPartition, winnerPartition).Ignore();
                }
                else
                {
                    // If the silo is another silo in our cluster, send over the set of loser and winner activations.
                    GetDirectoryReference(addr).ProcessAntiEntropyResults(loserPartition, winnerPartition).Ignore();
                }
            }

            // It may be the case that a silo has _no_ loser activations, in which case its SiloAddress will not appaear in the 
            // partitioned loseractivation dictionary. However, the silo may have a set of winner activations. In this case, we need to
            // send over the set of winner activations.
            if (partitionedWinners != null)
            {
                foreach (var keyvalue in partitionedWinners)
                {
                    SiloAddress addr = keyvalue.Key;
                    Dictionary<ActivationId, GrainId> winnerPartition = keyvalue.Value;

                    // Ensure that we're only processing silos which did not appear in the loser activation partition. If the silo is in
                    // partitionedLosers, then it we have already contacted it.
                    if (!partitionedLosers.ContainsKey(addr))
                    {
                        if (addr.Equals(router.MyAddress))
                        {
                            this.ProcessAntiEntropyResults(null, winnerPartition).Ignore();
                        }
                        else
                        {
                            GetDirectoryReference(addr).ProcessAntiEntropyResults(null, winnerPartition).Ignore();
                        }
                    }
                }
            }
        }

        // This function first obtains the set of DOUBTFUL activations in the cluster.
        public async Task ReconcileDoubtfulActivations()
        {
            Stopwatch sw = Stopwatch.StartNew();

            // Get all the DOUBTFUL activations in this cluster.
            Dictionary<ActivationId, GrainId> doubtfulActivations = await GetLocalDoubtfulActivations();
      
            // Run the anti-entropy protocol, and get the set of loser activations.
            var losers = await RunAntiEntropy(doubtfulActivations);
            Dictionary<ActivationId, GrainId> loserActivations = losers.Item1;
            bool convertOwned = losers.Item2;

            // If convertOwned is true, then it means that we can convert our DOUBTFUL activations to state OWNED. We store the set of DOUBTFUL
            // activations to be converted to state OWNED in "winnerActivations".
            Dictionary<ActivationId, GrainId> winnerActivations;
            if (convertOwned)
            {
                winnerActivations = doubtfulActivations;
                foreach (var keyvalue in loserActivations)
                {
                    winnerActivations.Remove(keyvalue.Key);
                }
            }
            else
            {
                winnerActivations = null;
            }

            // Finally, we fan out the set of loser activation and winner activations to all the silos in our cluster.
            DistributeAntiEntropyResults(loserActivations, winnerActivations);

            sw.Stop();
            double t = (double)sw.ElapsedTicks * 1000.0 / (double)Stopwatch.Frequency;
            logger.Warn(ErrorCode.Runtime, "Resolution Grains " + t + "Resolution-milliseconds");
        }

        // This function is called in order to process loser and winner activations which have been decided after running the anti-entropy
        // protocol. It performs two tasks: 1) Change the state of loser DOUBTFUL grains to REQUESTED_ACTIVATION, and change the state of 
        // winner DOUBTFUL grains to OWNED. 2) For each loser DOUBTFUL grain, re-execute the activation creation protocol. The reason we
        // re-run the activation creation protocol is to ensure that we obtain a CACHED reference to the appropriate remote activation.
        public Task ProcessAntiEntropyResults(Dictionary<ActivationId, GrainId> loserActivations, 
            Dictionary<ActivationId, GrainId> winnerActivations)
        {
            var toKillAddrList = partition.ProcessAntiEntropyResults(loserActivations, winnerActivations);
            KillActivations(toKillAddrList).Ignore();
            return Task.FromResult<object>(null);
        }

        // This method receives a set of DOUBTFUL activations from a remote cluster, and checks whether the receiving cluster contains
        // conflicting activations. If yes, it finds a winner using the scheme mentioned in Section 3.4 of the spec.
        public async Task<Tuple<Dictionary<ActivationId, GrainId>, bool>> ProcessRemoteDoubtfulActivations(
            Dictionary<ActivationId, GrainId> remoteDoubtfulActs, int sendingClusterId)
        {
            // Partition the received set of doubtful activations according to which silo owns the appropriate grain directory partition.
            Dictionary<SiloAddress, Dictionary<ActivationId, GrainId>> doubtfulPartitions = PartitionActivations(remoteDoubtfulActs);

            var loserActivationPromises = new List<Task<Dictionary<ActivationId, GrainId>>>();
            var remoteLoserActivations = new Dictionary<ActivationId, GrainId>();

            // Fan out to all the silos in the cluster.
            foreach (var keyValue in doubtfulPartitions)
            {
                var silo = keyValue.Key;
                var activations = keyValue.Value;
                if (router.MyAddress.Equals(silo))
                {
                    loserActivationPromises.Add(this.FindLoserDoubtfulActivations(activations, sendingClusterId));
                }
                else
                {
                    var loserListPromise =
                        GetDirectoryReference(silo)
                            .FindLoserDoubtfulActivations(activations, sendingClusterId)
                            .WithTimeout(ACTIVATION_RACE_DELAY);
                    loserActivationPromises.Add(loserListPromise);
                }
            }

            // Wait for the silos in our cluster to respond.
            bool allSuccessful = true;
            foreach (var promise in loserActivationPromises)
            {
                try
                {
                    var losers = await promise;
                    foreach (var keyValue in losers)
                    {
                        var actId = keyValue.Key;
                        var grainId = keyValue.Value;
                        remoteLoserActivations.Add(actId, grainId);
                    }
                }
                catch (Exception)
                {
                    allSuccessful = false;
                }
            }
            return Tuple.Create(remoteLoserActivations, allSuccessful);
        }

        // This function takes a list of ActivationAddresses as input. For each one, it runs the activation creation protocol. This function
        // does not actually kill activations. Instead, it runs the creation protocol to find out the remote activation of the grain, 
        // obtain a reference to the remote activation, add it to our grain directory, and finally kill off the local activation.
        private async Task KillActivations(List<ActivationAddress> toKill)
        {
            List<Task> resolveTasks = new List<Task>();
            foreach (var address in toKill)
            {
                resolveTasks.Add(InterClusterRegistrationFunc(address));
            }
            await Task.WhenAll(resolveTasks);
        }

        // This function checks for duplicate activations in the grain directory partition, and deterministically picks a winner.
        public Task<Dictionary<ActivationId, GrainId>> FindLoserDoubtfulActivations(
            Dictionary<ActivationId, GrainId> remoteDoubtful, int remoteClusterId)
        {
            var localLosers = partition.ResolveDoubtfulActivations(remoteDoubtful, remoteClusterId);
            KillActivations(localLosers).Ignore();
            return Task.FromResult(remoteDoubtful);
        }

        // This function runs the activation creation protocol described in our spec (Section 3.3). The comments inside this function
        // explain details :).
        private async Task InterClusterRegistrationFunc(ActivationAddress address)
        {
            while (true)
            {
                // The meaning of result's fields:
                // result.Item1 is the ActivationAddress of the grain which has already been activated by a remote cluster.
                //
                // result.Item2 is true if all clusters in the total multi-cluster responded to this cluster's activation request.
                //
                // result.Item3 is PASS if _all_ clusters that responded responded with PASS. If even a single one responds with FAIL,
                // then result.Item3 is FAIL.
                Tuple<ActivationAddress, bool, ActivationResponseStatus> result;

                // First ask each cluster whether it has already activated the grain. InterClusterLookup sends ACTIVATION_REQUEST
                // messages to all other clusters (as described in our spec).
                result = await router.SendActivationRequests(address.Grain);

                // If result.Item3 == PASS, no cluster which responded to our ACTIVATION_REQUEST responded with FAIL. Go ahead
                // and change the state of our activation to either OWNED or DOUBTFUL.
                if (result.Item3 == ActivationResponseStatus.PASS)
                {
                    // result.Item2 is true if we got a response from _all_ clusters in the total multicluster. Otherwise it is false. 
                    // If we have received a response from all clusters in the total multi-cluster, our activation's state is OWNED.
                    // Otherwise, our activation's state is DOUBTFUL.
                    ActivationStatus status = result.Item2
                        ? ActivationStatus.OWNED
                        : ActivationStatus.DOUBTFUL;

                    // TakeOwnership returns true if we were able to successfully change our activation's state to OWNED/DOUBTFUL.
                    // The reason the activation's state may not be changed to OWNED/DOUBTFUL is because we raced with another cluster
                    // and the activation's state got changed to RACE_LOSER.
                    if (partition.TakeOwnership(address.Grain, address.Activation, address.Silo, status))
                    {
                        if (logger.IsVerbose2) logger.Verbose2("Own grain {0}. Owner is address {1}. {2}.", address.Grain, address.Silo, status);
                        return;
                    }
                    else
                    {
                        // We failed to take Status because we raced with another cluster. Try again after a delay.
                        // await Task.Delay(ACTIVATION_RACE_DELAY);
                        continue;
                    }
                }
                else if (result.Item1 != null)
                {
                    // result.Item3 == FAIL and some cluster has told us that it has already activated the grain. We keep a reference
                    // to this cluster's activation in our grain directory in state CACHED.
                    //
                    // remoteSilo is the silo where the remote activation resides. Note that it's not strictly necessary to store the
                    // complete silo address. We only require the ClusterId of the cluster which contains the activation. All requests
                    // to the associated grain will go through the cluster's gateway. Upon receiving an invocation of the activation,
                    // the gateway will forward the invocation to the appropriate silo. 
                    //
                    // remoteId is the ActivationId of the remote cluster's activation.
                    SiloAddress remoteSilo = result.Item1.Silo;
                    ActivationId remoteId = result.Item1.Activation;
                    if (logger.IsVerbose2) logger.Verbose2("Found the owner for grain {0}. Owner is address {1}", address.Grain, 
                        remoteSilo);

                    // Validate that the remoteSilo is not a silo on our local cluster. This is a _SERIOUS BUG_.
                    if (remoteSilo.ClusterId == -1 || remoteSilo.IsSameCluster(router.MyAddress))
                    {
                        throw new OrleansException("Unexpected cluster id!");
                    }

                    // CacheAddress removes our local activation from the grain directory, and replaces it with the one specified
                    // by the remote cluster.
                    partition.CacheAddress(address.Grain, remoteId, remoteSilo, address.Activation);

                    // De-activate the local activation of the grain.
                    GetCatalogReference(address.Silo).DeleteActivationsLocal(new List<ActivationAddress> {address}).Ignore();
                    return;
                }
                else
                {
                    // At this point, result.Item3 == FAIL and result.Item1 == null. This means that a remote cluster was racing to
                    // create the same grain as us, but hasn't yet successfully created the grain. We therefore delay, and run the 
                    // protocol again.
                    // await Task.Delay(ACTIVATION_RACE_DELAY);
                    continue;
                }
            }
        }

        /// <summary>
        /// Registers a new activation, in single activation mode, with the directory service.
        /// If there is already an activation registered for this grain, then the new activation will
        /// not be registered and the address of the existing activation will be returned.
        /// Otherwise, the passed-in address will be returned.
        /// 
        /// In order to validate that the no other cluster has already activated the grain, this method
        /// invokes InterClusterRegistrationFunc asynchronously.
        /// <para>This method must be called from a scheduler thread.</para>
        /// </summary>
        /// <param name="destination"></param>
        /// <param name="address">The address of the potential new activation.</param>
        /// <param name="retries"></param>
        /// <returns>The address registered for the grain's single activation.</returns>
        public async Task<ActivationAddress> RegisterSingleActivation(ActivationAddress address, int retries)
        {
            // validate that this request arrived correctly
            // logger.Assert(ErrorCode.Runtime_Error_100139, destination.Matches(router.MyAddress), "destination address != my address");
            if (logger.IsVerbose2) logger.Verbose2(string.Format("Trying to register activation for grain. GrainId: {0}. ActivationId: {1}.",
                address.Grain, address.Activation));
            
            router.registrationsSingleActRemoteReceived.Increment();
            // validate that this grain should be stored in our partition
            SiloAddress owner = router.CalculateTargetSilo(address.Grain);
            if (owner == null)
            {
                // We don't know about any other silos, and we're stopping, so throw
                throw new InvalidOperationException("Grain directory is stopping");
            }

            // If this silo is the current owner of the grain, continue processing, 
            // otherwise forward the request to the appropriate silo.
            if (router.MyAddress.Equals(owner))
            {
                router.registrationsSingleActLocal.Increment();
                ActivationAddress ret;
                
                // If the partition does not contain an entry for the grain, AddSingleActivation adds the grain 
                // to the grain directory in state REQUESTED_OWNERSHIP, and returns true. "ret" contains the 
                // ActivationAddress in the grain directory. If we succeed in adding the grain, then we run
                // the activation registration protocol by calling "InterClusterRegistrationFunc". 
                if (partition.AddSingleActivation(address.Grain, address.Activation, address.Silo, out ret))
                {
                    InterClusterRegistrationFunc(address).Ignore();
                }
                return ret;
            }

            if (retries > 0)
            {
                if (logger.IsVerbose2) logger.Verbose2("Retry " + retries + " RemoteGrainDirectory.RegisterSingleActivation for address=" + address + " at Owner=" + owner);
                PrepareForRetry(retries);

                await Task.Delay(RETRY_DELAY);

                SiloAddress o = router.CalculateTargetSilo(address.Grain);
                if (o == null)
                {
                    // We don't know about any other silos, and we're stopping, so throw
                    throw new InvalidOperationException("Grain directory is stopping");
                }
                if (o.Equals(router.MyAddress))
                {
                    return await RegisterSingleActivation(address, retries-1);
                }
                router.registrationsSingleActRemoteSent.Increment();
                return await GetDirectoryReference(o).RegisterSingleActivation(address, retries - 1);
            }
            throw new OrleansException("Silo " + router.MyAddress + " is not the owner of the grain " + address.Grain + " Owner=" + owner);
        }

        /// <summary>
        /// Registers multiple new activations, in single activation mode, with the directory service.
        /// If there is already an activation registered for any of the grains, then the corresponding new activation will
        /// not be registered and the address of the existing activation will be returned.
        /// Otherwise, the passed-in address will be returned.
        /// <para>This method must be called from a scheduler thread.</para>
        /// </summary>
        /// <param name="silo"></param>
        /// <param name="addresses"></param>
        /// <param name="retries">Number of retries to execute the method in case the virtual ring (servers) changes.</param>
        /// <returns></returns>
        public Task RegisterManySingleActivation(List<ActivationAddress> addresses, int retries)
        {
            // validate that this request arrived correctly
            //logger.Assert(ErrorCode.Runtime_Error_100140, silo.Matches(router.MyAddress), "destination address != my address");
            //var result = new List<ActivationAddress>();
            if (logger.IsVerbose2) logger.Verbose2("Received RegisterManySingleActivation");

            if (addresses.Count == 0)
            {
                return TaskDone.Done;
            }

            var done = addresses.Select(addr => this.RegisterSingleActivation(addr, retries));

            return Task.WhenAll(done);
        }

        public async Task Unregister(ActivationAddress address, bool force, int retries)
        {
            // validate that this request arrived correctly
            //logger.Assert(ErrorCode.Runtime_Error_100149, destination.Matches(router.MyAddress), "destination address != my address");

            router.unregistrationsRemoteReceived.Increment();
            // validate that this grain should be stored in our partition
            SiloAddress owner = router.CalculateTargetSilo(address.Grain);
            if (owner == null)
            {
                // We don't know about any other silos, and we're stopping, so throw
                throw new InvalidOperationException("Grain directory is stopping");
            }

            if (owner.Equals(router.MyAddress))
            {
                router.unregistrationsLocal.Increment();
                partition.RemoveActivation(address.Grain, address.Activation, force);
                return;
            }
            
            if (retries > 0)
            {
                if (logger.IsVerbose2) logger.Verbose2("Retry " + retries + " RemoteGrainDirectory.Unregister for address=" + address + " at Owner=" + owner);
                PrepareForRetry(retries);

                await Task.Delay(RETRY_DELAY);

                SiloAddress o = router.CalculateTargetSilo(address.Grain);
                if (o == null)
                {
                    // We don't know about any other silos, and we're stopping, so throw
                    throw new InvalidOperationException("Grain directory is stopping");
                }
                if (o.Equals(router.MyAddress))
                {
                    router.unregistrationsLocal.Increment();
                    partition.RemoveActivation(address.Grain, address.Activation, force);
                    return;
                }
                router.unregistrationsRemoteSent.Increment();
                await GetDirectoryReference(o).Unregister(address, force, retries - 1);
            }
            else
            {
                throw new OrleansException("Silo " + router.MyAddress + " is not the owner of the grain " + address.Grain + " Owner=" + owner);
            }
        }

        public async Task UnregisterMany(List<ActivationAddress> addresses, int retries)
        {
            router.unregistrationsManyRemoteReceived.Increment();
            var retry = new Dictionary<SiloAddress, List<ActivationAddress>>();
            foreach (var address in addresses)
            {
                SiloAddress owner = router.CalculateTargetSilo(address.Grain);
                if (owner == null)
                {
                    // We don't know about any other silos, and we're stopping, so throw
                    throw new InvalidOperationException("Grain directory is stopping");
                }
                else if (owner.Equals(router.MyAddress))
                {
                    router.unregistrationsLocal.Increment();
                    partition.RemoveActivation(address.Grain, address.Activation, true);
                }
                else
                {
                    List<ActivationAddress> list;
                    if (retry.TryGetValue(owner, out list))
                        list.Add(address);
                    else
                        retry[owner] = new List<ActivationAddress> {address};
                }
            }
            if (retry.Count == 0)
                return;
            if (retries <= 0)
                throw new OrleansException("Silo " + router.MyAddress + " is not the owner of grains" + 
                            Utils.DictionaryToString(retry, " "));
            PrepareForRetry(retries);

            await Task.Delay(RETRY_DELAY);

            await Task.WhenAll( retry.Select(p =>
                    {
                        router.unregistrationsManyRemoteSent.Increment();
                        return GetDirectoryReference(p.Key).UnregisterMany(p.Value, retries - 1);
                    }));
        }

        public async Task DeleteGrain(GrainId grain, int retries)
        {
            // validate that this request arrived correctly
            //logger.Assert(ErrorCode.Runtime_Error_100153, destination.Matches(router.MyAddress), "destination address != my address");

            // validate that this grain should be stored in our partition
            SiloAddress owner = router.CalculateTargetSilo(grain);
            if (owner == null)
            {
                // We don't know about any other silos, and we're stopping, so throw
                throw new InvalidOperationException("Grain directory is stopping");
            }
            
            if (owner.Equals(router.MyAddress))
            {
                partition.RemoveGrain(grain);
                return;
            }
            
            if (retries > 0)
            {
                if (logger.IsVerbose2) logger.Verbose2("Retry " + retries + " RemoteGrainDirectory.DeleteGrain for Grain=" + grain + " at Owner=" + owner);
                PrepareForRetry(retries);

                await Task.Delay(RETRY_DELAY);

                SiloAddress o = router.CalculateTargetSilo(grain);
                    if (o == null)
                    {
                        // We don't know about any other silos, and we're stopping, so throw
                        throw new InvalidOperationException("Grain directory is stopping");
                    }
                    if (o.Equals(router.MyAddress))
                    {
                        partition.RemoveGrain(grain);
                        return;
                    }
                    await GetDirectoryReference(o).DeleteGrain(grain, retries - 1);
            }
            else
            {
                throw new OrleansException("Silo " + router.MyAddress + " is not the owner of the grain " + grain + " Owner=" + owner);
            }
        }

        // A remote cluster calls this function while trying to activate a grain. This method processes an ACTIVATION_REQUEST message
        // as described in our spec (Section 3.3). ProcessActivationRequest returns a tuple whose fields take the following values:
        //
        // tuple.Item1 is either PASS or FAIL. The GrainStatusResponse method returns PASS or FAIL depending on the state of the grain's
        // activation.
        //
        // tuple.Item2 is the ActivationAddress of the local activation if it is in state DOUBTFUL or OWNED, or if this cluster is racing
        // to activate the grain with the requesting cluster, and wins the race. If tuple.Item1 is PASS then tuple.Item2 _must_ be null.
        // However, if tuple.Item1 is FAIL then tuple.Item2 is null if the requesting cluster lost a race, and we are not yet sure if
        // this cluster is going to activate the grain (because the protocol is still in progress). tuple.Item1 is FAIL and tuple.Item2 is
        // non-null if this cluster has already created a DOUBTFUL or OWNED activation.
        public async Task<Tuple<ActivationResponseStatus, ActivationAddress>> ProcessActivationRequest(GrainId grain, 
            int requestClusterId, int retries)
        {
            // Only lookup this silo's partition if it's actually responsible for the grain. If it is not responsible, then
            // it's because other silos may be entering or leaving the directory.
            SiloAddress owner = router.CalculateTargetSilo(grain, false);
            if (router.MyAddress.Equals(owner))
            {
                // GrainStatusResponse returns PASS or FAIL, and the appropriate ActivationAddress based on the state of the grain directory
                // partition at this silo.
                Tuple<ActivationResponseStatus, ActivationAddress> toReturn = null;
                toReturn = partition.GrainStatusResponse(grain, requestClusterId);
                if (logger.IsVerbose2) logger.Verbose2("Responded with {0}. GrainId={1}", toReturn.Item1, grain.ToString());
                return toReturn;
            }

            if (retries > 0)
            {
                if (logger.IsVerbose2) logger.Verbose2("Retry " + retries + " RemoteGrainDirectory.ProcessActivationRequest for Grain=" + 
                    grain + " at Owner=" + owner);
                PrepareForRetry(retries);

                await Task.Delay(RETRY_DELAY);

                // Recalculate the target silo because the directory may have changed.
                SiloAddress o = router.CalculateTargetSilo(grain, false);
                if (router.MyAddress.Equals(o))
                {
                    // GrainStatusResponse returns PASS or FAIL, and the appropriate ActivationAddress based on the state of the grain directory
                    // partition at this silo.
                    Tuple<ActivationResponseStatus, ActivationAddress> toReturn = null;
                    toReturn = partition.GrainStatusResponse(grain, requestClusterId);
                    if (logger.IsVerbose2) logger.Verbose2("Responded with {0}. GrainId={1}", toReturn.Item1, grain.ToString());
                    return toReturn;
                }
                else
                {
                    return await GetDirectoryReference(o).ProcessActivationRequest(grain, requestClusterId, retries - 1);
                }
            }
            throw new OrleansException("Couldn't satisfy process activation request!");
        }

        public async Task<Tuple<List<Tuple<SiloAddress, ActivationId>>, int>> LookUp(GrainId grain, int retries, bool ownedOnly = false)
        {
            router.remoteLookupsReceived.Increment();
            // validate that this request arrived correctly
            //logger.Assert(ErrorCode.Runtime_Error_100141, destination.Matches(router.MyAddress), "destination address != my address");

            // validate that this grain should be stored in our partition
            SiloAddress owner = router.CalculateTargetSilo(grain, false);
            if (router.MyAddress.Equals(owner))
            {
                router.localDirectoryLookups.Increment();
                // It can happen that we cannot find the grain in our partition if there were 
                // some recent changes in the membership. Return empty list in such case (and not null) to avoid
                // NullReference exceptions in the code of invokers
                var res = partition.LookUpGrain(grain);
                if (res != null)
                {
                    router.localDirectorySuccesses.Increment();
                    var retTuples = res.Item1.Select(x => new Tuple<SiloAddress, ActivationId>(x.Item1, x.Item2)).ToList();
                    return new Tuple<List<Tuple<SiloAddress, ActivationId>>, int>(retTuples, res.Item2);
                }
                
                return new Tuple<List<Tuple<SiloAddress, ActivationId>>, int>(new List<Tuple<SiloAddress, ActivationId>>(), -1);
            }
            
            if (retries > 0)
            {
                if (logger.IsVerbose2) logger.Verbose2("Retry " + retries + " RemoteGrainDirectory.LookUp for Grain=" + grain + " at Owner=" + owner);
                PrepareForRetry(retries);

                await Task.Delay(RETRY_DELAY);

                SiloAddress o = router.CalculateTargetSilo(grain, false);
                if (router.MyAddress.Equals(o))
                {
                    router.localDirectoryLookups.Increment();
                    var res = partition.LookUpGrain(grain);
                    if (res != null)
                    {
                        router.localDirectorySuccesses.Increment();
                        var retTuples = res.Item1.Select(x => new Tuple<SiloAddress, ActivationId>(x.Item1, x.Item2)).ToList();
                        return new Tuple<List<Tuple<SiloAddress, ActivationId>>, int>(retTuples, res.Item2);
                    }
                    else
                    {
                        return new Tuple<List<Tuple<SiloAddress, ActivationId>>, int>(new List<Tuple<SiloAddress, ActivationId>>(), -1);
                    }
                }
                router.remoteLookupsSent.Increment();
                return await GetDirectoryReference(o).LookUp(grain, retries - 1);
            }
            
            throw new OrleansException("Silo " + router.MyAddress + " is not the owner of the grain " + grain + " Owner=" + owner);
        }

        public Task<List<Tuple<GrainId, int, List<Tuple<SiloAddress, ActivationId>>>>> LookUpMany(List<Tuple<GrainId, int>> grainAndETagList, int retries)
        {
            router.cacheValidationsReceived.Increment();
            if (logger.IsVerbose2) logger.Verbose2("LookUpMany for {0} entries", grainAndETagList.Count);

            // validate that this request arrived correctly
            //logger.Assert(ErrorCode.Runtime_Error_100142, destination.Matches(router.MyAddress), "destination address != my address");

            List<Tuple<GrainId, int, List<Tuple<SiloAddress, ActivationId>>>> result =
                new List<Tuple<GrainId, int, List<Tuple<SiloAddress, ActivationId>>>>();

            foreach (Tuple<GrainId, int> tuple in grainAndETagList)
            {
                int curGen = partition.GetGrainETag(tuple.Item1);
                if (curGen == tuple.Item2 || curGen == -1)
                {
                    // the grain entry either does not exist in the local partition (curGen = -1) or has not been updated
                    result.Add(new Tuple<GrainId, int, List<Tuple<SiloAddress, ActivationId>>>(tuple.Item1, curGen, null));
                }
                else
                {
                    // the grain entry has been updated -- fetch and return its current version
                    var lookupResult = partition.LookUpGrain(tuple.Item1);
                    // validate that the entry is still in the directory (i.e., it was not removed concurrently)
                    if (lookupResult != null)
                    {
                        result.Add(new Tuple<GrainId, int, List<Tuple<SiloAddress, ActivationId>>>(tuple.Item1, lookupResult.Item2, lookupResult.Item1));
                    }
                    else
                    {
                        result.Add(new Tuple<GrainId, int, List<Tuple<SiloAddress, ActivationId>>>(tuple.Item1, -1, null));
                    }
                }
            }
            return Task.FromResult(result);
        }

        // TODO: start and end are ignored for now; they'll be used when we chunk full updates
        public Task RegisterReplica(SiloAddress source, GrainId start, GrainId end, Dictionary<GrainId, IGrainInfo> partition, 
            bool isFullCopy)
        {
            // validate that this request arrived correctly
            //logger.Assert(ErrorCode.Runtime_Error_100143, destination.Matches(router.MyAddress), "destination address != my address");

            router.ReplicationAgent.RegisterReplica(source, partition, isFullCopy);
            return TaskDone.Done;
        }

        public Task UnregisterReplica(SiloAddress source)
        {
            // validate that this request arrived correctly
            //logger.Assert(ErrorCode.Runtime_Error_100144, destination.Matches(router.MyAddress), "destination address != my address");

            router.ReplicationAgent.UnregisterReplica(source);
            return TaskDone.Done;
        }
        
        /// <summary>
        /// This method is called before retrying to access the current owner of a grain, following
        /// a request that was sent to us, while we are not the owner of the given grain.
        /// This may happen if during the time the request was on its way, a ring has changed 
        /// (new servers came up / failed down).
        /// Here we might take some actions before the actual retrial is done.
        /// For example, we might back-off for some random time.
        /// </summary>
        /// <param name="retries"></param>
        protected void PrepareForRetry(int retries)
        {
            // For now, we do not do anything special ...
        }

        private IRemoteGrainDirectory GetDirectoryReference(SiloAddress target)
        {
            return RemoteGrainDirectoryFactory.GetSystemTarget(Constants.DirectoryServiceId, target);
        }

        private ICatalog GetCatalogReference(SiloAddress target)
        {
            return CatalogFactory.GetSystemTarget(Constants.CatalogId, target);
        }
    }
}
