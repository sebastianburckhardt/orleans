using System;
using System.Collections.Generic;
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

        private static readonly TimeSpan RETRY_DELAY = TimeSpan.FromSeconds(5); // Pause 5 seconds between forwards to let the membership directory settle down

        internal RemoteGrainDirectory(LocalGrainDirectory r, GrainId id)
            : base(id, r.MyAddress)
        {
            router = r;
            partition = r.DirectoryPartition;
            logger = Logger.GetLogger("Orleans.GrainDirectory.CacheValidator", Logger.LoggerType.Runtime);
        }

        public async Task Register(ActivationAddress address, int retries)
        {
            // validate that this request arrived correctly
            //logger.Assert(ErrorCode.Runtime_Error_100135, destination.Matches(router.MyAddress), "destination address != my address");

            router.registrationsRemoteReceived.Increment();
            // validate that this grain should be stored in our partition
            SiloAddress owner = router.CalculateTargetSilo(address.Grain);
            if (owner == null)
            {
                // We don't know about any other silos, and we're stopping, so throw
                throw new InvalidOperationException("Grain directory is stopping");
            }
            
            if (router.MyAddress.Equals(owner))
            {
                router.registrationsLocal.Increment();
                partition.AddActivation(address.Grain, address.Activation, address.Silo);
            }
            else if (retries > 0)
            {
                if (logger.IsVerbose2) logger.Verbose2("Retry " + retries + " RemoteGrainDirectory.Register for address=" + address + " at Owner=" + owner);
                PrepareForRetry(retries);

                await Task.Delay(RETRY_DELAY);
                
                SiloAddress o = router.CalculateTargetSilo(address.Grain);
                if (o == null)
                {
                    // We don't know about any other silos, and we're stopping, so throw
                    throw new InvalidOperationException("Grain directory is stopping");
                }
                if (router.MyAddress.Equals(o))
                {
                    router.registrationsLocal.Increment();
                    partition.AddActivation(address.Grain, address.Activation, address.Silo);
                    return;
                }
                router.registrationsRemoteSent.Increment();
                await GetDirectoryReference(o).Register(address, retries - 1);
            }
            else
            {
                throw new OrleansException("Silo " + router.MyAddress + " is not the owner of the grain " + address.Grain + " Owner=" + owner);
            }
        }

        public Task RegisterMany(List<ActivationAddress> addresses, int retries)
        {
            // validate that this request arrived correctly
            //logger.Assert(ErrorCode.Runtime_Error_100138, destination.Matches(router.MyAddress), "destination address != my address");

            return Task.WhenAll(addresses.Select(addr => Register(addr, retries)));
        }

        /// <summary>
        /// Registers a new activation, in single activation mode, with the directory service.
        /// If there is already an activation registered for this grain, then the new activation will
        /// not be registered and the address of the existing activation will be returned.
        /// Otherwise, the passed-in address will be returned.
        /// <para>This method must be called from a scheduler thread.</para>
        /// </summary>
        /// <param name="destination"></param>
        /// <param name="address">The address of the potential new activation.</param>
        /// <param name="retries"></param>
        /// <returns>The address registered for the grain's single activation.</returns>
        public async Task<ActivationAddress> RegisterSingleActivation(ActivationAddress address, int retries)
        {
            // validate that this request arrived correctly
            //logger.Assert(ErrorCode.Runtime_Error_100139, destination.Matches(router.MyAddress), "destination address != my address");

            router.registrationsSingleActRemoteReceived.Increment();
            // validate that this grain should be stored in our partition
            SiloAddress owner = router.CalculateTargetSilo(address.Grain);
            if (owner == null)
            {
                // We don't know about any other silos, and we're stopping, so throw
                throw new InvalidOperationException("Grain directory is stopping");
            }

            if (router.MyAddress.Equals(owner))
            {
                router.registrationsSingleActLocal.Increment();
                return partition.AddSingleActivation(address.Grain, address.Activation, address.Silo);
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
                    router.registrationsSingleActLocal.Increment();
                    return partition.AddSingleActivation(address.Grain, address.Activation, address.Silo);
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

        public async Task<Tuple<List<Tuple<SiloAddress, ActivationId>>, int>> LookUp(GrainId grain, int retries)
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
                    return res;
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
                        return res;
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
                    Tuple<List<Tuple<SiloAddress, ActivationId>>, int> lookupResult = partition.LookUpGrain(tuple.Item1);
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
        public Task RegisterReplica(SiloAddress source, GrainId start, GrainId end, 
            Dictionary<GrainId, IGrainInfo> partition, bool isFullCopy)
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
    }
}
