using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans.GrainDirectory;

namespace Orleans.Runtime.GrainDirectory
{
    internal class RemoteGrainDirectory : SystemTarget, IRemoteGrainDirectory
    {
        private readonly LocalGrainDirectory router;
        private readonly GrainDirectoryPartition partition;
        private readonly TraceLogger logger;

        private static readonly TimeSpan RETRY_DELAY = TimeSpan.FromSeconds(5); // Pause 5 seconds between forwards to let the membership directory settle down

        internal RemoteGrainDirectory(LocalGrainDirectory r, GrainId id)
            : base(id, r.MyAddress)
        {
            router = r;
            partition = r.DirectoryPartition;
            logger = TraceLogger.GetLogger("Orleans.GrainDirectory.CacheValidator", TraceLogger.LoggerType.Runtime);
        }

        public async Task<Tuple<ActivationAddress, int>> RegisterAsync(ActivationAddress address, bool singleActivation, int hopcount)
        {
            (singleActivation ? router.RegistrationsSingleActRemoteReceived : router.RegistrationsRemoteReceived).Increment();
            
            return await router.RegisterAsync(address, singleActivation, hopcount);
        }

        public Task RegisterMany(List<ActivationAddress> addresses, bool singleActivation)
        {
            // validate that this request arrived correctly
            //logger.Assert(ErrorCode.Runtime_Error_100140, silo.Matches(router.MyAddress), "destination address != my address");

            if (logger.IsVerbose2) logger.Verbose2("RegisterMany Count={0}", addresses.Count);

            if (addresses.Count == 0)
                return TaskDone.Done;

            return Task.WhenAll(addresses.Select(addr => router.RegisterAsync(addr, singleActivation, 1)));
        }

        public Task UnregisterAsync(ActivationAddress address, bool force, int hopcount)
        {
            return router.UnregisterAsync(address, force, hopcount);
        }

        public Task UnregisterManyAsync(List<ActivationAddress> addresses, int hopcount)
        {
            return router.UnregisterManyAsync(addresses, hopcount);
        }

        public  Task DeleteGrainAsync(GrainId grain, int hopcount)
        {
            return router.DeleteGrainAsync(grain, hopcount);
        }

        public async Task<AddressesAndTag> LookupAsync(GrainId gid, int hopcount)
        {
            return await router.LookupAsync(gid, hopcount);
        }

        public async Task<List<Tuple<GrainId, int, List<ActivationAddress>>>> LookUpMany(List<Tuple<GrainId, int>> grainAndETagList)
        {
            router.CacheValidationsReceived.Increment();
            if (logger.IsVerbose2) logger.Verbose2("LookUpMany for {0} entries", grainAndETagList.Count);

            var result = new List<Tuple<GrainId, int, List<ActivationAddress>>>();

            foreach (Tuple<GrainId, int> tuple in grainAndETagList)
            {
                int curGen = partition.GetGrainETag(tuple.Item1);
                if (curGen == tuple.Item2 || curGen == GrainInfo.NO_ETAG)
                {
                    // the grain entry either does not exist in the local partition (curGen = -1) or has not been updated
                    result.Add(new Tuple<GrainId, int, List<ActivationAddress>>(tuple.Item1, curGen, null));
                }
                else
                {
                    // the grain entry has been updated -- fetch and return its current version
                    var lookupResult = await router.LookupAsync(tuple.Item1, 1);
                    // validate that the entry is still in the directory (i.e., it was not removed concurrently)
                    if (lookupResult.Addresses != null)
                    {
                        result.Add(new Tuple<GrainId, int, List<ActivationAddress>>(tuple.Item1, lookupResult.VersionTag, lookupResult.Addresses));
                    }
                    else
                    {
                        result.Add(new Tuple<GrainId, int, List<ActivationAddress>>(tuple.Item1, GrainInfo.NO_ETAG, null));
                    }
                }
            }
            return result;
        }

        public Task AcceptHandoffPartition(SiloAddress source, Dictionary<GrainId, IGrainInfo> partition, bool isFullCopy)
        {
            router.HandoffManager.AcceptHandoffPartition(source, partition, isFullCopy);
            return TaskDone.Done;
        }

        public Task RemoveHandoffPartition(SiloAddress source)
        {
            router.HandoffManager.RemoveHandoffPartition(source);
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
            return InsideRuntimeClient.Current.InternalGrainFactory.GetSystemTarget<IRemoteGrainDirectory>(Constants.DirectoryServiceId, target);
        }
    }
}
