/*
Project Orleans Cloud Service SDK ver. 1.0
 
Copyright (c) Microsoft Corporation
 
All rights reserved.
 
MIT License

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and 
associated documentation files (the ""Software""), to deal in the Software without restriction,
including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense,
and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;


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

        public async Task<Tuple<ActivationAddress, int>> Register(ActivationAddress address, bool withRetry /*ignored*/)
        {
            router.RegistrationsRemoteReceived.Increment();
            
            return await router.Register(address, false);
        }

        public Task RegisterMany(List<ActivationAddress> addresses, int retries)
        {
            // validate that this request arrived correctly
            //logger.Assert(ErrorCode.Runtime_Error_100140, silo.Matches(router.MyAddress), "destination address != my address");

            if (logger.IsVerbose2) logger.Verbose2("RegisterMany Count={0}", addresses.Count);

            if (addresses.Count == 0)
                return TaskDone.Done;
            
            return Task.WhenAll(addresses.Select(addr => Register(addr, false)));
        }

        public async Task<bool> Unregister(ActivationAddress address, bool force = true, bool withRetry = true /*ignored*/)
        {
            router.UnregistrationsRemoteReceived.Increment();
            return await router.Unregister(address, force, false);
        }

        public async Task<List<ActivationAddress>> UnregisterManyAsync(List<ActivationAddress> addresses, bool withRetry = true /*ignored*/)
        {
            router.UnregistrationsManyRemoteReceived.Increment();
            return await router.UnregisterManyAsync(addresses, false);
        }

        public async Task<bool> DeleteGrain(GrainId grain, bool withRetry = true)
        {
            return await router.DeleteGrain(grain, false);
        }

        public async Task<Tuple<List<Tuple<SiloAddress, ActivationId>>, int>> LookUp(GrainId grain, int retries)
        {
            router.RemoteLookupsReceived.Increment();

            // validate that this grain should be stored in our partition
            SiloAddress owner = router.CalculateTargetSilo(grain, false);
            if (router.MyAddress.Equals(owner))
            {
                router.LocalDirectoryLookups.Increment();
                // It can happen that we cannot find the grain in our partition if there were 
                // some recent changes in the membership. Return empty list in such case (and not null) to avoid
                // NullReference exceptions in the code of invokers
                Tuple<List<Tuple<SiloAddress, ActivationId>>, int> res = partition.LookUpGrain(grain);
                if (res != null)
                {
                    router.LocalDirectorySuccesses.Increment();
                    return res;
                }

                return new Tuple<List<Tuple<SiloAddress, ActivationId>>, int>(new List<Tuple<SiloAddress, ActivationId>>(), GrainInfo.NO_ETAG);
            }

            if (retries <= 0)
                throw new OrleansException("Silo " + router.MyAddress + " is not the owner of the grain " + 
                    grain + " Owner=" + owner);

            if (logger.IsVerbose2) logger.Verbose2("Retry " + retries + " RemoteGrainDirectory.LookUp for Grain=" + grain + " at Owner=" + owner);
            
            PrepareForRetry(retries);
            await Task.Delay(RETRY_DELAY);

            SiloAddress o = router.CalculateTargetSilo(grain, false);
            if (router.MyAddress.Equals(o))
            {
                router.LocalDirectoryLookups.Increment();
                var res = partition.LookUpGrain(grain);
                if (res == null)
                    return new Tuple<List<Tuple<SiloAddress, ActivationId>>, int>(
                        new List<Tuple<SiloAddress, ActivationId>>(), GrainInfo.NO_ETAG);

                router.LocalDirectorySuccesses.Increment();
                return res;
            }
            router.RemoteLookupsSent.Increment();
            return await GetDirectoryReference(o).LookUp(grain, retries - 1);
        }

        public Task<List<Tuple<GrainId, int, List<Tuple<SiloAddress, ActivationId>>>>> LookUpMany(List<Tuple<GrainId, int>> grainAndETagList, int retries)
        {
            router.CacheValidationsReceived.Increment();
            if (logger.IsVerbose2) logger.Verbose2("LookUpMany for {0} entries", grainAndETagList.Count);

            var result = new List<Tuple<GrainId, int, List<Tuple<SiloAddress, ActivationId>>>>();

            foreach (Tuple<GrainId, int> tuple in grainAndETagList)
            {
                int curGen = partition.GetGrainETag(tuple.Item1);
                if (curGen == tuple.Item2 || curGen == GrainInfo.NO_ETAG)
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
                        result.Add(new Tuple<GrainId, int, List<Tuple<SiloAddress, ActivationId>>>(tuple.Item1, GrainInfo.NO_ETAG, null));
                    }
                }
            }
            return Task.FromResult(result);
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
