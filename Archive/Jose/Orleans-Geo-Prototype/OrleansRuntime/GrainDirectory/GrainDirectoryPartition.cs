using System.Collections.Generic;
using System.Linq;
using System.Text;
using System;


namespace Orleans.Runtime.GrainDirectory
{
    [Serializable]
    internal class ActivationInfo : IActivationInfo
    {
        public ActivationStatus Status { get; private set; }
        public SiloAddress SiloAddress { get; private set; }
        public DateTime TimeCreated { get; private set; }
       

        public ActivationInfo(SiloAddress siloAddress, ActivationStatus ownershipstatus)
        {
            this.SiloAddress = siloAddress;
            this.TimeCreated = DateTime.UtcNow;
            this.Status = ownershipstatus;
        }

        public ActivationInfo(IActivationInfo iActivationInfo)
        {
            SiloAddress = iActivationInfo.SiloAddress;
            TimeCreated = iActivationInfo.TimeCreated;
            Status = iActivationInfo.Status;
        }

        public override string ToString()
        {
            return String.Format("{0}, {1}", SiloAddress, TimeCreated);
        }

        public void UpdateStatus(ActivationStatus status)
        {
            this.Status = status;
        }
    }
    
    [Serializable]
    internal class GrainInfo : IGrainInfo
    {
        public Dictionary<ActivationId, IActivationInfo> Instances { get; private set; }
        public int VersionTag { get; private set; }
        public bool SingleInstance { get; private set; }

        private static readonly SafeRandom rand;

        static GrainInfo()
        {
            rand = new SafeRandom();
        }

        internal GrainInfo()
        {
            Instances = new Dictionary<ActivationId, IActivationInfo>();
            VersionTag = 0;
            SingleInstance = false;
        }

        public void AddActivation(ActivationId act, SiloAddress silo, ActivationStatus type)
        {
            if (SingleInstance && (Instances.Count > 0) && !Instances.ContainsKey(act))
            {
                throw new InvalidOperationException(
                    "Attempting to add a second activation to an existing grain in single activation mode");
            }
            Instances[act] = new ActivationInfo(silo, type);
            VersionTag = rand.Next();
        }

        // We call this method when we're trying to register an activation for a grain. If an activation for the grain
        // already exists, we return the pre-existing activation. Otherwise, we add the specified activation, "act" to 
        // the directory, and set its state to REQUESTED_OWNERSHIP.
        public bool AddSingleActivation(GrainId grain, ActivationId act, SiloAddress silo, out ActivationAddress addr)
        {
            SingleInstance = true;

            // If we already have a pre-existing activation, return it.
            if (Instances.Count > 0)
            {
                var item = Instances.First();
                addr = ActivationAddress.GetAddress(item.Value.SiloAddress, grain, item.Key);
                return false;
            }
            else
            {
                // We don't know about any activations for the grain. We therefore accept the ActivationAddress "act", and
                // add it to the directory in state REQUESTED_OWNERSHIP.
                Instances.Add(act, new ActivationInfo(silo, ActivationStatus.REQUESTED_OWNERSHIP));
                VersionTag = rand.Next();
                addr = ActivationAddress.GetAddress(silo, grain, act);
                return true;
            }
        }

        public ActivationAddress AddSingleActivationForce(GrainId grain, ActivationId act, SiloAddress silo,
            ActivationStatus status)
        {
            SingleInstance = true;
            if (Instances.Count > 0)
            {
                var item = Instances.First();
                return ActivationAddress.GetAddress(item.Value.SiloAddress, grain, item.Key);
            }
            else
            {
                Instances.Add(act, new ActivationInfo(silo, status));
                VersionTag = rand.Next();
                return ActivationAddress.GetAddress(silo, grain, act);
            }
        }

        public bool RemoveCachedActivation(ActivationAddress addr)
        {
            if (!SingleInstance)
            {
                throw new OrleansException("Method expects single instance grain");
            }

            IActivationInfo info;
            if (Instances.TryGetValue(addr.Activation, out info))
            {
                if (info.Status == ActivationStatus.CACHED)
                {
                    Instances.Remove(addr.Activation);
                    VersionTag = rand.Next();
                }
            }
            return Instances.Count == 0;
        }

        public bool RemoveActivation(ActivationId act, bool force)
        {
            if (force)
            {
                Instances.Remove(act);
                VersionTag = rand.Next();                
            }
            else
            {
                if (Silo.CurrentSilo.OrleansConfig.Globals.DirectoryLazyDeregistrationDelay > TimeSpan.Zero)
                {
                    IActivationInfo info;
                    if (Instances.TryGetValue(act, out info))
                    {
                        if (info.TimeCreated >= DateTime.UtcNow - Silo.CurrentSilo.OrleansConfig.Globals.DirectoryLazyDeregistrationDelay)
                        {
                            Instances.Remove(act);
                            VersionTag = rand.Next();
                        }
                    }
                }
            }
            return Instances.Count == 0;
        }

        public bool Merge(GrainId grain, IGrainInfo other)
        {
            bool modified = false;
            foreach (var pair in other.Instances)
            {
                if (!Instances.ContainsKey(pair.Key))
                {
                    Instances[pair.Key] = new ActivationInfo(pair.Value.SiloAddress, pair.Value.Status);
                    modified = true;
                }
            }
            if (modified)
            {
                VersionTag = rand.Next();
            }
            if (SingleInstance && (Instances.Count > 0))
            {
                // Grain is supposed to be in single activation mode, but we have two activations!!
                // Eventually we should somehow delegate handling this to the silo, but for now, we'll arbitrarily pick one value.
                // TODO: delegate multiple activation handling to the silo
                var orderedActivations = Instances.OrderBy(pair => pair.Key);
                var activationToKeep = orderedActivations.First();
                var activationsToDrop = orderedActivations.Skip(1);
                Instances.Clear();
                Instances.Add(activationToKeep.Key, activationToKeep.Value);
                var list = new List<ActivationAddress>(1);
                foreach (var activation in activationsToDrop.Select(keyValuePair => ActivationAddress.GetAddress(keyValuePair.Value.SiloAddress, grain, keyValuePair.Key)))
                {
                    list.Add(activation);
                    CatalogFactory.GetSystemTarget(Constants.CatalogId, activation.Silo).
                        DeleteActivationsLocal(list).Ignore();

                    list.Clear();
                }
                return true;
            }
            return false;
        }
   
        // This method is called when we find that a remote cluster has already created an activation for grain. 
        // Our directory partition contains an entry which maps grain => originalId. originalId corresponds to 
        // the local activation of the grain. Now that we have found that a remote cluster has already activated
        // the grain, remove originalId, and replace it with remoteId, which corresponds to the ActivationId of 
        // the remote activation.
        public void CacheAddress(GrainId grain, ActivationId remoteId, SiloAddress silo, ActivationId originalId)
        {
            IActivationInfo info;

            // We expect this method to be called on single instance grains. What does it mean 
            if (!SingleInstance)
            {
                throw new OrleansException("Method expects a single instance grain");
            }
              
            // The grain directory must contain the original activation. If this condition fails, it's a 
            // _SERIOUS BUG_.
            if (!Instances.TryGetValue(originalId, out info))
            {
                throw new OrleansException("Couldn't find activation info!");
            }

            // We only cache an activation address if we are in the process of registering our local activation, and 
            // find that another cluster has already created the activation.
            if (info.Status != ActivationStatus.REQUESTED_OWNERSHIP &&
                info.Status != ActivationStatus.RACE_LOSER)
            {
                throw new OrleansException("Activation's state must be either RACE_LOSER or REQUESTED_OWNERSHIP.");
            }

            // Remove our local activation, and then add the remote activation of the grain.
            Instances.Remove(originalId);
            Instances.Add(remoteId, new ActivationInfo(silo, ActivationStatus.CACHED));
        }

        public bool RemoveActivation(ActivationAddress addr)
        {
            return RemoveActivation(addr.Activation, true);
        }
    }

    internal class GrainDirectoryPartition
    {
        // TODO: should we change this to SortedList<> or SortedDictionary so we can extract chunks better for shipping the full
        // parition to a follower, or should we leave it as a Dictionary to get O(1) lookups instead of O(log n), figuring we do
        // a lot more lookups and so can sort periodically?
        /// <summary>
        /// contains a map from grain to its list of activations along with the version (etag) counter for the list
        /// </summary>
        private Dictionary<GrainId, IGrainInfo> partitionData;
        private Dictionary<ActivationId, GrainId> doubtfulIndex;

        private List<ActivationId> remoteWinnerActivations; 

        private readonly object lockable;
        private readonly Logger log;
        private ISiloStatusOracle membership;
        private int clusterId;

        internal int Count { get { return partitionData.Count; } }

        internal GrainDirectoryPartition(int clusterId)
        {
            partitionData = new Dictionary<GrainId, IGrainInfo>();
            doubtfulIndex = new Dictionary<ActivationId, GrainId>();

            remoteWinnerActivations = new List<ActivationId>();

            lockable = new object();
            log = Logger.GetLogger("DirectoryPartition");
            membership = Silo.CurrentSilo.LocalSiloStatusOracle;
            this.clusterId = clusterId;
        }

        private bool IsValidSilo(SiloAddress silo)
        {
            if (membership == null)
            {
                membership = Silo.CurrentSilo.LocalSiloStatusOracle;
            }
            return membership.IsValidSilo(silo);
        }

        internal void Clear()
        {
            lock (lockable)
            {
                partitionData.Clear();
            }
        }

        /// <summary>
        /// Returns all entries stored in the partition as an enumerable collection
        /// </summary>
        /// <returns></returns>
        public Dictionary<GrainId, IGrainInfo> GetItems()
        {
            lock (lockable)
            {
                return partitionData.Copy();
            }
        }

        internal virtual ActivationAddress AddSystemActivation(GrainId grain, ActivationId activation,
            SiloAddress silo)
        {
            lock (lockable)
            {
                if (!partitionData.ContainsKey(grain))
                {
                    partitionData[grain] = new GrainInfo();
                }
                return partitionData[grain].AddSingleActivationForce(grain, activation, silo, ActivationStatus.SYSTEM);
            }
        }

        internal virtual ActivationAddress AddSingleActivationTest(GrainId grain, ActivationId activation, SiloAddress silo, 
            ActivationStatus status)
        {
            lock (lockable)
            {
                if (!partitionData.ContainsKey(grain))
                {
                    partitionData.Add(grain, new GrainInfo());   
                }
                var ret = partitionData[grain].AddSingleActivationForce(grain, activation, silo, ActivationStatus.DOUBTFUL);
                if (ret.Equals(ActivationAddress.GetAddress(silo, grain, activation)))
                {
                    doubtfulIndex.Add(activation, grain);
                }
                return ret;
            }
        }

        /// <summary>
        /// Adds a new activation to the directory partition
        /// </summary>
        internal virtual bool AddSingleActivation(GrainId grain, ActivationId activation, SiloAddress silo, out ActivationAddress addr)
        {
            if (log.IsVerbose3) log.Verbose3("Adding single activation for grain {0}{1}{2}", silo, grain, activation);
            if (!IsValidSilo(silo))
            {
                throw new OrleansException("Got invalid silo!");
            }
            lock (lockable)
            {
                IGrainInfo info;
                if (!partitionData.TryGetValue(grain, out info))
                {
                    info = new GrainInfo();
                    partitionData[grain] = info;
                }
                return info.AddSingleActivation(grain, activation, silo, out addr);
            }
        }

        internal void RemoveCachedActivation(ActivationAddress address)
        {
            var grain = address.Grain;
            lock (lockable)
            {
                if (partitionData.ContainsKey(grain) &&
                    partitionData[grain].RemoveCachedActivation(address))
                {
                    partitionData.Remove(grain);
                }
            }
        }

        // Go through the set of remote doubtful activations, and check if this grain directory partition contains any conflicting
        // activations for the grain.
        internal List<ActivationAddress> ResolveDoubtfulActivations(Dictionary<ActivationId, GrainId> doubtful, 
            int sendingCluster)
        {
            // Return a set of activations we need to kill.
            var toKill = new List<ActivationAddress>();
            lock (lockable)
            {
                // Analyze all remote doubtful activation.
                foreach (var keyValue in doubtful)
                {
                    var actId = keyValue.Key;
                    var grainId = keyValue.Value;

                    IGrainInfo info;
                    if (partitionData.TryGetValue(grainId, out info))
                    {
                        var act = info.Instances.First();
                        var actInfo = act.Value;

                        // If our grain directory partition contains an activation of the grain in state OWNED or in state DOUBTFUL, then
                        // we need to pick a winner activation based on the precedence function.
                        if (actInfo.Status == ActivationStatus.DOUBTFUL || actInfo.Status == ActivationStatus.OWNED)
                        {
                            // We lose. The remote activation takes precedence. Remove the local activation from the set of DOUBTFUL
                            // activations, and update the local status to REQUESTED_OWNERSHIP. The reason we add activations to the
                            // remoteWinnerActivations list is that we're iterating over the dictionary, and can't remove elements while
                            // we're iterating. 
                            if (!ActivationPrecedenceFunc(grainId, clusterId, sendingCluster))
                            {
                                doubtfulIndex.Remove(act.Key);
                                actInfo.UpdateStatus(ActivationStatus.REQUESTED_OWNERSHIP);
                                toKill.Add(ActivationAddress.GetAddress(actInfo.SiloAddress, grainId, act.Key));
                                remoteWinnerActivations.Add(actId);
                            }
                        }
                    }
                    else
                    {
                        // Our grain directory partition does not contain an activation of the grain. Add the activation to 
                        // remoteWinnerActivations.
                        remoteWinnerActivations.Add(actId);
                    }
                }

                // Finally, remove all winner activations from doubtful.
                foreach (var actId in remoteWinnerActivations)
                {
                    doubtful.Remove(actId);
                }
                remoteWinnerActivations.Clear();
            }
            return toKill;
        }

        /// <summary>
        /// Removes an activation of the given grain from the partition
        /// </summary>
        /// <param name="grain"></param>
        /// <param name="activation"></param>
        /// <param name="force"></param>
        internal void RemoveActivation(GrainId grain, ActivationId activation, bool force)
        {
            lock (lockable)
            {
                if (partitionData.ContainsKey(grain) && partitionData[grain].RemoveActivation(activation, force))
                {
                    partitionData.Remove(grain);
                }
                doubtfulIndex.Remove(activation);
            }
            if (log.IsVerbose3) log.Verbose3("Removing activation for grain {0}", grain.ToString());
        }

        // This method is called in order to run the grain resolution algorithm. It just returns a copy of the doubtfulIndex.
        internal Dictionary<ActivationId, GrainId> GetDoubtfulActivations()
        {
            lock (lockable)
            {
                return doubtfulIndex.Copy();
            }
        }

        // ProcessAntiEntropyResults is called once the gateway silo of this cluster has obtained the results of the anti-entropy 
        // protocol. This function removes all loser activations from the grain directory, and converts the state of winner activations
        // from DOUBTFUL to OWNED. The function returns a list of ActivationAddresses. These addresses correspond to DOUBTFUL activations 
        // that must be de-activated.
        internal List<ActivationAddress> ProcessAntiEntropyResults(
            Dictionary<ActivationId, GrainId> loserActivations, Dictionary<ActivationId, GrainId> winnerActivations)
        {
            // Create an empty list of activation addresses to return.
            var ret = new List<ActivationAddress>();
            lock (lockable)
            {
                // We first get rid of all loser activations. 
                if (loserActivations != null)
                {
                    foreach (var keyValue in loserActivations)
                    {
                        var actId = keyValue.Key;
                        var grain = keyValue.Value;
                        IGrainInfo info;

                        if (partitionData.TryGetValue(grain, out info))
                        {
                            IActivationInfo actInfo;
                            if (info.Instances.TryGetValue(actId, out actInfo))
                            {
                                // When we encounter the DOUBTFUL activation, we don't immediately remove it from the grain directory.
                                // Instead, we convert the activation status to REQUESTED_OWNERSHIP, and re-run the activation creation
                                // protocol for this activation.The activation creation protocol will then cache the appropriate remote
                                // activation in our grain directory. We also remove the activation from doubtfulIndex because we have
                                // changed its state from DOUBTFUL to REQUESTED_OWNERSHIP.
                                if (actInfo.Status == ActivationStatus.DOUBTFUL)
                                {
                                    // log.Info("Removed DOUBTFUL activation for grain {0}", grain);
                                    actInfo.UpdateStatus(ActivationStatus.REQUESTED_OWNERSHIP);
                                    ret.Add(ActivationAddress.GetAddress(actInfo.SiloAddress, grain, actId));
                                    doubtfulIndex.Remove(actId);
                                }
                            }
                        }
                    } 
                }

                // Convert activations from doubtful to owned.
                if (winnerActivations != null)
                {
                    foreach (var keyValue in winnerActivations)
                    {
                        var actId = keyValue.Key;
                        var grain = keyValue.Value;
                        IGrainInfo grainInfo;

                        if (partitionData.TryGetValue(grain, out grainInfo))
                        {
                            IActivationInfo actInfo;
                            if (grainInfo.Instances.TryGetValue(actId, out actInfo))
                            {
                                // When we encounter the DOUBTFUL activation, just update its status to OWNED. We also remove it from
                                // the doubtfulIndex because it's no longer DOUBTFUL.
                                if (actInfo.Status == ActivationStatus.DOUBTFUL)
                                {
                                    // log.Info("Now own DOUBTFUL grain {0}", grain);
                                    actInfo.UpdateStatus(ActivationStatus.OWNED);
                                    doubtfulIndex.Remove(actId);
                                }
                            }
                        }
                    }
                }
            }
            return ret;
        }

        /// <summary>
        /// Removes the grain (and, effectively, all its activations) from the diretcory
        /// </summary>
        /// <param name="grain"></param>
        internal void RemoveGrain(GrainId grain)
        {
            lock (lockable)
            {
                if (partitionData.ContainsKey(grain))
                {
                    var instances = partitionData[grain].Instances;
                    foreach (var keyValue in instances)
                    {
                        var actId = keyValue.Key;
                        doubtfulIndex.Remove(actId);
                    }
                }
                partitionData.Remove(grain);
            }
            if (log.IsVerbose3) log.Verbose3("Removing grain {0}", grain.ToString());
        }

        // Precedence function to resolve races among clusters that are trying to create an activation for a particular grain.
        // The calling convention is as follows: grain is the GrainID under consideration. The function returns "true" if clusterLeft
        // has precedence over clusterRight.
        internal static bool ActivationPrecedenceFunc(GrainId grain, int clusterLeft, int clusterRight)
        {
            // Make sure that we're not calling this function with default cluster identifiers.
            if (clusterLeft == -1 || clusterRight == -1)
            {
                throw new OrleansException("ActivationPrecedenceFunction must be called with valid cluster identifiers.");
            }

            var precLeft = grain.GetUniformHashCode() ^ clusterLeft.GetHashCode();
            var precRight = grain.GetUniformHashCode() ^ clusterRight.GetHashCode();
            return (precLeft < precRight) || (precLeft == precRight && clusterLeft < clusterRight);
        }

        /// <summary>
        ///  This function gets called when a remote cluster is checking whether this cluster contains an activation for 
        ///  a grain. It takes as input the GrainId that the remote cluster is asking us about, and the remote cluster's identifer,
        ///  remoteClusterId.
        /// </summary>
        /// <param name="grain"></param>
        /// <param name="remoteClusterId"></param>
        /// <returns></returns>
        internal Tuple<ActivationResponseStatus, ActivationAddress> GrainStatusResponse(GrainId grain, int remoteClusterId)
        {
            lock (lockable)
            {
                IGrainInfo info;

                // Check if we contain an activation for the grain.
                if (partitionData.TryGetValue(grain, out info))
                {
                    // Take the first activation we find from the ActivationInfo associated with the grain. 
                    // Taking the first activation from the ActivationInfo implicitly assumes that the grain can only
                    // have a single activation. 
                    if (!info.SingleInstance)
                    {
                        throw new OrleansException("Geo-distributed grains must be single instance");
                    }
                    var activation = info.Instances.First();
                    var actId = activation.Key;
                    var actInfo = activation.Value;
                    
                    switch (actInfo.Status)
                    {
                        // If we have already activated the grain, tell the remote cluster.
                        case ActivationStatus.OWNED:
                        case ActivationStatus.DOUBTFUL:
                            return Tuple.Create(ActivationResponseStatus.FAILED,
                                ActivationAddress.GetAddress(actInfo.SiloAddress, grain, actId));

                        // If this activation is in state REQUESTED_OWNERSHIP, then it means that this cluster
                        // is trying to concurrently create an activation for the grain. This is a race condition.
                        case ActivationStatus.REQUESTED_OWNERSHIP:

                            // Use the precedence function to decide which cluster wins the race condition.
                            if (!ActivationPrecedenceFunc(grain, clusterId, remoteClusterId))
                            {
                                // This cluster does not have precedence. Change the state of its activation to RACE_LOSER to
                                // indicate that it has lost a race condition.
                                actInfo.UpdateStatus(ActivationStatus.RACE_LOSER);
                                return Tuple.Create(ActivationResponseStatus.PASS, (ActivationAddress)null);
                            }
                            else
                            {
                                // This cluster has precedence. We indicate so by specifying a "-1" to the third argument
                                // to CreateLookupResponse.
                                return Tuple.Create(ActivationResponseStatus.FAILED, (ActivationAddress)null);
                            }

                        // If this cluster's activation of the grain is in state RACE_LOSER or CACHED, then we respond as
                        // if we have never heard about the grain before (as required by the grain activation protocol).
                        default:
                            return Tuple.Create(ActivationResponseStatus.PASS, (ActivationAddress)null);
                    }
                }
                else
                {
                    // This cluster does not contain an activation for the grain. 
                    return Tuple.Create(ActivationResponseStatus.PASS, (ActivationAddress)null);
                }
            }
        }

        /// <summary>
        /// Returns a list of activations (along with the version number of the list) for the given grain.
        /// If the grain is not found, null is returned.
        /// </summary>
        /// <param name="grain"></param>
        /// <returns></returns>
        internal Tuple<List<Tuple<SiloAddress, ActivationId>>, int> LookUpGrain(GrainId grain)
        {
            lock (lockable)
            {
                if (partitionData.ContainsKey(grain))
                {
                    var result = new Tuple<List<Tuple<SiloAddress, ActivationId>>, int>(
                            new List<Tuple<SiloAddress, ActivationId>>(), partitionData[grain].VersionTag);

                    foreach (var route in partitionData[grain].Instances)
                    {
                        if (IsValidSilo(route.Value.SiloAddress))
                        {
                            result.Item1.Add(new Tuple<SiloAddress, ActivationId>(route.Value.SiloAddress, 
                                                                                                  route.Key));
                        }
                    }
                    return result;
                }
            }
            return null;
        }

        /// <summary>
        /// Returns the version number of the list of activations for the grain.
        /// If the grain is not found, -1 is returned.
        /// </summary>
        /// <param name="grain"></param>
        /// <returns></returns>
        internal int GetGrainETag(GrainId grain)
        {
            lock (lockable)
            {
                if (partitionData.ContainsKey(grain))
                {
                    return partitionData[grain].VersionTag;
                }
                else
                {
                    return -1;
                }
            }
        }

        /// <summary>
        /// Merges one partition into another, asuuming partitions are disjoint.
        /// This method is supposed to be used by replication manager to update replicated partitions when the system view (set of live silos) changes.
        /// </summary>
        /// <param name="other"></param>
        internal void Merge(GrainDirectoryPartition other)
        {
            // Find out the set of doubtful activations received.
            var doubtfulDelta = new Dictionary<ActivationId, GrainId>();
            foreach (var pair in other.partitionData)
            {
                var grainId = pair.Key;
                var grainInfo = pair.Value;

                foreach (var instance in grainInfo.Instances)
                {
                    var actId = instance.Key;
                    var actInfo = instance.Value;

                    if (actInfo.Status == ActivationStatus.DOUBTFUL)
                    {
                        doubtfulDelta.Add(actId, grainId);
                    }
                }
            }

            lock (lockable)
            {
                foreach (var pair in other.partitionData)
                {
                    if (partitionData.ContainsKey(pair.Key))
                    {
                        if (log.IsVerbose) log.Verbose("While merging two disjoint partitions, same grain " + pair.Key + " was found in both partitions");
                        partitionData[pair.Key].Merge(pair.Key, pair.Value);
                    }
                    else
                    {
                        partitionData.Add(pair.Key, pair.Value);
                    }
                }

                foreach (var pair in doubtfulDelta)
                {
                    if (log.IsVerbose) log.Verbose(string.Format("Got two activations with the same activationId: {0}; for grain {1}",
                        pair.Key, pair.Value));
                    doubtfulIndex.Add(pair.Key, pair.Value);
                }
            }
        }

        internal ActivationAddress TransferSingleActivation(GrainId grain, ActivationId activation, SiloAddress silo,
            ActivationStatus status)
        {
            lock (lockable)
            {
                IGrainInfo info = null;
                if (!partitionData.TryGetValue(grain, out info))
                {
                    info = new GrainInfo();
                    partitionData[grain] = info;
                }
                return info.AddSingleActivationForce(grain, activation, silo, status);
            }
        }

        internal void TransferActivation(GrainId grain, ActivationId activation, SiloAddress silo,
            ActivationStatus status)
        {
            if (status == ActivationStatus.REQUESTED_OWNERSHIP)
            {
                
            }

            // Currently can't deal with multiple activations!
            lock (lockable)
            {
                IGrainInfo info = null;
                if (!partitionData.TryGetValue(grain, out info))
                {
                    info = new GrainInfo();
                    partitionData[grain] = info;
                }
                info.AddActivation(activation, silo, status);
            }
        }

        /// <summary>
        /// Runs through all entries in the partition and moves/copies (depending on the given flag) the
        /// entries satisfying the given predicate into a new partition.
        /// This method is supposed to be used by replication manager to update replicated partitions when the system view (set of live silos) changes.
        /// </summary>
        /// <param name="predicate">filter predicate (usually if the given grain is owned by particular silo)</param>
        /// <param name="modifyOrigin">flag controling whether the source partition should be modified (i.e., the entries should be moved or just copied) </param>
        /// <returns>new grain directory partition containing entries satisfying the given predicate</returns>
        internal GrainDirectoryPartition Split(Predicate<GrainId> predicate, bool modifyOrigin)
        {
            var newDirectory = new GrainDirectoryPartition(clusterId);

            if (modifyOrigin)
            {
                // SInce we use the "pairs" list to modify the underlying collection below, we need to turn it into an actual list here
                List<KeyValuePair<GrainId, IGrainInfo>> pairs;
                lock (lockable)
                {
                    pairs = partitionData.Where(pair => predicate(pair.Key)).ToList();
                }
                
                foreach (var pair in pairs)
                {
                    newDirectory.partitionData.Add(pair.Key, pair.Value);
                }

                lock (lockable)
                {
                    foreach (var pair in pairs)
                    {
                        partitionData.Remove(pair.Key);
                    }
                }
            }
            else
            {
                lock (lockable)
                {
                    foreach (var pair in partitionData.Where(pair => predicate(pair.Key)))
                    {
                        newDirectory.partitionData.Add(pair.Key, pair.Value);
                    }
                }
            }

            return newDirectory;
        }

        internal List<ActivationAddress> ToListOfActivations(
            bool singleActivation)
        {
            var result = new List<ActivationAddress>();
            lock (lockable)
            {
                foreach (var pair in partitionData)
                {
                    var grain = pair.Key;
                    if (pair.Value.SingleInstance == singleActivation)
                    {
                        foreach (var actPair in pair.Value.Instances)
                        {
                            result.Add(ActivationAddress.GetAddress(actPair.Value.SiloAddress, grain, actPair.Key));
                        }
                    }
                }
                return result;
            }
        }

        /// <summary>
        /// Sets the internal parition dictionary to the one given as input parameter.
        /// This method is supposed to be used by replication manager to update replicated partition with a new replica.
        /// </summary>
        /// <param name="newPartitionData">new internal partition dictionary</param>
        internal void Set(Dictionary<GrainId, IGrainInfo> newData)
        {
            var newDoubtful = new Dictionary<ActivationId, GrainId>();
            foreach (var grainKeyValue in newData)
            {
                var grain = grainKeyValue.Key;
                var grainInfo = grainKeyValue.Value;

                foreach (var instance in grainInfo.Instances)
                {
                    if (instance.Value.Status == ActivationStatus.DOUBTFUL)
                    {
                        newDoubtful.Add(instance.Key, grain);
                    }
                }
            }

            lock (lockable)
            {
                partitionData = newData;
                doubtfulIndex = newDoubtful;
            }
        }

        // This function is called when running the activation creation protocol, if all clusters respond to our activation request with
        // PASS, then we can upgrade the status of our activation from REQUESTED_OWNERSHIP to OWNED/DOUBTFUL. One subtlety is that
        // our local activation may have been changed to state RACE_LOSER while we were running the protocol. If this is the case, 
        // we change the state of our local activation from RACE_LOSER to REQUESTED_OWNERSHIP, and return false. By returning false, the
        // caller knows that the call to TakeOwnership was unsuccessful, and re-executes the activation creation protocol. If we are
        // able to successfully take ownership of the grain, we return true.
        internal bool TakeOwnership(GrainId grain, ActivationId id, SiloAddress silo, ActivationStatus status)
        {
            // Status must be OWNED or DOUBTFUL.
            if (status != ActivationStatus.OWNED && status != ActivationStatus.DOUBTFUL)
            {
                throw new OrleansException("ActivationStatus must be either OWNED or DOUBTFUL");    
            }

            lock (lockable)
            {
                IGrainInfo info;

                // If we can't find the grain in our directory, then the grain's directory entry was moved to a remote silo.
                // Throw an exception and stop trying to register the grain's activation, the remote silo is in charge of it now.
                if (!partitionData.TryGetValue(grain, out info))
                {
                    throw new OrleansException(string.Format("Couldn't find grain {0} in my directory. Stop trying to register it.", grain));    
                }

                // Get the info associated with the activation.
                var activation = info.Instances.First();
                var actId = activation.Key;
                var actInfo = activation.Value;

                // The activation _must_ be in either one of requested_ownership or race_loser. Only the calling context can change
                // the Status of the activation to OWNED, DOUBTFUL, or CACHED. This is a _SERIOUS BUG_
                if ((actInfo.Status != ActivationStatus.REQUESTED_OWNERSHIP &&
                    actInfo.Status != ActivationStatus.RACE_LOSER) || !actId.Equals(id))
                {
                    throw new OrleansException(string.Format("Grain {0}'s activation, {1}, is in an unexpected state.", grain, actId));
                }

                // Our attempt to create the activation raced with another cluster. Change the ownership status of our local activation to
                // REQUESTED_OWNERSHIP, and notify the caller to re-execute the activation creation protocol.
                if (actInfo.Status == ActivationStatus.RACE_LOSER)
                {
                    actInfo.UpdateStatus(ActivationStatus.REQUESTED_OWNERSHIP);
                    return false;
                }

                // If we've reached this point,  we can go ahead and mark the activation as either OWNED or DOUBTFUL. If status is 
                // DOUBTFUL, then track the activation in the doubtfulIndex.
                if (status == ActivationStatus.DOUBTFUL)
                {
                    doubtfulIndex.Add(actId, grain);
                }
                actInfo.UpdateStatus(status);
                return true;
            }
        }

        // This function is called while running the activation creation protocol, when we are unable to create a local activation for a 
        // grain, because another cluster has already activated the grain. We first remove the activation which we were trying to activate
        // from the grain directory, and then store a reference to the remote activation with state CACHED.
        internal void CacheAddress(GrainId grain, ActivationId id, SiloAddress silo, ActivationId originalId)
        {
            lock (lockable)
            {
                IGrainInfo info;

                // If we can't find the grain in our directory, then the grain's directory entry was moved to a remote silo.
                // Throw an exception and stop trying to register the grain's activation, the remote silo is in charge of it now.
                if (!partitionData.TryGetValue(grain, out info))
                {
                    throw new OrleansException(string.Format("Couldn't find grain {0} in my directory. Stop trying to register it.", grain));
                }

                // Get the info associated with the activation.
                var activation = info.Instances.First();
                var actId = activation.Key;
                var actInfo = activation.Value;

                // The activation _must_ be in either one of REQUESTED_OWNERSHIP or RACE_LOSER. Only the calling context can change
                // the Status of the activation to OWNED, DOUBTFUL, or CACHED. It's a _SERIOUS BUG_ if the activation is not in state
                // REQUESTED_OWNERSHIP or RACE_LOSER.
                if (actInfo.Status != ActivationStatus.REQUESTED_OWNERSHIP &&
                    actInfo.Status != ActivationStatus.RACE_LOSER)
                {
                    throw new OrleansException(string.Format("Grain {0}'s activation, {1}, is in an unexpected state.", grain, actId));
                }

                // Finally, remove our local activation from the grain directory, and add the remote activation.
                info.CacheAddress(grain, id, silo, originalId);
            }
        }

        /// <summary>
        /// Updates partition with a new delta of changes.
        /// This method is supposed to be used by replication manager to update replicated partition with a set of delta changes.
        /// </summary>
        /// <param name="newPartitionDelta">dictionary holding a set of delta updates to this partition.
        /// If the value for a given key in the delta is valid, then existing entry in the partition is replaced.
        /// Otherwise, i.e., if the value is null, the corresponding entry is removed.
        /// </param>
        internal void Update(Dictionary<GrainId, IGrainInfo> newDelta)
        {
            lock (lockable)
            {
                foreach (GrainId grain in newDelta.Keys)
                {
                    if (newDelta[grain] != null)
                    {
                        partitionData[grain] = newDelta[grain];
                    }
                    else
                    {
                        partitionData.Remove(grain);
                    }
                }
            }
        }

        public override string ToString()
        {
            var sb = new StringBuilder();

            lock (lockable)
            {
                foreach (var grainEntry in partitionData)
                {
                    foreach (var activationEntry in grainEntry.Value.Instances)
                    {
                        sb.Append("    ").Append(grainEntry.Key.ToString()).Append("[" + grainEntry.Value.VersionTag + "]").
                            Append(" => ").Append(activationEntry.Key.ToString()).
                            Append(" @ ").AppendLine(activationEntry.Value.ToString());
                    }
                }
            }

            return sb.ToString();
        }
    }
}
