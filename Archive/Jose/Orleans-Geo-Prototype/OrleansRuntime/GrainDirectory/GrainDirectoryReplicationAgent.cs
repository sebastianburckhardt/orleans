using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;

using Orleans.Scheduler;
//using IGrainInfo = Orleans.Runtime.RuntimeGrains.IGrainInfo;
using Orleans.Runtime.Scheduler;


namespace Orleans.Runtime.GrainDirectory
{
    // TODO ageller: Should this run inside of the remote directory system target, or as a system target of its own?
    // Right now it's an async agent, which means it has its own thread and runs asynchronously from the remote directory system target.
    // This might be fine, or it might be better to run this as a system target itself; that would allow all of the synchronization code
    // (MethodImplOptions.Synchronized) to be removed.
    /// <summary>
    /// Most methods of this class are synchronized since they might be called both
    /// by the replication agent, i.e., from Run(), and from LocalGrainDirectory
    /// </summary>
    internal class GrainDirectoryReplicationAgent : AsynchAgent
    {
        private readonly TimeSpan sleepTimeBetweenRefreshes;
        // how frequently send full replica instead of deltas (-1 means never do that)
        // TODO: reset to some reasonable number (100? 1000?) once full replica chunking is enabled
        private int FULL_REPLICATION_UPDATE_CYCLE = -1;
        private const int REPLICATION_CHUNK_SIZE = 500;
        private int replicationCyclesCounter;

        private readonly LocalGrainDirectory localDirectory;

        private readonly Dictionary<SiloAddress, GrainDirectoryPartition> replicatedPartitions;
        private Dictionary<GrainId, int> localDirectoryReplicationMap;
        private readonly List<SiloAddress> silosHoldingMyPartition;

        private readonly Dictionary<SiloAddress, AsyncCompletion> lastPromise;

        internal GrainDirectoryReplicationAgent(LocalGrainDirectory localDirectory, GlobalConfiguration config)
        {
            this.localDirectory = localDirectory;
            sleepTimeBetweenRefreshes = config.DirectoryReplicationPeriod;

            replicatedPartitions = new Dictionary<SiloAddress, GrainDirectoryPartition>();
            localDirectoryReplicationMap = new Dictionary<GrainId, int>();
            silosHoldingMyPartition = new List<SiloAddress>();
            lastPromise = new Dictionary<SiloAddress, AsyncCompletion>();
        }

        protected override void Run()
        {
            while (localDirectory.running)
            {
                replicationCyclesCounter++;

                if (FULL_REPLICATION_UPDATE_CYCLE != -1 && replicationCyclesCounter % FULL_REPLICATION_UPDATE_CYCLE == 0)
                {
                    DoFullReplication();
                }
                else
                {
                    DoDeltaReplication();
                }

                Thread.Sleep(sleepTimeBetweenRefreshes);
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        internal List<ActivationAddress> GetReplicatedInfo(GrainId grain)
        {
            foreach (var replica in replicatedPartitions.Values)
            {
                var result = replica.LookUpGrain(grain);
                if (result != null)
                {
                    // Force the list to be created in order to avoid race conditions
                    return result.Item1.Select(pair => ActivationAddress.GetAddress(pair.Item1, grain, pair.Item2)).ToList();
                }
            }

            return null;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        protected void DoFullReplication()
        {
            PrintStatus("FULL Replication started");

            // take a copy of the current directory partition
            Dictionary<GrainId, IGrainInfo> batchUpdate =
                localDirectory.DirectoryPartition.GetItems();

            var newReplicationMap = new Dictionary<GrainId, int>();
            foreach (var pair in batchUpdate)
            {
                newReplicationMap[pair.Key] = pair.Value.VersionTag;
            }

            UpdateReplicas(batchUpdate, newReplicationMap, true).Ignore();
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        protected void DoDeltaReplication()
        {
            PrintStatus("DELTA Replication started");

            var batchUpdate = new Dictionary<GrainId, IGrainInfo>();
            var newReplicationMap = new Dictionary<GrainId, int>();

            // run through all entries in my directory and check if their version has been updated
            foreach (var pair in localDirectory.DirectoryPartition.GetItems())
            {
                int currentVersion = pair.Value.VersionTag;
                // check if this is a new (non-replicated) element, or its version has been updated
                if (!localDirectoryReplicationMap.ContainsKey(pair.Key) ||
                    localDirectoryReplicationMap[pair.Key] != currentVersion)
                {
                    batchUpdate.Add(pair.Key, pair.Value);
                }
                newReplicationMap[pair.Key] = currentVersion;
            }

            // check if some entries should be removed from the replicas (they are not in the directory anymore)
            foreach (GrainId grain in localDirectoryReplicationMap.Keys.Where(grain => !newReplicationMap.ContainsKey(grain)))
            {
                batchUpdate.Add(grain, null);
            }

            UpdateReplicas(batchUpdate, newReplicationMap, false).Ignore();
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        protected AsyncCompletion UpdateReplicas(Dictionary<GrainId, IGrainInfo> batchUpdate,
            Dictionary<GrainId, int> newReplicationMap, bool isFullReplica)
        {
            if (batchUpdate.Count > 0 && silosHoldingMyPartition.Count > 0)
            {
                if (log.IsVerbose) log.Verbose("Sending {0} items to my {1} replicas: {2} (ring status is {3})", batchUpdate.Count, silosHoldingMyPartition.Count,
                    silosHoldingMyPartition.ToStrings(), localDirectory.RingStatusToString());

                var promises = new List<AsyncCompletion>();

                var n = 0;
                var chunk = new Dictionary<GrainId, IGrainInfo>();
                // Note that batchUpdate will not change while this method is executing
                foreach (var pair in batchUpdate)
                {
                    chunk[pair.Key] = pair.Value;
                    n++;
                    if ((n % REPLICATION_CHUNK_SIZE != 0) && (n != batchUpdate.Count))
                    {
                        // If we haven't filled in a chunk yet, keep looping.
                        continue;
                    }

                    foreach (SiloAddress silo in silosHoldingMyPartition)
                    {
                        SiloAddress captureSilo = silo;
                        Dictionary<GrainId, IGrainInfo> captureChunk = chunk;
                        bool captureIsFullReplica = isFullReplica;
                        Func<AsyncCompletion> sendAction = () => localDirectory.Scheduler.RunOrQueueAsyncCompletion(
                            () => AsyncCompletion.FromTask( localDirectory.GetDirectoryReference(captureSilo).RegisterReplica(
                                    localDirectory.MyAddress,
                                    null, null, 
                                    captureChunk,
                                    captureIsFullReplica)),
                            localDirectory.RemGrainDirectory.SchedulingContext);
                        if (log.IsVerbose) log.Verbose("Sending replica to " + captureSilo);
                        AsyncCompletion pendingRequest;
                        var promise = lastPromise.TryGetValue(captureSilo, out pendingRequest) ?
                            pendingRequest.FastSystemContinueWith(sendAction, ex => sendAction()) : sendAction();
                        lastPromise[captureSilo] = promise;
                        promises.Add(promise);
                    }
                    // We need to use a new Dictionary because the call to RegisterReplica, which reads the current Dictionary,
                    // happens asynchronously (and typically after some delay).
                    chunk = new Dictionary<GrainId, IGrainInfo>();

                    // TODO: this is a hack. We send a full replica by sending one chunk as a full replica and follow-on chunks as deltas.
                    // Obviously, this will really mess up if there's a failure after the first chunk but before the others are sent, since on a
                    // full replica receive the follower dumps all old data and replcaes it with the new full replica. 
                    // On the other hand, with FULL_REPLICATION_UPDATE_CYCLE set ot -1, we only send a full replica when the membership changes,
                    // which is rare, and over time things should correct themselves, so the possibility of data loss is pretty low
                    // (and, of course, losing directory data isn't necessarily catastrophic).
                    isFullReplica = false;
                }
                return AsyncCompletion.JoinAll(promises).ContinueWith(() => localDirectoryReplicationMap = newReplicationMap);
            }
            else
            {
                localDirectoryReplicationMap = newReplicationMap;
                if (log.IsVerbose) log.Verbose((isFullReplica ? "FULL" : "DELTA") + " replication finished with empty delta (nothing to send)");
                return AsyncCompletion.Done;
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        internal void ProcessSiloRemoveEvent(SiloAddress removedSilo)
        {
            if (log.IsVerbose) log.Verbose("Processing silo remove event for " + removedSilo);

            // Reset our follower list to take the changes into account
            ResetFollowers();

            // check if this is one of our successors (i.e., if I hold this silo's replica)
            // (if yes, adjust local and/or replicated directory partitions)
            if (replicatedPartitions.ContainsKey(removedSilo))
            {
                // at least one predcessor should exist, which is me
                SiloAddress predecessor = localDirectory.FindPredecessors(removedSilo, 1)[0];
                if (localDirectory.MyAddress.Equals(predecessor))
                {
                    if (log.IsVerbose) log.Verbose("Merging my partition with the replica of silo " + removedSilo);
                    // now I am responsible for this directory part
                    localDirectory.DirectoryPartition.Merge(replicatedPartitions[removedSilo]);
                    // no need to send our new partition to all replicas, as they
                    // will realize the change and combine their replicas without any additional 
                    // communication (see below)
                }
                else
                {
                    if (log.IsVerbose) log.Verbose("Merging partition of " + predecessor + " with the replica of silo " + removedSilo);
                    // adjust replica for the predcessor of the failed silo
                    replicatedPartitions[predecessor].Merge(replicatedPartitions[removedSilo]);
                }
                if (log.IsVerbose) log.Verbose("Removed replicated partition of silo " + removedSilo);
                replicatedPartitions.Remove(removedSilo);
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        internal void ProcessSiloStoppingEvent()
        {
            if (log.IsVerbose) log.Verbose("Processing silo stopping event");

            // If there are no current back-ups of my partition -- for instance, if we're configured to not use replication -- select
            // our nearest predecessor to receive our hand-off, since that's the silo that will wind up owning our partition (assuming
            // that it doesn't also fail and that no other silo joins during the transition period).
            if (silosHoldingMyPartition.Count == 0)
            {
                silosHoldingMyPartition.AddRange(localDirectory.FindPredecessors(localDirectory.MyAddress, 1));
            }
            // take a copy of the current directory partition
            Dictionary<GrainId, IGrainInfo> batchUpdate = localDirectory.DirectoryPartition.GetItems();

            var newReplicationMap = new Dictionary<GrainId, int>();
            foreach (var pair in batchUpdate)
            {
                newReplicationMap[pair.Key] = pair.Value.VersionTag;
            }

            UpdateReplicas(batchUpdate, newReplicationMap, true).ContinueWith(() => localDirectory.MarkStopPreparationCompleted(),
                ex => localDirectory.MarkStopPreparationFailed(ex)).Ignore();
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        internal void ProcessSiloAddEvent(SiloAddress addedSilo)
        {
            if (log.IsVerbose) log.Verbose("Processing silo add event for " + addedSilo);

            // Reset our follower list to take the changes into account
            ResetFollowers();

            // check if this is one of our successors (i.e., if I should hold this silo's replica)
            // (if yes, adjust local and/or replicated directory partitions by splitting them between old successors and the new one)
            // NOTE: Even when REPLICATION_FACTOR is set to 0, we need to move part of our local
            // NOTE: directory to the new silo if it is an immediate successor.
            // NOTE: This is why we have Math.Max below
            List<SiloAddress> successors = localDirectory.FindSuccessors(localDirectory.MyAddress, Math.Max(localDirectory.ReplicationFactor, 1));
            if (successors.Contains(addedSilo))
            {
                // check if this is an immediate successor
                if (successors[0].Equals(addedSilo))
                {
                    // split my local directory and send to my new immediate successor his share
                    if (log.IsVerbose) log.Verbose("Splitting my partition between me and " + addedSilo);
                    GrainDirectoryPartition splitPart = localDirectory.DirectoryPartition.Split(
                        grain =>
                        {
                            var s = localDirectory.CalculateTargetSilo(grain);
                            return (s != null) && !localDirectory.MyAddress.Equals(s);
                        }, false);
                    List<ActivationAddress> splitPartListSingle = splitPart.ToListOfActivations(true);
                    List<ActivationAddress> splitPartListMulti = splitPart.ToListOfActivations(false);

                    if (splitPartListSingle.Count > 0)
                    {
                        if (log.IsVerbose) log.Verbose("Sending " + splitPartListSingle.Count + " single activation entries to " + addedSilo);
                        localDirectory.Scheduler.QueueAsyncCompletion(() =>
                            AsyncCompletion.FromTask(localDirectory.GetDirectoryReference(successors[0]).RegisterManySingleActivation(
                                splitPartListSingle, LocalGrainDirectory.NUM_RETRIES)).ContinueWith(() =>
                                    splitPartListSingle.ForEach(activationAddress => localDirectory.DirectoryPartition.RemoveGrain(activationAddress.Grain))),
                                localDirectory.RemGrainDirectory.SchedulingContext).Ignore();
                    }

                    if (splitPartListMulti.Count > 0)
                    {
                        if (log.IsVerbose) log.Verbose("Sending " + splitPartListMulti.Count + " entries to " + addedSilo);
                        localDirectory.Scheduler.QueueAsyncCompletion(() =>
                            AsyncCompletion.FromTask(localDirectory.GetDirectoryReference(successors[0]).RegisterMany(
                                splitPartListMulti)).ContinueWith(() =>
                                    splitPartListMulti.ForEach(activationAddress => localDirectory.DirectoryPartition.RemoveGrain(activationAddress.Grain))),
                                localDirectory.RemGrainDirectory.SchedulingContext).Ignore();
                    }

                    if (localDirectory.ReplicationFactor > 0)
                    {
                        replicatedPartitions[addedSilo] = splitPart;
                    }
                }
                else
                {
                    // adjust replicated partitions by splitting them accordingly between new and old silos
                    SiloAddress predecessorOfNewSilo = localDirectory.FindPredecessors(addedSilo, 1)[0];
                    if (!replicatedPartitions.ContainsKey(predecessorOfNewSilo))
                    {
                        // we should have the partition of the predcessor of our new successor
                        log.Warn(ErrorCode.DirectoryPartitionPredecessorExpected, "This silo is expected to hold directory partition replica of " + predecessorOfNewSilo);
                    }
                    else
                    {
                        if (log.IsVerbose) log.Verbose("Splitting partition of " + predecessorOfNewSilo + " and creating a replica for " + addedSilo);
                        GrainDirectoryPartition splitPart = replicatedPartitions[predecessorOfNewSilo].Split(
                            grain =>
                            {
                                //TODO: Alan, please review the 2nd line condition.
                                var s = localDirectory.CalculateTargetSilo(grain);
                                return (s != null) && !predecessorOfNewSilo.Equals(s);
                            }, true);
                        replicatedPartitions[addedSilo] = splitPart;
                    }
                }

                // remove partition of one of the old successors that we do not need to replicate now
                SiloAddress oldSuccessor = replicatedPartitions.FirstOrDefault(pair => !successors.Contains(pair.Key)).Key;
                if (oldSuccessor != null)
                {
                    if (log.IsVerbose) log.Verbose("Removing replica of the directory partition of silo " + oldSuccessor + " (holding replica of " + addedSilo + " instead)");
                    replicatedPartitions.Remove(oldSuccessor);
                }
            }
        }

        internal void RegisterReplica(SiloAddress source, Dictionary<GrainId, IGrainInfo> partition, bool isFullCopy)
        {
            if (log.IsVerbose) log.Verbose("Got request to register " + (isFullCopy ? "FULL" : "DELTA") + " directory partition replica with " + partition.Count + " elements from " + source);

            List<SiloAddress> successors = localDirectory.FindSuccessors(localDirectory.MyAddress, localDirectory.ReplicationFactor);
            if (!successors.Contains(source))
            {
                // this might happen if there was a recent change in membership, and I am not updated yet
                log.Warn(ErrorCode.DirectoryReplicaFromUnexpectedSilo,
                    "Got new replica from unexpected silo {0} (MBR status {1}). MBR active cluster size is {2}. My ring status is: {3}.",
                    source, localDirectory.membership.GetApproximateSiloStatus(source), localDirectory.membership.GetApproximateSiloStatuses(true).Count,
                    localDirectory.RingStatusToString());
            }

            if (!replicatedPartitions.ContainsKey(source))
            {
                if (!isFullCopy)
                {
                    log.Warn(ErrorCode.DirectoryUnexpectedDelta, String.Format("Got delta replica from silo {0} (MBR status {1}) while not holding a full copy. MBR active cluster size is {2}",
                                                    source, localDirectory.membership.GetApproximateSiloStatus(source), localDirectory.membership.GetApproximateSiloStatuses(true).Count));
                }

                replicatedPartitions[source] = new GrainDirectoryPartition(localDirectory.MyAddress.ClusterId);
            }

            if (isFullCopy)
            {
                replicatedPartitions[source].Set(partition);
            }
            else
            {
                replicatedPartitions[source].Update(partition);
            }
        }

        internal void UnregisterReplica(SiloAddress source)
        {
            if (log.IsVerbose) log.Verbose("Got request to unregister directory partition replica with from " + source);

            if (!replicatedPartitions.Remove(source))
            {
                // this might happen if there was a recent change in membership
                if (log.IsVerbose) log.Verbose("Got request to unregister replica from silo {0} whose replica I do not hold", source);
            }
        }

        protected void ResetFollowers()
        {
            List<SiloAddress> predecessors = localDirectory.FindPredecessors(localDirectory.MyAddress, localDirectory.ReplicationFactor);
            var newReplicas = predecessors.FindAll(silo => !silosHoldingMyPartition.Contains(silo));
            var noLongerReplicas = silosHoldingMyPartition.FindAll(silo => !predecessors.Contains(silo));
            foreach (var follower in noLongerReplicas)
            {
                RemoveOldFollower(follower);
            }
            foreach (var follower in newReplicas)
            {
                AddNewFollower(follower);
            }
        }

        protected void AddNewFollower(SiloAddress silo)
        {
            if (log.IsVerbose) log.Verbose("Registering my replica on silo " + silo);
            silosHoldingMyPartition.Add(silo);
            localDirectory.Scheduler.RunOrQueueAsyncCompletion(() => 
                AsyncCompletion.FromTask(
                localDirectory.GetDirectoryReference(silo).RegisterReplica(localDirectory.MyAddress, null, null, localDirectory.DirectoryPartition.GetItems(), true)),
                localDirectory.RemGrainDirectory.SchedulingContext).Ignore();
        }

        protected void RemoveOldFollower(SiloAddress silo)
        {
            if (log.IsVerbose) log.Verbose("Removing my replica from silo " + silo);
            // release this old replica, as we have got a new one
            silosHoldingMyPartition.Remove(silo);
            localDirectory.Scheduler.QueueAsyncCompletion(() =>
                AsyncCompletion.FromTask(localDirectory.GetDirectoryReference(silo).UnregisterReplica(localDirectory.MyAddress)),
                localDirectory.RemGrainDirectory.SchedulingContext).Ignore();
        }

        protected void PrintStatus(string prefix = "")
        {
            if (log.IsVerbose)
            {
                var sb = new StringBuilder();
                if (prefix != "")
                {
                    sb.AppendFormat("{0}: ", prefix);
                }
                sb.Append("Holding replicas for ");
                var first = true;
                foreach (SiloAddress silo in replicatedPartitions.Keys)
                {
                    if (!first)
                    {
                        sb.Append(", ");
                    }
                    sb.Append(silo.ToString());
                    first = false;
                }
                sb.Append(". My partition is replicated on ");
                first = true;
                foreach (SiloAddress silo in silosHoldingMyPartition)
                {
                    if (!first)
                    {
                        sb.Append(", ");
                    }
                    sb.Append(silo.ToString());
                    first = false;
                }
                log.Verbose(sb.ToString());
            }
        }
    }
}
