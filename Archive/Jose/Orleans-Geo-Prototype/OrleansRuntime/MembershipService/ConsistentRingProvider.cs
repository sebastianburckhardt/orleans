using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

using Orleans.Scheduler;


namespace Orleans.Runtime.MembershipService
{
    /// <summary>
    /// We use the 'backward/clockwise' definition to assign responsibilities on the ring. 
    /// E.g. in a ring of nodes {5, 10, 15} the responsible for key 7 is 10 (the node is responsible for its predecessing range). 
    /// The backwards/clockwise approach is consistent with many overlays, e.g., Chord, Cassandra, etc.
    /// Note: MembershipOracle uses 'forward/anti-clockwise' definition to assign responsibilities. 
    /// E.g. in a ring of nodes {5, 10, 15}, the responsible of key 7 is node 5 (the node is responsible for its sucessing range)..
    /// </summary>
    internal class ConsistentRingProvider : MarshalByRefObject, IConsistentRingProvider, ISiloStatusListener // make the ring shutdown-able?
    {
        /// <summary>
        /// list of silo members sorted by the hash value of their address
        /// </summary>
        private readonly List<SiloAddress> membershipRingList;

        private readonly Logger log;

        internal bool running;

        internal SiloAddress MyAddress { get; private set; }
        internal IRingRange MyRange { get; private set; }
        private readonly int myKey;

        private readonly List<IRingRangeListener> statusListeners;


        public ConsistentRingProvider(SiloAddress siloAddr)
        {
            log = Logger.GetLogger("ConsistentRingProvider");

            membershipRingList = new List<SiloAddress>();
            MyAddress = siloAddr;
            myKey = MyAddress.GetConsistentHashCode();

            // add myself to the list of members
            AddServer(MyAddress);
            MyRange = RangeFactory.CreateFullRange(); // i am responsible for the whole range
            this.statusListeners = new List<IRingRangeListener>();
           
            Start();
        }

        /// <summary>
        /// Returns the silo that this silo thinks is the primary owner of the key
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public SiloAddress GetPrimary(int key)
        {
            return CalculateTargetSilo(key);
        }

        public IRingRange GetMyRange()
        {
            return MyRange; // its immutable, so no need to clone
        }

        public List<SiloAddress> GetMySucessors(int n = 1)
        {
            return FindSuccessors(MyAddress, n);
        }

        public List<SiloAddress> GetMyPredecessors(int n = 1)
        {
            return FindPredecessors(MyAddress, n);
        }

        #region Handling the membership

        // mainly copied from LocalGrainDirectory.cs

        private void Start()
        {
            running = true;
        }

        private void Stop()
        {
            running = false;
        }

        internal void AddServer(SiloAddress silo)
        {
            lock (membershipRingList)
            {
                if (membershipRingList.Contains(silo))
                {
                    // we already have this silo
                    return;
                }

                int myOldIndex = membershipRingList.FindIndex(elem => elem.Equals(MyAddress));

                if (!(membershipRingList.Count == 0 || myOldIndex != -1))
                {
                    throw new OrleansException(string.Format("{0}: Couldn't find my position in the ring {1}.", MyAddress, Utils.IEnumerableToString(membershipRingList)));
                }

                // insert new silo in the sorted order
                int hash = silo.GetConsistentHashCode();

                // Find the last silo with hash smaller than the new silo, and insert the latter after (this is why we have +1 here) the former.
                // Notice that FindLastIndex might return -1 if this should be the first silo in the list, but then
                // 'index' will get 0, as needed.
                int index = membershipRingList.FindLastIndex(siloAddr => siloAddr.GetConsistentHashCode() < hash) + 1;
                membershipRingList.Insert(index, silo);

                // relating to triggering handler ... new node took over some of my responsibility
                if (index == myOldIndex || // new node was inserted in my place
                    (myOldIndex == 0 && index == membershipRingList.Count - 1)) // I am the first node, and the new server is the last node
                {
                    IRingRange oldRange = MyRange;
                    try
                    {
                        MyRange = RangeFactory.CreateRange(unchecked((uint)hash), unchecked((uint)myKey));
                    }
                    catch (OverflowException exc)
                    {
                        log.Error(ErrorCode.ConsistentRingProviderBase + 5,
                            String.Format("OverflowException: hash as int= x{0, 8:X8}, hash as uint= x{1, 8:X8}, myKey as int x{2, 8:X8}, myKey as uint x{3, 8:X8}.",
                            hash, (uint)hash, myKey, (uint)myKey), exc);
                        //Logger.Flush();
                        //Environment.Exit(1);
                    }
                    NotifyLocalRangeSubscribers(oldRange, MyRange, false);
                }

                log.Info("Added Server {0} hash {1}. Current view: {2}", silo, silo.GetConsistentHashCode(), this.ToString());
            }
        }

        // just for debugging
        public override string ToString()
        {
            lock (membershipRingList)
            {
                return Utils.IEnumerableToString(membershipRingList, silo => String.Format("{0}/x{1, 8:X8}", silo, silo.GetConsistentHashCode()));
            }
        }

        internal void RemoveServer(SiloAddress silo)
        {
            lock (membershipRingList)
            {
                int indexOfFailedSilo = membershipRingList.FindIndex(elem => elem.Equals(silo));
                if (indexOfFailedSilo < 0)
                {
                    // we have already removed this silo
                    return;
                }

                membershipRingList.Remove(silo);

                // related to triggering handler
                int myNewIndex = membershipRingList.FindIndex(elem => elem.Equals(MyAddress));

                if (myNewIndex == -1)
                {
                    throw new OrleansException(string.Format("{0}: Couldn't find my position in the ring {1}.", MyAddress, this.ToString()));
                }

                bool wasMyPred = ((myNewIndex == indexOfFailedSilo) || (myNewIndex == 0 && indexOfFailedSilo == membershipRingList.Count)); // no need for '- 1'
                if (wasMyPred) // failed node was our predecessor
                {
                    if (log.IsVerbose) log.Verbose("Failed server was my pred? {0}, updated view {1}", wasMyPred, this.ToString());

                    IRingRange oldRange = MyRange;
                    if (membershipRingList.Count == 1) // i'm the only one left
                    {
                        MyRange = RangeFactory.CreateFullRange();
                        NotifyLocalRangeSubscribers(oldRange, MyRange, true);
                    }
                    else
                    {
                        int myNewPredIndex = myNewIndex == 0 ? membershipRingList.Count - 1 : myNewIndex - 1;
                        int myPredecessorsHash = membershipRingList[myNewPredIndex].GetConsistentHashCode();

                        MyRange = RangeFactory.CreateRange(unchecked((uint)myPredecessorsHash), unchecked((uint)myKey));
                        NotifyLocalRangeSubscribers(oldRange, MyRange, true);
                    }
                }
                log.Info("Removed Server {0} hash {1}. Current view {2}", silo, silo.GetConsistentHashCode(), this.ToString());
            }
        }

        internal List<SiloAddress> FindPredecessors(SiloAddress silo, int count)
        {
            lock (membershipRingList)
            {
                int index = membershipRingList.FindIndex(elem => elem.Equals(silo));
                if (index == -1)
                {
                    log.Warn(ErrorCode.Runtime_Error_100201, "Got request to find predecessors of silo " + silo + ", which is not in the list of members.");
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
            lock (membershipRingList)
            {
                int index = membershipRingList.FindIndex(elem => elem.Equals(silo));
                if (index == -1)
                {
                    log.Warn(ErrorCode.Runtime_Error_100203, "Got request to find successors of silo " + silo + ", which is not in the list of members.");
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

        #region Notification related
        // Notification related

        public bool SubscribeToRangeChangeEvents(IRingRangeListener observer)
        {
            lock (statusListeners)
            {
                if (statusListeners.Contains(observer))
                {
                    return false;
                }
                statusListeners.Add(observer);
                return true;
            }
        }

        public bool UnSubscribeFromRangeChangeEvents(IRingRangeListener observer)
        {
            lock (statusListeners)
            {
                if (statusListeners.Contains(observer))
                {
                    return statusListeners.Remove(observer);
                }
                return false;
            }
        }

        private void NotifyLocalRangeSubscribers(IRingRange old, IRingRange now, bool increased)
        {
            //if (log.IsVerbose2) log.Verbose2("-NotifyLocalRangeSubscribers about old {0} new {1} increased? {2}", old, now, increased);
            log.Info("-NotifyLocalRangeSubscribers about old {0} new {1} increased? {2}", old, now, increased);
            List<IRingRangeListener> copy;
            lock (statusListeners)
            {
                copy = statusListeners.ToList();
            }
            foreach (IRingRangeListener listener in copy)
            {
                try
                {
                    listener.RangeChangeNotification(old, now, increased);
                }
                catch (Exception exc)
                {
                    log.Error(ErrorCode.CRP_Local_Subscriber_Exception,
                        String.Format("Local IRangeChangeListener {0} has thrown an exception when was notified about RangeChangeNotification about old {1} new {2} increased? {3}",
                        listener.GetType().FullName, old, now, increased), exc);
                }
            }
        }

        // End of Notification related
        #endregion

        public void SiloStatusChangeNotification(SiloAddress updatedSilo, SiloStatus status)
        {
            // This silo's status has changed
            if (updatedSilo.Equals(MyAddress))
            {
                if (status == SiloStatus.Dead || status.Equals(SiloStatus.ShuttingDown) || status == SiloStatus.Stopping)
                {
                    Stop();
                }
            }
            else // Status change for some other silo
            {
                if (status.Equals(SiloStatus.Dead) || status.Equals(SiloStatus.ShuttingDown) || status.Equals(SiloStatus.Stopping))
                {
                    RemoveServer(updatedSilo);
                }
                else if (status.Equals(SiloStatus.Active))      // do not do anything with SiloStatus.Created or SiloStatus.Joining -- wait until it actually becomes active
                {
                    AddServer(updatedSilo);
                }
            }
        }
        #endregion

        /// <summary>
        /// Finds the silo that owns the given hash value.
        /// This routine will always return a non-null silo address unless the excludeThisSiloIfStopping parameter is true,
        /// this is the only silo known, and this silo is stopping.
        /// </summary>
        /// <param name="hash"></param>
        /// <param name="excludeThisSiloIfStopping"></param>
        /// <returns></returns>
        public SiloAddress CalculateTargetSilo(int hash, bool excludeThisSiloIfStopping = true)
        {
            SiloAddress s;

            lock (membershipRingList)
            {
                // excludeMySelf from being a TargetSilo if we're not running and the excludeThisSIloIfStopping flag is true. see the comment in the Stop method.
                bool excludeMySelf = excludeThisSiloIfStopping && !running;

                if (membershipRingList.Count == 0)
                {
                    // If the membership ring is empty, then we're the owner by default unless we're stopping.
                    if (excludeMySelf)
                    {
                        return null;
                    }
                    return MyAddress;
                }

                // use clockwise ... current code in membershipOracle.CalculateTargetSilo() does anti-clockwise ...
                // if you want to stick to anti-clockwise, change the responsibility definition in 'In()' method & responsibility defs in OrleansReminderMemory
                // need to implement a binary search, but for now simply traverse the list of silos sorted by their hashes
                s = membershipRingList.Find(siloAddr => (siloAddr.GetConsistentHashCode() >= hash) && // <= hash for anti-clockwise responsibilities
                                    (!siloAddr.Equals(MyAddress) || !excludeMySelf));

                if (s == null)
                {
                    // if not found in traversal, then first silo should be returned (we are on a ring)
                    // if you go back to their anti-clockwise policy, then change the 'In()' method in OrleansReminderMemory
                    s = membershipRingList[0]; // vs [membershipRingList.Count - 1]; for anti-clockwise policy
                    // Make sure it's not us...
                    if (s.Equals(MyAddress) && excludeMySelf)
                    {
                        if (membershipRingList.Count > 1)
                        {
                            s = membershipRingList[1]; // vs [membershipRingList.Count - 2]; for anti-clockwise policy
                        }
                        else
                        {
                            s = null;
                        }
                    }
                }
            }
            if (log.IsVerbose2) log.Verbose2("Silo {0} calculated ring partition owner silo {1} for key {2}: {3} --> {4}", MyAddress, s, hash, hash, s.GetConsistentHashCode());
            return s;
        }

    }
}
