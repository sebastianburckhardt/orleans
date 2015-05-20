using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Threading;

using Orleans.Scheduler;

using Orleans.Counters;

namespace Orleans.Runtime.MembershipService
{
    internal class MembershipOracleData
    {
        private Dictionary<SiloAddress, MembershipEntry> localTable;           // all silos not including current silo
        private Dictionary<SiloAddress, SiloStatus> localTableCopy;            // a cached copy of a local table, including current silo, for fast access
        private Dictionary<SiloAddress, SiloStatus> localTableCopyOnlyActive;  // a cached copy of a local table, for fast access, including only active nodes and current silo (if active)

        private readonly List<ISiloStatusListener> statusListeners;
        private readonly Logger logger;
        
        private IntValueStatistic clusterSizeStatistic;
        private StringValueStatistic clusterStatistic;

        internal readonly DateTime SiloStartTime;
        internal readonly SiloAddress MyAddress;
        internal readonly string MyHostname;
        internal SiloStatus CurrentStatus { get; private set; } // current status of this silo.

        internal MembershipOracleData(Silo silo, Logger log)
        {
            this.logger = log;
            this.localTable = new Dictionary<SiloAddress, MembershipEntry>();  
            this.localTableCopy = new Dictionary<SiloAddress, SiloStatus>();       
            this.localTableCopyOnlyActive = new Dictionary<SiloAddress, SiloStatus>();  
            this.statusListeners = new List<ISiloStatusListener>();
            
            this.SiloStartTime = DateTime.UtcNow;
            this.MyAddress = silo.SiloAddress;
            this.MyHostname = silo.LocalConfig.DNSHostName;
            this.CurrentStatus = SiloStatus.Created;
            this.clusterSizeStatistic = IntValueStatistic.FindOrCreate(StatNames.STAT_MEMBERSHIP_ACTIVE_CLUSTER_SIZE, () => localTableCopyOnlyActive.Count);
            this.clusterStatistic = StringValueStatistic.FindOrCreate(StatNames.STAT_MEMBERSHIP_ACTIVE_CLUSTER,
                    () => 
                        {
                            List<string> list = localTableCopyOnlyActive.Keys.Select(addr => addr.ToLongString()).ToList();
                            list.Sort();
                            return Utils.IEnumerableToString(list);
                        });
        }

        #region ISiloStatusOracle Members

        // ONLY access localTableCopy and not the localTable, to prevent races, as this method may be called outside the turn.
        internal SiloStatus GetApproximateSiloStatus(SiloAddress siloAddress)
        {
            SiloStatus status = SiloStatus.None;
            if (siloAddress.Equals(MyAddress))
            {
                status = CurrentStatus;
            }
            else
            {
                if (!localTableCopy.TryGetValue(siloAddress, out status))
                {
                    if (CurrentStatus.Equals(SiloStatus.Active))
                        if (logger.IsVerbose) logger.Verbose(ErrorCode.Runtime_Error_100209, "-The given siloAddress {0} is not registered in this MembershipOracle.", siloAddress.ToLongString());
                    status = SiloStatus.None; // todo: review - we don't GC, so it's not Dead - maybe None?
                }
            }
            if (logger.IsVerbose3) logger.Verbose3("-GetApproximateSiloStatus returned {0} for silo: {1}", status, siloAddress.ToLongString());
            return status;
        }

        // ONLY access localTableCopy or localTableCopyOnlyActive and not the localTable, to prevent races, as this method may be called outside the turn.
        internal Dictionary<SiloAddress, SiloStatus> GetApproximateSiloStatuses(bool onlyActive = false)
        {
            Dictionary<SiloAddress, SiloStatus> dict = onlyActive ? localTableCopyOnlyActive : localTableCopy;
            if (logger.IsVerbose3) logger.Verbose3("-GetApproximateSiloStatuses returned {0} silos: {1}", dict.Count, Utils.DictionaryToString(dict));
            return dict;
        }

        internal bool IsValidSilo(SiloAddress silo)
        {
            if (silo.Equals(MyAddress))
                return true;
            SiloStatus status = GetApproximateSiloStatus(silo);
            if (status == SiloStatus.ShuttingDown || status == SiloStatus.Stopping || status == SiloStatus.Dead)
            {
                return false;
            }
            return true;
        }

        internal bool IsDeadSilo(SiloAddress silo)
        {
            if (silo.Equals(MyAddress))
                return false;
            return GetApproximateSiloStatus(silo) == SiloStatus.Dead;
        }

        internal bool SubscribeToSiloStatusEvents(ISiloStatusListener observer)
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

        internal bool UnSubscribeFromSiloStatusEvents(ISiloStatusListener observer)
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

        #endregion
                
        internal bool IsFunctional(SiloStatus status)
        {
            return status.Equals(SiloStatus.Active) || status.Equals(SiloStatus.ShuttingDown) || status.Equals(SiloStatus.Stopping);
        }

        internal void UpdateMyStatusLocal(SiloStatus status)
        {
            if (CurrentStatus != status)
            {
                // make copies
                var tmp_localTableCopy = GetSiloStatuses(st => true, true); // all the silos including me.
                var tmp_localTableCopyOnlyActive = GetSiloStatuses(st => st.Equals(SiloStatus.Active), true);    // only active silos including me.

                CurrentStatus = status;

                tmp_localTableCopy[MyAddress] = status;

                if (status.Equals(SiloStatus.Active))
                {
                    tmp_localTableCopyOnlyActive[MyAddress] = status;
                }
                else if (tmp_localTableCopyOnlyActive.ContainsKey(MyAddress))
                {
                    tmp_localTableCopyOnlyActive.Remove(MyAddress);
                }
                localTableCopy = tmp_localTableCopy;
                localTableCopyOnlyActive = tmp_localTableCopyOnlyActive;
                NotifyLocalSubscribers(MyAddress, CurrentStatus);
            }
        }

        private SiloStatus GetSiloStatus(SiloAddress siloAddress)
        {
            if (siloAddress.Equals(MyAddress))
            {
                return CurrentStatus;
            }
            MembershipEntry data;
            if (!localTable.TryGetValue(siloAddress, out data))
            {
                return SiloStatus.None; // todo: review - we don't GC, so it's not Dead - maybe None?
            }
            return data.Status;
        }

        internal MembershipEntry GetSiloEntry(SiloAddress siloAddress)
        {
            return localTable[siloAddress];
        }

        internal Dictionary<SiloAddress, SiloStatus> GetSiloStatuses(Func<SiloStatus, bool> filter, bool includeMyself)
        {
            Dictionary<SiloAddress, SiloStatus> dict = localTable.Where(pair => { return filter(pair.Value.Status); }).ToDictionary(pair => pair.Key, pair => pair.Value.Status);
            if (includeMyself && filter(CurrentStatus)) // add myself
            {
                dict.Add(MyAddress, CurrentStatus);
            }
            return dict;
        }

        internal MembershipEntry CreateNewMembershipEntry(NodeConfiguration nodeConf, SiloStatus myStatus)
        {
            return CreateNewMembershipEntry(nodeConf, MyAddress, MyHostname, myStatus, SiloStartTime);
        }

        private static MembershipEntry CreateNewMembershipEntry(NodeConfiguration nodeConf, SiloAddress myAddress, string myHostname, SiloStatus myStatus, DateTime startTime)
        {
            var assy = Assembly.GetEntryAssembly() ?? Assembly.GetExecutingAssembly();
            string roleName = assy.GetName().Name;

            MembershipEntry entry = new MembershipEntry
            {
                SiloAddress = myAddress,

                HostName = myHostname, //nodeConf.HostName,
                InstanceName = nodeConf.SiloName,

                Status = myStatus,
                ProxyPort = (nodeConf.IsGatewayNode ? nodeConf.ProxyGatewayEndpoint.Port : 0),
                Primary = nodeConf.IsPrimaryNode,

                RoleName = roleName,
                //UpdateZone = nodeConfig.UpdateZone,
                //FaultZone = nodeConfig.FaultZone,

                SuspectTimes = new List<Tuple<SiloAddress, DateTime>>(),
                StartTime = startTime,
                IAmAliveTime = DateTime.UtcNow
            };
            return entry;
        }

        internal bool TryUpdateStatusAndNotify(MembershipEntry entry)
        {
            bool changed = TryUpdateStatus(entry);
            if (changed)
            {
                localTableCopy = GetSiloStatuses(status => true, true); // all the silos including me.
                localTableCopyOnlyActive = GetSiloStatuses(status => status.Equals(SiloStatus.Active), true);    // only active silos including me.

                if (logger.IsVerbose) logger.Verbose("-Updated my local view of {0} status. It is now {1}.", entry.SiloAddress.ToLongString(), GetSiloStatus(entry.SiloAddress));

                NotifyLocalSubscribers(entry.SiloAddress, entry.Status);
            }
            return changed;
        }

        // return true if the status changed
        private bool TryUpdateStatus(MembershipEntry updatedSilo)
        {
            MembershipEntry currSiloData = null;
            if (!localTable.TryGetValue(updatedSilo.SiloAddress, out currSiloData))
            {
                // an optimization - if I learn about dead silo and I never knew about him before, I don't care, can just ignore him.
                if (updatedSilo.Status == SiloStatus.Dead)
                    return false;
                localTable.Add(updatedSilo.SiloAddress, updatedSilo);
                return true;
            }
            if (currSiloData.Status != updatedSilo.Status)
            {
                currSiloData.Update(updatedSilo);
                return true;
            }
            return false;
        }

        internal bool TryUpdateStatusesAndNotify(Dictionary<SiloAddress, MembershipEntry> newEntries)
        {
            List<MembershipEntry> changed = TryUpdateStatuses(newEntries);
            if (changed != null && changed.Count > 0)
            {
                localTableCopy = GetSiloStatuses(status => true, true); // all the silos including me.
                localTableCopyOnlyActive = GetSiloStatuses(status => status.Equals(SiloStatus.Active), true);    // only active silos including me.

                foreach (MembershipEntry entry in changed)
                {
                    if (logger.IsVerbose) logger.Verbose("-Updated my local view of {0} status. It is now {1}.", entry.SiloAddress.ToLongString(), GetSiloStatus(entry.SiloAddress));
                    NotifyLocalSubscribers(entry.SiloAddress, entry.Status);
                }
                return true;
            }
            return false;
        }

        // return the list of MembershipEntry that changed.
        // first find all entries in the new list that either don't appear in the old list or have changed.
        // second, find all entries in the old list that have been removed from the new list.
        private List<MembershipEntry> TryUpdateStatuses(Dictionary<SiloAddress, MembershipEntry> newEntries)
        {
            List<MembershipEntry> changedEntries = null;
            foreach (MembershipEntry updatedSilo in newEntries.Values.Where(item => !item.SiloAddress.Endpoint.Equals(MyAddress.Endpoint)))
            {
                MembershipEntry currSiloData = null;
                if (!localTable.TryGetValue(updatedSilo.SiloAddress, out currSiloData))
                {
                    // an optimization - if I learn about dead silo and I never knew about him before, I don't care, can just ignore him.
                    if (updatedSilo.Status == SiloStatus.Dead)
                        continue;
                    localTable.Add(updatedSilo.SiloAddress, updatedSilo);
                    changedEntries = changedEntries ?? new List<MembershipEntry>();
                    changedEntries.Add(updatedSilo);
                }
                else if (currSiloData.Status != updatedSilo.Status)
                {
                    currSiloData.Update(updatedSilo);
                    changedEntries = changedEntries ?? new List<MembershipEntry>();
                    changedEntries.Add(updatedSilo);
                }
            }
            List<MembershipEntry> removedEntries = null;
            foreach (var currSilo in localTable.Values.Where(item => !item.Status.Equals(SiloStatus.Dead)))
            {
                if (!newEntries.ContainsKey(currSilo.SiloAddress))
                {
                    currSilo.Status = SiloStatus.Dead;
                    removedEntries = removedEntries ?? new List<MembershipEntry>();
                    removedEntries.Add(currSilo);
                }
            }
            // Do NOT remove from old list, leave tombstones, to be compatible with Table based MBR.
            //if (removedEntries != null)
            //{
            //    foreach (var entry in removedEntries)
            //    {
            //        localTable.Remove(entry.SiloAddress);
            //    }
            //}
            return SetExtensions.Union(changedEntries, removedEntries);
        }

        private void NotifyLocalSubscribers(SiloAddress siloAddress, SiloStatus newStatus)
        {
            if (logger.IsVerbose2) logger.Verbose2("-NotifyLocalSubscribers about {0} status {1}", siloAddress.ToLongString(), newStatus);
            List<ISiloStatusListener> copy;
            lock (statusListeners)
            {
                copy = statusListeners.ToList();
            }
            foreach (ISiloStatusListener listener in copy)
            {
                try
                {
                    listener.SiloStatusChangeNotification(
                        siloAddress,
                        newStatus);
                }
                catch (Exception exc)
                {
                    logger.Error(ErrorCode.MBRLocalSubscriberException,
                        String.Format("Local ISiloStatusListener {0} has thrown an exception when was notified about SiloStatusChangeNotification about silo {1} new status {2}",
                        listener.GetType().FullName, siloAddress.ToLongString(), newStatus), exc);
                }
            }
        }

        public override string ToString()
        {
            //return String.Format("CurrentSiloStatus = {0}, {1}", CurrentStatus, new NamingServiceData(localTable).ToString());
            return string.Format("CurrentSiloStatus = {0}, {1} silos: {2}.",
                CurrentStatus,
                localTableCopy.Count,
                Utils.IEnumerableToString(localTableCopy, pair => String.Format("SiloAddress={0} Status={1}", pair.Key.ToLongString(), pair.Value)));
        }
    }
}
