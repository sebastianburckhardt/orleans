using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Orleans.Runtime.Configuration;
using System.Diagnostics;

namespace Orleans.Runtime.MembershipService
{
    internal class MembershipOracleData
    {
        private readonly Dictionary<SiloAddress, MembershipEntry> localTable;  // all silos not including current silo
        private Dictionary<SiloAddress, SiloStatus> localTableCopy;            // a cached copy of a local table, including current silo, for fast access
        private Dictionary<SiloAddress, SiloStatus> localTableCopyOnlyActive;  // a cached copy of a local table, for fast access, including only active nodes and current silo (if active)
        private Dictionary<SiloAddress, string> localNamesTableCopy;           // a cached copy of a map from SiloAddress to Silo Name, not including current silo, for fast access
        private List<SiloAddress> localMultiClusterGatewaysCopy;                           // a cached copy of the silos that are designated gateways

        private readonly List<ISiloStatusListener> statusListeners;
        private readonly TraceLogger logger;
        
        private IntValueStatistic clusterSizeStatistic;
        private StringValueStatistic clusterStatistic;

        internal readonly DateTime SiloStartTime;
        internal readonly SiloAddress MyAddress;
        internal readonly string MyHostname;
        internal SiloStatus CurrentStatus { get; private set; } // current status of this silo.
        internal string SiloName { get; private set; } // name of this silo.
 
        private readonly string GlobalServiceId; // set by configuration
        private readonly int MaxMultiClusterGateways; // set by configuration

        private UpdateFaultCombo MyFaultAndUpdateZones;
        private struct UpdateFaultCombo // define struct so we can use it as a key for group-by
        {
            public int UpdateZone;
            public int FaultZone;
        }
 

        internal MembershipOracleData(Silo silo, TraceLogger log)
        {
            logger = log;
            localTable = new Dictionary<SiloAddress, MembershipEntry>();  
            localTableCopy = new Dictionary<SiloAddress, SiloStatus>();       
            localTableCopyOnlyActive = new Dictionary<SiloAddress, SiloStatus>();
            localNamesTableCopy = new Dictionary<SiloAddress, string>();
            localMultiClusterGatewaysCopy = new List<SiloAddress>();
            statusListeners = new List<ISiloStatusListener>();
            
            SiloStartTime = DateTime.UtcNow;
            MyAddress = silo.SiloAddress;
            MyHostname = silo.LocalConfig.DNSHostName;
            SiloName = silo.LocalConfig.SiloName;
            GlobalServiceId = silo.GlobalConfig.GlobalServiceId;
            MaxMultiClusterGateways = silo.GlobalConfig.MaxMultiClusterGateways;
            CurrentStatus = SiloStatus.Created;
            clusterSizeStatistic = IntValueStatistic.FindOrCreate(StatisticNames.MEMBERSHIP_ACTIVE_CLUSTER_SIZE, () => localTableCopyOnlyActive.Count);
            clusterStatistic = StringValueStatistic.FindOrCreate(StatisticNames.MEMBERSHIP_ACTIVE_CLUSTER,
                    () => 
                        {
                            List<string> list = localTableCopyOnlyActive.Keys.Select(addr => addr.ToLongString()).ToList();
                            list.Sort();
                            return Utils.EnumerableToString(list);
                        });
        }

        
        // ONLY access localTableCopy and not the localTable, to prevent races, as this method may be called outside the turn.
        internal SiloStatus GetApproximateSiloStatus(SiloAddress siloAddress)
        {
            var status = SiloStatus.None;
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
                    status = SiloStatus.None;
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

        internal List<SiloAddress> GetApproximateMultiClusterGateways()
        {
            if (logger.IsVerbose3) logger.Verbose3("-GetApproximateMultiClusterGateways returned {0} silos: {1}", localMultiClusterGatewaysCopy.Count, string.Join(",", localMultiClusterGatewaysCopy));
            return localMultiClusterGatewaysCopy;
        }

        internal bool TryGetSiloName(SiloAddress siloAddress, out string siloName)
        {
            if (siloAddress.Equals(MyAddress))
            {
                siloName = SiloName;
                return true;
            }
            return localNamesTableCopy.TryGetValue(siloAddress, out siloName);
        }

        internal bool SubscribeToSiloStatusEvents(ISiloStatusListener observer)
        {
            lock (statusListeners)
            {
                if (statusListeners.Contains(observer))
                    return false;
                
                statusListeners.Add(observer);
                return true;
            }
        }

        internal bool UnSubscribeFromSiloStatusEvents(ISiloStatusListener observer)
        {
            lock (statusListeners)
            {
                return statusListeners.Contains(observer) && statusListeners.Remove(observer);
            }
        }

        internal void UpdateMyStatusLocal(SiloStatus status)
        {
            if (CurrentStatus == status) return;

            // make copies
            var tmpLocalTableCopy = GetSiloStatuses(st => true, true); // all the silos including me.
            var tmpLocalTableCopyOnlyActive = GetSiloStatuses(st => st.Equals(SiloStatus.Active), true);    // only active silos including me.
            var tmpLocalTableNamesCopy = localTable.ToDictionary(pair => pair.Key, pair => pair.Value.InstanceName);   // all the silos excluding me.

            CurrentStatus = status;

            tmpLocalTableCopy[MyAddress] = status;

            if (status.Equals(SiloStatus.Active))
            {
                tmpLocalTableCopyOnlyActive[MyAddress] = status;
            }
            else if (tmpLocalTableCopyOnlyActive.ContainsKey(MyAddress))
            {
                tmpLocalTableCopyOnlyActive.Remove(MyAddress);
            }
            localTableCopy = tmpLocalTableCopy;
            localTableCopyOnlyActive = tmpLocalTableCopyOnlyActive;
            localNamesTableCopy = tmpLocalTableNamesCopy;

            if (!string.IsNullOrEmpty(GlobalServiceId))
                localMultiClusterGatewaysCopy = DetermineMultiClusterGateways();

            NotifyLocalSubscribers(MyAddress, CurrentStatus);
        }

        private SiloStatus GetSiloStatus(SiloAddress siloAddress)
        {
            if (siloAddress.Equals(MyAddress))
                return CurrentStatus;
            
            MembershipEntry data;
            return !localTable.TryGetValue(siloAddress, out data) ? SiloStatus.None : data.Status;
        }

        internal MembershipEntry GetSiloEntry(SiloAddress siloAddress)
        {
            return localTable[siloAddress];
        }

        internal Dictionary<SiloAddress, SiloStatus> GetSiloStatuses(Func<SiloStatus, bool> filter, bool includeMyself)
        {
            Dictionary<SiloAddress, SiloStatus> dict = localTable.Where(
                pair => filter(pair.Value.Status)).ToDictionary(pair => pair.Key, pair => pair.Value.Status);

            if (includeMyself && filter(CurrentStatus)) // add myself
                dict.Add(MyAddress, CurrentStatus);
            
            return dict;
        }

        internal MembershipEntry CreateNewMembershipEntry(NodeConfiguration nodeConf, SiloStatus myStatus)
        {
            return CreateNewMembershipEntry(nodeConf, MyAddress, MyHostname, myStatus, SiloStartTime);
        }

        private static MembershipEntry CreateNewMembershipEntry(NodeConfiguration nodeConf, SiloAddress myAddress, string myHostname, SiloStatus myStatus, DateTime startTime)
        {
            var assy = Assembly.GetEntryAssembly() ?? typeof(MembershipOracleData).GetTypeInfo().Assembly;
            var roleName = assy.GetName().Name;

            var entry = new MembershipEntry
            {
                SiloAddress = myAddress,

                HostName = myHostname,
                InstanceName = nodeConf.SiloName,

                Status = myStatus,
                ProxyPort = (nodeConf.IsGatewayNode ? nodeConf.ProxyGatewayEndpoint.Port : 0),

                RoleName = roleName,
                
                SuspectTimes = new List<Tuple<SiloAddress, DateTime>>(),
                StartTime = startTime,
                IAmAliveTime = DateTime.UtcNow
            };
            return entry;
        }

        internal void UpdateMyFaultAndUpdateZone(MembershipEntry entry)
        {
            MyFaultAndUpdateZones.FaultZone = entry.FaultZone;
            MyFaultAndUpdateZones.UpdateZone = entry.UpdateZone;
        }

        internal bool TryUpdateStatusAndNotify(MembershipEntry entry)
        {
            if (!TryUpdateStatus(entry)) return false;

            localTableCopy = GetSiloStatuses(status => true, true); // all the silos including me.
            localTableCopyOnlyActive = GetSiloStatuses(status => status.Equals(SiloStatus.Active), true);    // only active silos including me.
            localNamesTableCopy = localTable.ToDictionary(pair => pair.Key, pair => pair.Value.InstanceName);   // all the silos excluding me.
            
            if (!string.IsNullOrEmpty(GlobalServiceId))
                localMultiClusterGatewaysCopy = DetermineMultiClusterGateways();

            if (logger.IsVerbose) logger.Verbose("-Updated my local view of {0} status. It is now {1}.", entry.SiloAddress.ToLongString(), GetSiloStatus(entry.SiloAddress));

            NotifyLocalSubscribers(entry.SiloAddress, entry.Status);
            return true;
        }

        // return true if the status changed
        private bool TryUpdateStatus(MembershipEntry updatedSilo)
        {
            MembershipEntry currSiloData = null;
            if (!localTable.TryGetValue(updatedSilo.SiloAddress, out currSiloData))
            {
                // an optimization - if I learn about dead silo and I never knew about him before, I don't care, can just ignore him.
                if (updatedSilo.Status == SiloStatus.Dead) return false;

                localTable.Add(updatedSilo.SiloAddress, updatedSilo);
                return true;
            }

            if (currSiloData.Status == updatedSilo.Status) return false;

            currSiloData.Update(updatedSilo);
            return true;
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
                    listener.SiloStatusChangeNotification(siloAddress, newStatus);
                }
                catch (Exception exc)
                {
                    logger.Error(ErrorCode.MembershipLocalSubscriberException,
                        String.Format("Local ISiloStatusListener {0} has thrown an exception when was notified about SiloStatusChangeNotification about silo {1} new status {2}",
                        listener.GetType().FullName, siloAddress.ToLongString(), newStatus), exc);
                }
            }
        }

    

        // deterministic function for designating the silos that should act as gateways
        private List<SiloAddress> DetermineMultiClusterGateways()
        {
            // function should never be called if we are not in a multicluster
            if (string.IsNullOrEmpty(GlobalServiceId))
                throw new OrleansException("internal error: should not call this function without multicluster network");

            // take all the active silos if their count does not exceed the desired number of gateways
            if (localTableCopyOnlyActive.Count <= MaxMultiClusterGateways)
                return localTableCopyOnlyActive.Keys.ToList();

            var candidates = new List<SiloAddress>();

            foreach(var x in localTableCopyOnlyActive)
                candidates.Add(x.Key);
            candidates.Sort();

            // group by fault/update zones
            var groups = new Dictionary<UpdateFaultCombo, List<SiloAddress>>();
            var keys = new List<UpdateFaultCombo>();
            foreach (var c in candidates)
            {
                UpdateFaultCombo key = default(UpdateFaultCombo);
                if (c.Equals(MyAddress))
                {
                    key = MyFaultAndUpdateZones;
                }
                else
                {
                    var e = localTable[c];
                    key.FaultZone = e.FaultZone;
                    key.UpdateZone = e.UpdateZone;
                }
                List<SiloAddress> list;
                if (!groups.TryGetValue(key, out list))
                {
                    groups[key] = list = new List<SiloAddress>();
                    keys.Add(key);
                }
                list.Add(c);
            }
            keys.Sort();
              
            // pick round-robin from groups
            var  result = new List<SiloAddress>();
            for (int i = 0; result.Count < MaxMultiClusterGateways; i++)
            {
                var list = groups[keys[i % keys.Count]];
                var col = i / keys.Count;
                if (col < list.Count)
                    result.Add(list[col]); 
            }
            return result;
        }


        public override string ToString()
        {
            return string.Format("CurrentSiloStatus = {0}, {1} silos: {2}.",
                CurrentStatus,
                localTableCopy.Count,
                Utils.EnumerableToString(localTableCopy, pair => 
                    String.Format("SiloAddress={0} Status={1}", pair.Key.ToLongString(), pair.Value)));
        }
    }
}
