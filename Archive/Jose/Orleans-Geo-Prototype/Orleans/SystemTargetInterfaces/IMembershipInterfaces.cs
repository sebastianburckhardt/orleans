using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.Threading.Tasks;

using Orleans;

namespace Orleans
{
    //// This is an internal interface for IMembershipService implementation.
    [Unordered]
    internal interface IMembershipTable : IGrain
    {
        Task<MembershipTableData> ReadRow(SiloAddress key);

        Task<MembershipTableData> ReadAll();

        Task<bool> InsertRow(MembershipEntry entry, TableVersion tableVersion);

        /// <summary>
        /// Writes a new entry iff the entry etag is equal to the provided etag parameter.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="entry"></param>
        /// <param name="etag"></param>
        /// <returns>true iff the write was successful</returns>
        Task<bool> UpdateRow(MembershipEntry entry, string etag, TableVersion tableVersion);

        Task MergeColumn(MembershipEntry entry);
    }

    //// This is an internal interface for IMembershipService implementation.
    internal interface IMembershipNamingService
    {
        Uri GetServiceName();

        string GetServiceFullUri();

        NamingServiceData GetAllServiceInstances();

        bool SubscribeToNamingServiceEvents(IMembershipNamingServiceListener observer);

        bool UnSubscribeFromNamingServiceEvents(IMembershipNamingServiceListener observer);
    }

    internal interface IMembershipNamingServiceListener
    {
        /// <summary>
        /// Receive notifications about silo status events. 
        /// </summary>
        /// <param name="type">silos</param>All silos.
        /// <returns></returns>
        void AllMembersNamingServiceNotification(NamingServiceData silos);
    }

    [Serializable]
    internal class TableVersion
    {
        public int Version { get; private set; }
        public string VersionEtag { get; private set; }

        public TableVersion(int version, string eTag)
        {
            Version = version;
            VersionEtag = eTag;
        }

        public TableVersion Next()
        {
            return new TableVersion(Version + 1, VersionEtag);
        }

        public override string ToString()
        {
            return string.Format("<{0}, {1}>", Version, VersionEtag);
        }
    }

    [Serializable]
    internal class MembershipTableData
    {
        public List<Tuple<MembershipEntry, string>> Members { get; private set; }
        public TableVersion Version { get; private set; }

        public MembershipTableData(List<Tuple<MembershipEntry, string>> list, TableVersion version)
        {
            Members = list;
            Version = version;
        }

        public MembershipTableData(Tuple<MembershipEntry, string> tuple, TableVersion version)
        {
            Members = new List<Tuple<MembershipEntry, string>>();
            Members.Add(tuple);
            Version = version;
        }

        public MembershipTableData(TableVersion version)
        {
            Members = new List<Tuple<MembershipEntry, string>>();
            Version = version;
        }

        public Tuple<MembershipEntry, string> Get(SiloAddress silo)
        {
            return Members.First(tuple => tuple.Item1.SiloAddress.Equals(silo));
        }

        public bool Contains(SiloAddress silo)
        {
            return Members.Any(tuple => tuple.Item1.SiloAddress.Equals(silo));
        }

        public override string ToString()
        {
            int active = Members.Where(e => e.Item1.Status == SiloStatus.Active).Count();
            int dead = Members.Where(e => e.Item1.Status == SiloStatus.Dead).Count();
            int created = Members.Where(e => e.Item1.Status == SiloStatus.Created).Count();
            int joining = Members.Where(e => e.Item1.Status == SiloStatus.Joining).Count();
            int shuttingDown = Members.Where(e => e.Item1.Status == SiloStatus.ShuttingDown).Count();
            int stopping = Members.Where(e => e.Item1.Status == SiloStatus.Stopping).Count();

            string otherCounts = String.Format("{0}{1}{2}{3}",
                                created > 0 ? (", " + created + " are Created") : "",
                                joining > 0 ? (", " + joining + " are Joining") : "",
                                shuttingDown > 0 ? (", " + shuttingDown + " are ShuttingDown") : "",
                                stopping > 0 ? (", " + stopping + " are Stopping") : "");

            return string.Format("{0} silos, {1} are Active, {2} are Dead{3}: {4}. Version={5}",
                Members.Count,
                active,
                dead,
                otherCounts,
                Utils.IEnumerableToString(Members, (tuple) => tuple.Item1.ToFullString()), //+ " -> eTag " + tuple.Item2),
                Version);
        }

        // return a copy of the table removing all dead appereances of dead nodes, except for the last one.
        public MembershipTableData SupressDuplicateDeads()
        {
            Dictionary<string, Tuple<MembershipEntry, string>> deads = new Dictionary<string, Tuple<MembershipEntry, string>>();
            // pick only latest Dead for each instance
            foreach (var next in this.Members.Where(item => item.Item1.Status == SiloStatus.Dead))
            {
                string name = next.Item1.InstanceName;
                Tuple<MembershipEntry, string> prev = null;
                if (!deads.TryGetValue(name, out prev))
                {
                    deads[name] = next;
                }
                else
                {
                    // later Dead.
                    if (next.Item1.SiloAddress.Generation.CompareTo(prev.Item1.SiloAddress.Generation) > 0)
                    {
                        deads[name] = next;
                    }
                }
            }
            //now add back non Deads
            List<Tuple<MembershipEntry, string>> all = deads.Values.ToList();
            all.AddRange(this.Members.Where(item => item.Item1.Status != SiloStatus.Dead));
            return new MembershipTableData(all, this.Version);
        }
    }


    [Serializable]
    internal class NamingServiceData
    {
        public Dictionary<SiloAddress, MembershipEntry> Members { get; private set; }

        public static readonly NamingServiceData Empty = new NamingServiceData(new Dictionary<SiloAddress, MembershipEntry>(0));

        public NamingServiceData(Dictionary<SiloAddress, MembershipEntry> dictionary)
        {
            Members = dictionary;
        }

        public MembershipEntry Get(SiloAddress silo)
        {
            MembershipEntry value;
            Members.TryGetValue(silo, out value);
            return value;
        }

        public bool Contains(SiloAddress silo)
        {
            return Members.ContainsKey(silo);
        }

        public bool Contains(IPEndPoint endpoint)
        {
            return Members.Any(tuple => tuple.Key.Endpoint.Equals(endpoint));
        }

        public override string ToString()
        {
            return string.Format("{0} silos: {1}.",
                Members.Count,
                Utils.IEnumerableToString(Members.Values, member => member.ToFullString()));
        }
    }


    [Serializable]
    internal class MembershipEntry
    {
        public SiloAddress SiloAddress { get; set; }

        public string HostName { get; set; }              // Mandatory
        public SiloStatus Status { get; set; }                // Mandatory
        public int ProxyPort { get; set; }             // Optional
        public bool Primary { get; set; }               // Optional - should be depricated

        public string RoleName { get; set; }              // Optional - only for Azure role
        public string InstanceName { get; set; }          // Optional - only for Azure role
        public int UpdateZone { get; set; }            // Optional - only for Azure role
        public int FaultZone { get; set; }             // Optional - only for Azure role

        public DateTime StartTime { get; set; }             // Time this silo was started. For diagnostics.
        public DateTime IAmAliveTime { get; set; }          // Time this silo updated it was alive. For diagnostics.

        public List<Tuple<SiloAddress, DateTime>> SuspectTimes { get; set; }

        private static readonly List<Tuple<SiloAddress, DateTime>> EmptyList = new List<Tuple<SiloAddress, DateTime>>(0);

        public static bool COMPACT_MBR_LOGGING = true;

        // partialUpdate arrivies via gossiping with other oracles. In such a case only take the status.
        internal void Update(MembershipEntry updatedSiloEntry)
        {
            SiloAddress = updatedSiloEntry.SiloAddress;
            Status = updatedSiloEntry.Status;
            //---
            HostName = updatedSiloEntry.HostName;
            ProxyPort = updatedSiloEntry.ProxyPort;
            Primary = updatedSiloEntry.Primary;

            RoleName = updatedSiloEntry.RoleName;
            InstanceName = updatedSiloEntry.InstanceName;
            UpdateZone = updatedSiloEntry.UpdateZone;
            FaultZone = updatedSiloEntry.FaultZone;

            SuspectTimes = updatedSiloEntry.SuspectTimes;
            StartTime = updatedSiloEntry.StartTime;
            IAmAliveTime = updatedSiloEntry.IAmAliveTime;
        }

        internal List<Tuple<SiloAddress, DateTime>> GetFreshVotes(TimeSpan expiration)
        {
            if (SuspectTimes == null)
                return EmptyList;
            DateTime now = DateTime.UtcNow;
            return SuspectTimes.FindAll(voter =>
                {
                    DateTime otherVoterTime = voter.Item2;
                    // If now is smaller than otherVoterTime, than assume the otherVoterTime is fresh.
                    // This could happen if clocks are not synchronized and the other voter clock is ahead of mine.
                    if (now < otherVoterTime) 
                        return true;
                    else
                        return now.Subtract(otherVoterTime) < expiration;
                });
        }

        internal void AddSuspector(Tuple<SiloAddress, DateTime> suspector)
        {
            if (SuspectTimes == null)
                SuspectTimes = new List<Tuple<SiloAddress, DateTime>>();
            SuspectTimes.Add(suspector);
        }

        internal void TryUpdateStartTime(DateTime startTime)
        {
            if (StartTime.Equals(default(DateTime)))
            {
                StartTime = startTime;
            }
        }

        public override string ToString()
        {
            return string.Format("SiloAddress={0} Status={1}", SiloAddress.ToLongString(), Status);
        }

        internal string ToFullString()
        {
            if (MembershipEntry.COMPACT_MBR_LOGGING)
            {
                return ToString();
            }
            else
            {
                List<SiloAddress> suspecters = SuspectTimes == null ? null : SuspectTimes.Select(tuple => tuple.Item1).ToList();
                List<DateTime> timestamps = SuspectTimes == null ? null : SuspectTimes.Select(tuple => tuple.Item2).ToList();
                return string.Format("[SiloAddress={0} Status={1} HostName={2} ProxyPort={3} Primary={4} " +
                    "RoleName={5} InstanceName={6} UpdateZone={7} FaultZone={8} StartTime = {9} IAmAliveTime = {10} {11} {12}]",
                    SiloAddress.ToLongString(),
                    Status,
                    HostName,
                    ProxyPort,
                    Primary,
                    RoleName,
                    InstanceName,
                    UpdateZone,
                    FaultZone,
                    Logger.PrintDate(StartTime),
                    Logger.PrintDate(IAmAliveTime),
                    suspecters == null ? "" : "Suspecters = " + Utils.IEnumerableToString(suspecters, (SiloAddress sa) => sa.ToLongString()),
                    timestamps == null ? "" : "SuspectTimes = " + Utils.IEnumerableToString(timestamps, (DateTime dt) => Logger.PrintDate(dt))
                    );
            }
        }
    }
}
