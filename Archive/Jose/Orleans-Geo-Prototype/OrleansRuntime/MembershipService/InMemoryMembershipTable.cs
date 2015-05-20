using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.Diagnostics;
using Orleans;


using System.IO;
using Orleans.Serialization;

namespace Orleans.Runtime.MembershipService
{
    [Serializable]
    internal class InMemoryMembershipTable
    {
        private Dictionary<SiloAddress, Tuple<MembershipEntry, string>> SiloTable;
        internal TableVersion TableVersion;
        private long LastETagCounter;

        public InMemoryMembershipTable()
        {
            SiloTable = new Dictionary<SiloAddress, Tuple<MembershipEntry, string>>();
            LastETagCounter = 0;
            TableVersion = new TableVersion(0, LastETagCounter++.ToString());
        }

        public MembershipTableData Read(SiloAddress key)
        {
            if (SiloTable.ContainsKey(key))
                return new MembershipTableData(SiloTable[key], TableVersion);
            else
                return new MembershipTableData(TableVersion);
        }

        public MembershipTableData ReadAll()
        {
            return new MembershipTableData(SiloTable.Values.Select(tuple => new Tuple<MembershipEntry, string>(tuple.Item1, tuple.Item2)).ToList(), TableVersion);
        }

        public TableVersion ReadTableVersion()
        {
            return TableVersion;
        }

        public bool Insert(MembershipEntry entry, TableVersion version)
        {
            Tuple<MembershipEntry, string> data = null;
            SiloTable.TryGetValue(entry.SiloAddress, out data);
            if (data == null)
            {
                if (TableVersion.VersionEtag.Equals(version.VersionEtag))
                {
                    SiloTable[entry.SiloAddress] = new Tuple<MembershipEntry, string>(entry, LastETagCounter++.ToString());
                    TableVersion = new TableVersion(version.Version, LastETagCounter++.ToString());
                    return true;
                }
            }
            return false;
        }

        public bool Update(MembershipEntry entry, string etag, TableVersion tableVersion)
        {
            Tuple<MembershipEntry, string> data = null;
            SiloTable.TryGetValue(entry.SiloAddress, out data);
            if (data != null)
            {
                if (data.Item2.Equals(etag) && tableVersion.VersionEtag.Equals(tableVersion.VersionEtag))
                {
                    SiloTable[entry.SiloAddress] = new Tuple<MembershipEntry, string>(entry, LastETagCounter++.ToString());
                    tableVersion = new TableVersion(tableVersion.Version, LastETagCounter++.ToString());
                    return true;
                }
            }
            return false;
        }

        public void MergeColumn(MembershipEntry entry)
        {
            Tuple<MembershipEntry, string> data = null;
            SiloTable.TryGetValue(entry.SiloAddress, out data);
            if (data != null)
            {
                data.Item1.IAmAliveTime = entry.IAmAliveTime;
                SiloTable[entry.SiloAddress] = new Tuple<MembershipEntry, string>(data.Item1, LastETagCounter++.ToString());
            }
        }

        public override string ToString()
        {
            return String.Format("Table = {0}, ETagCounter={1}", ReadAll().ToString(), LastETagCounter);
        }
    }
}

