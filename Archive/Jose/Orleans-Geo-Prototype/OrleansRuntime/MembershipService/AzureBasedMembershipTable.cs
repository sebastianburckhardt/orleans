using System;
using System.Collections.Generic;
using System.Globalization;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Orleans.AzureUtils;


namespace Orleans.Runtime.MembershipService
{
    internal class AzureBasedMembershipTable : IMembershipTable
    {
        private readonly Logger logger;
        private OrleansSiloInstanceManager tableManager;

        private AzureBasedMembershipTable()
        {
            logger = Logger.GetLogger("AzureSiloMembershipTable", Logger.LoggerType.Runtime);
        }

        public static async Task<AzureBasedMembershipTable> GetAzureBasedMembershipTable(string deploymentId, string connectionString, bool tryInitTableVersion = false)
        {
            AzureBasedMembershipTable table = new AzureBasedMembershipTable();
            table.tableManager = await OrleansSiloInstanceManager.GetManager(deploymentId, connectionString);

            // even if I am not the one who created the table, try to insert an initial table version if it is not already there,
            // so we have a first row, always before this silo starts working.
            if (tryInitTableVersion)
            {
                SiloInstanceTableEntry entry = table.tableManager.CreateTableVersionEntry(0);
                bool didInsert = await table.tableManager.InsertSiloEntryConditionally(entry, null, null, false).WithTimeout(AzureTableDefaultPolicies.TableOperation_TIMEOUT);   // ignore return value, since we don't care if I inserted it or not, as long as it is in there. 
            }
            return table;
        }

        public Task DeleteAzureMembershipTableEntries(string deploymentId)
        {
            return tableManager.DeleteTableEntries(deploymentId);
        }

        public async Task<MembershipTableData> ReadRow(SiloAddress key)
        {
            try
            {
                var entries = await tableManager.FindSiloEntryAndTableVersionRow(key);
                MembershipTableData data = Convert(entries);
                if (logger.IsVerbose2) logger.Verbose2("Read my entry {0} Table=\n{1}", key.ToLongString(), data.ToString());
                return data;
            }
            catch (Exception exc)
            {
                logger.Warn(ErrorCode.AzureTable_20, String.Format("Intermediate error reading silo entry for key {0} from the table {1}.",
                                key.ToLongString(), tableManager.TableName), exc);
                throw;
            }
        }

        public async Task<MembershipTableData> ReadAll()
        {
             try
             {
                var entries = await tableManager.FindAllSiloEntries();   
                MembershipTableData data = Convert(entries);
                if (logger.IsVerbose2) logger.Verbose2("ReadAll Table=\n{0}", data.ToString());
                return data; 
            }
            catch (Exception exc)
            {
                logger.Warn(ErrorCode.AzureTable_21, String.Format("Intermediate error reading all silo entries {0}.",
                                tableManager.TableName), exc);
                throw exc;
            }
        }

        public async Task<bool> InsertRow(MembershipEntry entry, TableVersion tableVersion)
        {
            try
            {
                if (logger.IsVerbose) logger.Verbose("InsertRow entry = {0}, table version = {1}", entry.ToFullString(), tableVersion);
                SiloInstanceTableEntry tableEntry = Convert(entry, tableManager.DeploymentId);
                SiloInstanceTableEntry versionEntry = tableManager.CreateTableVersionEntry(tableVersion.Version);

                bool result = await tableManager.InsertSiloEntryConditionally(tableEntry, versionEntry, tableVersion.VersionEtag);
                if (result == false)
                    logger.Warn(ErrorCode.AzureTable_22, String.Format("Insert failed due to contention on the table. Will retry. Entry {0}, table version = {1}", entry.ToFullString(), tableVersion));
                return result;
            }
            catch (Exception exc)
            {
                logger.Warn(ErrorCode.AzureTable_23, String.Format("Intermediate error inserting entry {0} tableVersion {1} to the table {2}.",
                                entry.ToFullString(), (tableVersion == null ? "null" : tableVersion.ToString()), tableManager.TableName), exc);
                throw;
            }
        }

        public async Task<bool> UpdateRow(MembershipEntry entry, string etag, TableVersion tableVersion)
        {
            try
            {
                if (logger.IsVerbose) logger.Verbose("UpdateRow entry = {0}, etag = {1}, table version = {2}", entry.ToFullString(), etag, tableVersion);
                SiloInstanceTableEntry siloEntry = Convert(entry, tableManager.DeploymentId);
                SiloInstanceTableEntry versionEntry = tableManager.CreateTableVersionEntry(tableVersion.Version);

                bool result = await tableManager.UpdateSiloEntryConditionally(siloEntry, etag, versionEntry, tableVersion.VersionEtag);
                if (result == false)
                    logger.Warn(ErrorCode.AzureTable_24, String.Format("Update failed due to contention on the table. Will retry. Entry {0}, eTag {1}, table version = {2} ", entry.ToFullString(), etag, tableVersion));
                return result;
            }
            catch (Exception exc)
            {
                logger.Warn(ErrorCode.AzureTable_25, String.Format("Intermediate error updating entry {0} tableVersion {1} to the table {2}.",
                        entry.ToFullString(), (tableVersion == null ? "null" : tableVersion.ToString()), tableManager.TableName), exc);
                throw;
            }
        }

        public async Task MergeColumn(MembershipEntry entry)
        {
            try
            {
                if (logger.IsVerbose) logger.Verbose("Merge entry = {0}", entry.ToFullString());
                SiloInstanceTableEntry siloEntry = ConvertPartial(entry, tableManager.DeploymentId);
                await tableManager.MergeTableEntryAsync(siloEntry);
            }
            catch (Exception exc)
            {
                logger.Warn(ErrorCode.AzureTable_26, String.Format("Intermediate error merging entry {0} to the table {1}.", entry.ToFullString(), tableManager.TableName), exc);
                throw;
            }
        }

        private MembershipTableData Convert(List<Tuple<SiloInstanceTableEntry, string>> entries)
        {
            try
            {
                List<Tuple<MembershipEntry, string>> memEntries = new List<Tuple<MembershipEntry, string>>();
                TableVersion tableVersion = null;
                foreach (var tuple in entries)
                {
                    SiloInstanceTableEntry tableEntry = tuple.Item1;
                    if (tableEntry.RowKey.Equals(SiloInstanceTableEntry.TABLE_VERSION_ROW))
                    {
                        tableVersion = new TableVersion(Int32.Parse(tableEntry.MBRVersion), tuple.Item2);
                    }
                    else
                    {
                        try
                        {
                            
                            MembershipEntry mbrEntry = Parse(tableEntry);
                            memEntries.Add(new Tuple<MembershipEntry, string>(mbrEntry, tuple.Item2));
                        }
                        catch (Exception exc)
                        {
                            logger.Error(ErrorCode.AzureTable_61, String.Format("Intermediate error parsing SiloInstanceTableEntry to MembershipTableData: {0}. Ignoring this entry.",
                                tableEntry), exc);
                        }
                    }
                }
                MembershipTableData data = new MembershipTableData(memEntries, tableVersion);
                return data;
            }
            catch (Exception exc)
            {
                logger.Error(ErrorCode.AzureTable_60, String.Format("Intermediate error parsing SiloInstanceTableEntry to MembershipTableData: {0}.", 
                    Utils.IEnumerableToString(entries, tuple => tuple.Item1.ToString())), exc);
                throw;
            }
        }

        private static MembershipEntry Parse(SiloInstanceTableEntry tableEntry)
        {
            MembershipEntry memEntry = new MembershipEntry();
            memEntry.HostName = tableEntry.HostName;
            memEntry.Status = (SiloStatus)Enum.Parse(typeof(SiloStatus), tableEntry.Status);

            if (!string.IsNullOrEmpty(tableEntry.ProxyPort))
                memEntry.ProxyPort = int.Parse(tableEntry.ProxyPort);
            if (!string.IsNullOrEmpty(tableEntry.Primary))
                memEntry.Primary = bool.Parse(tableEntry.Primary);

            int port = 0;
            if (!string.IsNullOrEmpty(tableEntry.Port))
                int.TryParse(tableEntry.Port, out port);
            int gen = 0;
            if (!string.IsNullOrEmpty(tableEntry.Generation))
                int.TryParse(tableEntry.Generation, out gen);
            memEntry.SiloAddress = SiloAddress.New(new IPEndPoint(IPAddress.Parse(tableEntry.Address), port), gen);

            memEntry.RoleName = tableEntry.RoleName;
            memEntry.InstanceName = tableEntry.InstanceName;
            if (!string.IsNullOrEmpty(tableEntry.UpdateZone))
                memEntry.UpdateZone = int.Parse(tableEntry.UpdateZone);
            if (!string.IsNullOrEmpty(tableEntry.FaultZone))
                memEntry.FaultZone = int.Parse(tableEntry.FaultZone);

            if (!string.IsNullOrEmpty(tableEntry.StartTime))
            {
                memEntry.StartTime = Logger.ParseDate(tableEntry.StartTime);
            }
            else
            {
                memEntry.StartTime = default(DateTime);
            }
            if (!string.IsNullOrEmpty(tableEntry.IAmAliveTime))
            {
                memEntry.IAmAliveTime = Logger.ParseDate(tableEntry.IAmAliveTime);
            }
            else
            {
                memEntry.IAmAliveTime = default(DateTime);
            }

            List<SiloAddress> suspectingSilos = new List<SiloAddress>();
            List<DateTime> suspectingTimes = new List<DateTime>();
            if (!string.IsNullOrEmpty(tableEntry.SuspectingSilos))
            {
                string[] silos = tableEntry.SuspectingSilos.Split('|');
                foreach (string silo in silos)
                {
                    suspectingSilos.Add(SiloAddress.FromParsableString(silo));
                }
            }

            if (!string.IsNullOrEmpty(tableEntry.SuspectingTimes))
            {
                string[] times = tableEntry.SuspectingTimes.Split('|');
                foreach (string time in times)
                {
                    suspectingTimes.Add(Logger.ParseDate(time));
                }
            }

            if (suspectingSilos.Count != suspectingTimes.Count)
                throw new OrleansException(String.Format("SuspectingSilos.Length of {0} as read from Azure table is not eqaul to SuspectingTimes.Length of {1}", suspectingSilos.Count, suspectingTimes.Count));

            for (int i = 0; i < suspectingSilos.Count; i++)
            {
                memEntry.AddSuspector(new Tuple<SiloAddress, DateTime>(suspectingSilos[i], suspectingTimes[i]));
            }
            return memEntry;
        }

        private static SiloInstanceTableEntry Convert(MembershipEntry memEntry, string deploymentId)
        {
            SiloInstanceTableEntry tableEntry = new SiloInstanceTableEntry();
            tableEntry.DeploymentId = deploymentId;
            tableEntry.Address = memEntry.SiloAddress.Endpoint.Address.ToString();
            tableEntry.Port = memEntry.SiloAddress.Endpoint.Port.ToString(CultureInfo.InvariantCulture);
            tableEntry.Generation = memEntry.SiloAddress.Generation.ToString(CultureInfo.InvariantCulture);

            tableEntry.HostName = memEntry.HostName;
            tableEntry.Status = memEntry.Status.ToString();
            tableEntry.ProxyPort = memEntry.ProxyPort.ToString(CultureInfo.InvariantCulture);
            tableEntry.Primary = memEntry.Primary.ToString();

            tableEntry.RoleName = memEntry.RoleName;
            tableEntry.InstanceName = memEntry.InstanceName;
            tableEntry.UpdateZone = memEntry.UpdateZone.ToString(CultureInfo.InvariantCulture);
            tableEntry.FaultZone = memEntry.FaultZone.ToString(CultureInfo.InvariantCulture);

            tableEntry.StartTime = Logger.PrintDate(memEntry.StartTime);
            tableEntry.IAmAliveTime = Logger.PrintDate(memEntry.IAmAliveTime);

            if (memEntry.SuspectTimes != null)
            {
                StringBuilder siloList = new StringBuilder();
                StringBuilder timeList = new StringBuilder();
                bool first = true;
                foreach (var tuple in memEntry.SuspectTimes)
                {
                    if (!first)
                    {
                        siloList.Append('|');
                        timeList.Append('|');
                    }
                    siloList.Append(tuple.Item1.ToParsableString());
                    timeList.Append(Logger.PrintDate(tuple.Item2));
                    first = false;
                }

                tableEntry.SuspectingSilos = siloList.ToString();
                tableEntry.SuspectingTimes = timeList.ToString();
            }
            else
            {
                tableEntry.SuspectingSilos = String.Empty;
                tableEntry.SuspectingTimes = String.Empty;
            }
            tableEntry.PartitionKey = deploymentId;
            tableEntry.RowKey = SiloInstanceTableEntry.ConstructRowKey(memEntry.SiloAddress);

            return tableEntry;
        }

        private static SiloInstanceTableEntry ConvertPartial(MembershipEntry memEntry, string deploymentId)
        {
            return new SiloInstanceTableEntry
            {
                DeploymentId = deploymentId,
                IAmAliveTime = Logger.PrintDate(memEntry.IAmAliveTime),
                PartitionKey = deploymentId,
                RowKey = SiloInstanceTableEntry.ConstructRowKey(memEntry.SiloAddress)
            };
        }
    }
}
