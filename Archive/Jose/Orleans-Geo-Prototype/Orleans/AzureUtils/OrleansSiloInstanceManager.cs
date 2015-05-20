using System;
using System.Collections.Generic;
using System.Data.Services.Common;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using System.Text;
using Microsoft.WindowsAzure.StorageClient;
using System.Threading.Tasks;


namespace Orleans.AzureUtils
{
    [DataServiceKey("PartitionKey", "RowKey")]
    internal class SiloInstanceTableEntry : TableServiceEntity
    {
        public string DeploymentId { get; set; }    // PartitionKey
        public string Address { get; set; }         // RowKey
        public string Port { get; set; }            // RowKey
        public string Generation { get; set; }      // RowKey

        public string HostName { get; set; }        // Mandatory
        public string Status { get; set; }          // Mandatory
        public string ProxyPort { get; set; }       // Optional
        public string Primary { get; set; }         // Optional - should be depricated

        public string RoleName { get; set; }        // Optional - only for Azure role
        public string InstanceName { get; set; }    // Optional - only for Azure role
        public string UpdateZone { get; set; }         // Optional - only for Azure role
        public string FaultZone { get; set; }          // Optional - only for Azure role

        public string SuspectingSilos { get; set; }          // For liveness
        public string SuspectingTimes { get; set; }          // For liveness

        public string StartTime       { get; set; }          // Time this silo was started. For diagnostics.
        public string IAmAliveTime    { get; set; }           // Time this silo updated it was alive. For diagnostics.
        public string MBRVersion      { get; set; }               // Special version row (for serializing table updates). // We'll have a designated row with only MBRVersion column.

        internal const string TABLE_VERSION_ROW = "VersionRow"; // Row key for version row.

        public static string ConstructRowKey(SiloAddress silo)
        {
            return String.Format("{0}-{1}-{2}", silo.Endpoint.Address, silo.Endpoint.Port, silo.Generation);
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            if (RowKey.Equals(TABLE_VERSION_ROW))
            {
                sb.Append("VersionRow [").Append(DeploymentId);
                sb.Append(" Deployment=").Append(DeploymentId);
                sb.Append(" MBRVersion=").Append(MBRVersion);
                sb.Append("]");
            }
            else
            {
                sb.Append("OrleansSilo [");
                sb.Append(" Deployment=").Append(DeploymentId);
                sb.Append(" LocalEndpoint=").Append(Address);
                sb.Append(" LocalPort=").Append(Port);
                sb.Append(" Generation=").Append(Generation);

                sb.Append(" Host=").Append(HostName);
                sb.Append(" Status=").Append(Status);
                sb.Append(" ProxyPort=").Append(ProxyPort);
                sb.Append(" Primary=").Append(Primary);

                if (!string.IsNullOrEmpty(RoleName)) sb.Append(" RoleName=").Append(RoleName);
                sb.Append(" Instance=").Append(InstanceName);
                sb.Append(" UpgradeZone=").Append(UpdateZone);
                sb.Append(" FaultZone=").Append(FaultZone);

                if (!string.IsNullOrEmpty(SuspectingSilos)) sb.Append(" SuspectingSilos=").Append(SuspectingSilos);
                if (!string.IsNullOrEmpty(SuspectingTimes)) sb.Append(" SuspectingTimes=").Append(SuspectingTimes);
                sb.Append(" StartTime=").Append(StartTime);
                sb.Append(" IAmAliveTime=").Append(IAmAliveTime);
                sb.Append("]");
            }
            return sb.ToString();
        }
    }

    internal class OrleansSiloInstanceManager : AzureTableDataManager<SiloInstanceTableEntry>
    {
        private const string INSTANCE_TABLE_NAME = "OrleansSiloInstances";

        private readonly string INSTANCE_STATUS_CREATED = SiloStatus.Created.ToString();  //"Created";
        private readonly string INSTANCE_STATUS_ACTIVE = SiloStatus.Active.ToString();    //"Active";
        private readonly string INSTANCE_STATUS_DEAD = SiloStatus.Dead.ToString();        //"Dead";

        public string DeploymentId { get; private set; }

        private OrleansSiloInstanceManager(string deploymentId, string storageConnectionString)
            : base(INSTANCE_TABLE_NAME, storageConnectionString)
        {
            this.DeploymentId = deploymentId;
        }

        public static async Task<OrleansSiloInstanceManager> GetManager(string deploymentId, string storageConnectionString)
        {
            OrleansSiloInstanceManager instance = new OrleansSiloInstanceManager(deploymentId, storageConnectionString);
            try
            {
                await instance.InitTableAsync().WithTimeout(AzureTableDefaultPolicies.TableCreation_TIMEOUT);
                
            }
            catch (TimeoutException)
            {
                instance.logger.Fail(ErrorCode.AzureTable_32, String.Format("Unable to create or connect to the Azure table in {0}", AzureTableDefaultPolicies.TableCreation_TIMEOUT));
            }
            catch (Exception ex)
            {
                instance.logger.Fail(ErrorCode.AzureTable_33, String.Format("Exception trying to create or connect to the Azure table: {0}", ex));
            }
            return instance;
        }

        public SiloInstanceTableEntry CreateTableVersionEntry(int tableVersion)
        {
            return new SiloInstanceTableEntry
            {
                DeploymentId = this.DeploymentId,
                PartitionKey = this.DeploymentId,
                RowKey = SiloInstanceTableEntry.TABLE_VERSION_ROW,
                MBRVersion = tableVersion.ToString(CultureInfo.InvariantCulture)
            };
        }

        public void RegisterSiloInstance(SiloInstanceTableEntry entry)
        {
            entry.Status = INSTANCE_STATUS_CREATED;

            logger.Info(ErrorCode.Runtime_Error_100270, "Registering silo instance: {0}", entry.ToString());

            UpsertTableEntryAsync(entry).WaitWithThrow(AzureTableDefaultPolicies.TableOperation_TIMEOUT);
        }

        public void UnregisterSiloInstance(SiloInstanceTableEntry entry)
        {
            entry.Status = INSTANCE_STATUS_DEAD;

            logger.Info(ErrorCode.Runtime_Error_100271, "Unregistering silo instance: {0}", entry.ToString());

            UpsertTableEntryAsync(entry).WaitWithThrow(AzureTableDefaultPolicies.TableOperation_TIMEOUT);
        }

        public void ActivateSiloInstance(SiloInstanceTableEntry entry)
        {
            logger.Info(ErrorCode.Runtime_Error_100272, "Activating silo instance: {0}", entry.ToString());

            entry.Status = INSTANCE_STATUS_ACTIVE;

            UpsertTableEntryAsync(entry).WaitWithThrow(AzureTableDefaultPolicies.TableOperation_TIMEOUT);
        }

        public IPEndPoint FindPrimarySiloEndpoint()
        {
            SiloInstanceTableEntry primarySilo = FindPrimarySilo();
            if (primarySilo != null)
            {
                int port = 0;
                if (!string.IsNullOrEmpty(primarySilo.Port))
                {
                    int.TryParse(primarySilo.Port, out port);
                }
                return new IPEndPoint(IPAddress.Parse(primarySilo.Address), port);
            }
            return null;
        }

        public List<IPEndPoint> FindAllGatewayProxyEndpoints()
        {
            List<SiloInstanceTableEntry> gatewaySiloInstances = FindAllGatewaySilos();
            return gatewaySiloInstances.Select(gateway =>
            {
                int proxyPort = 0;
                if (!string.IsNullOrEmpty(gateway.ProxyPort))
                    int.TryParse(gateway.ProxyPort, out proxyPort);
                return new IPEndPoint(IPAddress.Parse(gateway.Address), proxyPort);
            }).ToList();
        }

        private SiloInstanceTableEntry FindPrimarySilo()
        {
            logger.Info(ErrorCode.Runtime_Error_100275, "Searching for active primary silo for deployment {0} ...", this.DeploymentId);
            string primary = true.ToString();

            Expression<Func<SiloInstanceTableEntry, bool>> query = instance =>
                instance.PartitionKey == this.DeploymentId
                && instance.Status == INSTANCE_STATUS_ACTIVE
                && instance.Primary == primary;

            var queryResults = ReadTableEntriesAndEtagsAsync(query)
                                 .WaitForResultWithThrow(AzureTableDefaultPolicies.TableOperation_TIMEOUT);

            SiloInstanceTableEntry primarySilo = default(SiloInstanceTableEntry);
            List<SiloInstanceTableEntry> primarySilosList = queryResults.Select(entity => entity.Item1).ToList();

            if (primarySilosList.Count == 0)
            {
                logger.Error(ErrorCode.Runtime_Error_100310, "Could not find Primary Silo");
            }
            else
            {
                 primarySilo = primarySilosList.FirstOrDefault();
                 logger.Info(ErrorCode.Runtime_Error_100276, "Found Primary Silo: {0}", primarySilo);
            }
            return primarySilo;
        }

        private List<SiloInstanceTableEntry> FindAllGatewaySilos()
        {
            logger.Info(ErrorCode.Runtime_Error_100277, "Searching for active gateway silos for deployment {0} ...", this.DeploymentId);
            const string zeroPort = "0";

            Expression<Func<SiloInstanceTableEntry, bool>> query = instance =>
                instance.PartitionKey == this.DeploymentId
                && instance.Status == INSTANCE_STATUS_ACTIVE
                && instance.ProxyPort != zeroPort;

            var queryResults = ReadTableEntriesAndEtagsAsync(query)
                                .WaitForResultWithThrow(AzureTableDefaultPolicies.TableOperation_TIMEOUT);

            List<SiloInstanceTableEntry> gatewaySiloInstances = queryResults.Select(entity => entity.Item1).ToList();

            logger.Info(ErrorCode.Runtime_Error_100278, "Found {0} active Gateway Silos.", gatewaySiloInstances.Count); //Utils.IEnumerableToString(gatewaySiloInstances),
            return gatewaySiloInstances;
        }

        public async Task<string> DumpSiloInstanceTable()
        {
            var queryResults = await ReadAllTableEntriesForPartitionAsync(this.DeploymentId);

            SiloInstanceTableEntry[] entries = queryResults.Select(entry => entry.Item1).ToArray();

            StringBuilder sb = new StringBuilder();
            string heading = String.Format("Deployment {0}. Silos: ", DeploymentId);
            sb.Append(heading);

            // Loop through the results, displaying information about the entity 
            Array.Sort(entries,
                (e1, e2) =>
                {
                    if (e1 == null) return (e2 == null) ? 0 : -1;
                    if (e2 == null) return (e1 == null) ? 0 : 1;
                    if (e1.InstanceName == null) return (e2.InstanceName == null) ? 0 : -1;
                    if (e2.InstanceName == null) return (e1.InstanceName == null) ? 0 : 1;
                    return String.CompareOrdinal(e1.InstanceName, e2.InstanceName);
                });
            foreach (SiloInstanceTableEntry entry in entries)
            {
                string str = String.Format("[IP {0}:{1}:{2}, {3}, Instance={4}, Status={5}]", entry.Address, entry.Port, entry.Generation,
                    entry.HostName, entry.InstanceName, entry.Status);
                sb.AppendLine(str);
            }
            return sb.ToString();
        }

        #region Silo instance table storage operations

        internal async Task<int> DeleteTableEntries(string deploymentId)
        {
            if (deploymentId == null)
            {
                await DeleteTableAsync();
                return -1;
            }
            else
            {
                List<Tuple<SiloInstanceTableEntry, string>> entries = await FindAllSiloEntries();
                await DeleteTableEntriesAsync(entries);
                return entries.Count;
            }
        }
        
        internal async Task<List<Tuple<SiloInstanceTableEntry, string>>> FindSiloEntryAndTableVersionRow(SiloAddress siloAddress)
        {
            string rowKey = SiloInstanceTableEntry.ConstructRowKey(siloAddress);

            Expression<Func<SiloInstanceTableEntry, bool>> query = instance =>
                instance.PartitionKey == this.DeploymentId
                && (instance.RowKey == rowKey || instance.RowKey == SiloInstanceTableEntry.TABLE_VERSION_ROW);

            var queryResults = await ReadTableEntriesAndEtagsAsync(query);

            var asList = queryResults.ToList();
            if (asList.Count < 1 || asList.Count > 2)
            {
                throw new KeyNotFoundException(string.Format("Could not find table version row or found too many entries. Was looking for key {0}, found = {1}", siloAddress.ToLongString(), Utils.IEnumerableToString(asList)));
            }
            int numTableVersionRows = asList.Count(tuple => tuple.Item1.RowKey == SiloInstanceTableEntry.TABLE_VERSION_ROW);
            if (numTableVersionRows < 1)
            {
                throw new KeyNotFoundException(string.Format("Did not read table version row. Read = {0}", Utils.IEnumerableToString(asList)));
            }
            if (numTableVersionRows > 1)
            {
                throw new KeyNotFoundException(string.Format("Read {0} table version rows, while was expecting only 1. Read = {1}", numTableVersionRows, Utils.IEnumerableToString(asList)));
            }
            return asList;
        }

        internal async Task<List<Tuple<SiloInstanceTableEntry, string>>> FindAllSiloEntries()
        {
            var queryResults = await ReadAllTableEntriesForPartitionAsync(this.DeploymentId);

            var asList = queryResults.ToList();
            if (asList.Count < 1)
            {
                throw new KeyNotFoundException(string.Format("Could not find enough rows in the FindAllSiloEntries call. Found = {0}", Utils.IEnumerableToString(asList)));
            }
            int numTableVersionRows = asList.Count(tuple => tuple.Item1.RowKey == SiloInstanceTableEntry.TABLE_VERSION_ROW);
            if (numTableVersionRows < 1)
            {
                throw new KeyNotFoundException(string.Format("Did not find table version row. Read = {0}", Utils.IEnumerableToString(asList)));
            }
            if (numTableVersionRows > 1)
            {
                throw new KeyNotFoundException(string.Format("Read {0} table version rows, while was expecting only 1. Read = {1}", numTableVersionRows, Utils.IEnumerableToString(asList)));
            }
            return asList;
        }

        /// <summary>
        /// Insert (create new) row entry
        /// </summary>
        /// <param name="siloEntry">Silo Entry to be written</param>
        internal async Task<bool> InsertSiloEntryConditionally(SiloInstanceTableEntry siloEntry, SiloInstanceTableEntry tableVersionEntry, string versionEtag, bool updateTableVersion = true)
        {
            try
            {
                await InsertTableEntryConditionallyAsync(siloEntry, tableVersionEntry, versionEtag, updateTableVersion);
                return true;
            }
            catch(Exception exc)
            {
                HttpStatusCode httpStatusCode;
                string restStatus;
                if (AzureStorageUtils.EvaluateException(exc, out httpStatusCode, out restStatus))
                {
                    if (logger.IsVerbose2) logger.Verbose2("InsertSiloEntryConditionally failed with httpStatusCode={0}, restStatus={1}", httpStatusCode, restStatus);
                    if (AzureStorageUtils.IsContentionError(httpStatusCode)) return false;
                }
                throw exc;
            }
        }

        /// <summary>
        /// Conditionally update the row for this entry, but only if the eTag matches with the current record in data store
        /// </summary>
        /// <param name="siloEntry">Silo Entry to be written</param>
        /// <param name="eTag">ETag value for the entry being updated</param>
        /// <returns></returns>
        internal async Task<bool> UpdateSiloEntryConditionally(SiloInstanceTableEntry siloEntry, string entryEtag, SiloInstanceTableEntry tableVersionEntry, string versionEtag)
        {
            try
            {
                await UpdateTableEntryConditionallyAsync(siloEntry, entryEtag, tableVersionEntry, versionEtag); 
                return true;
            }
            catch(Exception exc)
            {
                HttpStatusCode httpStatusCode;
                string restStatus;
                if (AzureStorageUtils.EvaluateException(exc, out httpStatusCode, out restStatus))
                {
                    if (logger.IsVerbose2) logger.Verbose2("UpdateSiloEntryConditionally failed with httpStatusCode={0}, restStatus={1}", httpStatusCode, restStatus);
                    if (AzureStorageUtils.IsContentionError(httpStatusCode)) return false;
                }
                throw exc;
            }
        }

        #endregion
    }
}