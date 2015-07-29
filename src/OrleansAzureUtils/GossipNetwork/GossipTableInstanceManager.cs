using Microsoft.WindowsAzure.Storage.Table;
using Orleans.AzureUtils;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime.GossipNetwork
{
    // 
    //  low-level representation details & functionality for Azure-Table-Based Gossip Channels
    //  to go into Azure Utils?

    internal class GossipTableEntry : TableEntity
    {
        // used for partitioning table
        internal string GlobalServiceId { get { return PartitionKey; } }

        public DateTime GossipTimestamp { get; set; }   // timestamp of gossip entry


        #region gateway entry

        public string Status { get; set; }

        // all of the following are packed in rowkey

        public string ClusterId;

        public IPAddress Address;

        public int Port;

        public int Generation;

        public SiloAddress SiloAddress;

        #endregion


        #region configuration entry

        public string Clusters { get; set; }   // comma-separated list of clusters

        #endregion


        internal const string CONFIGURATION_ROW = "CONFIG"; // Row key for configuration row.

        public static string ConstructRowKey(SiloAddress silo)
        {
            return String.Format("{0}-{1}-{2}-{3}", silo.ClusterId, silo.Endpoint.Address, silo.Endpoint.Port, silo.Generation);
        }

        internal const char Separator = '-';


        internal void UnpackRowKey()
        {
            var debugInfo = "UnpackRowKey";
            try
            {
#if DEBUG
                debugInfo = String.Format("UnpackRowKey: RowKey={0}", RowKey);
#endif
                int idx3 = RowKey.LastIndexOf(Separator);
                int idx2 = RowKey.LastIndexOf(Separator, idx3 - 1);
                int idx1 = RowKey.LastIndexOf(Separator, idx2 - 1);
#if DEBUG
                debugInfo = String.Format("UnpackRowKey: RowKey={0} Idx1={1} Idx2={2} Idx3={3}", RowKey, idx1, idx2, idx3);
                Trace.TraceInformation(debugInfo);
#endif
                ClusterId = RowKey.Substring(0, idx1);
                var addressstr = RowKey.Substring(idx1 + 1, idx2 - idx1 - 1);
                var portstr = RowKey.Substring(idx2 + 1, idx3 - idx2 - 1);
                var genstr = RowKey.Substring(idx3 + 1);
                Address = IPAddress.Parse(addressstr);
                Port = Int32.Parse(portstr);
                Generation = Int32.Parse(genstr);
#if DEBUG
                debugInfo = String.Format("UnpackRowKey: RowKey={0} -> ClusterId= {4} Address={1} Port={2} Generation={3}", RowKey, Address, Port, Generation, ClusterId);
                Trace.TraceInformation(debugInfo);
#endif

                this.SiloAddress = SiloAddress.New(new IPEndPoint(Address, Port), Generation, ClusterId);
            }
            catch (Exception exc)
            {
                throw new AggregateException("Error from " + debugInfo, exc);
            }
        }

        internal MultiClusterConfiguration ToConfiguration()
        {
            string clusterliststring = Clusters;
            var clusterlist = clusterliststring.Split(',');
            var admintimestamp = (GossipTimestamp == default(DateTime) ? Timestamp.UtcDateTime : GossipTimestamp);
            return new MultiClusterConfiguration(GossipTimestamp, clusterlist);
        }

        internal GatewayEntry ToGatewayEntry()
        {
            // call this only after already unpacking row key
            return new GatewayEntry()
            {
                SiloAddress = SiloAddress,
                Status = (GatewayStatus) Enum.Parse(typeof(GatewayStatus), Status),
                HeartbeatTimestamp = GossipTimestamp
            };
        }

        public override string ToString()
        {
            if (RowKey == CONFIGURATION_ROW)
                return ToConfiguration().ToString();
            else
                return string.Format("{0} {1}",
                    this.SiloAddress, this.Status);
        }
    }

    internal class GossipTableInstanceManager
    {
        public string TableName { get { return INSTANCE_TABLE_NAME; } }

        private const string INSTANCE_TABLE_NAME = "OrleansGossipTable";

        private readonly AzureTableDataManager<GossipTableEntry> storage;
        private readonly TraceLogger logger;

        internal static TimeSpan initTimeout = AzureTableDefaultPolicies.TableCreationTimeout;

        public string GlobalServiceId { get; private set; }

        private GossipTableInstanceManager(string globalServiceId, string storageConnectionString)
        {
            GlobalServiceId = globalServiceId;
            logger = TraceLogger.GetLogger(this.GetType().Name, TraceLogger.LoggerType.Runtime);
            storage = new AzureTableDataManager<GossipTableEntry>(
                INSTANCE_TABLE_NAME, storageConnectionString, logger);
        }

        public static async Task<GossipTableInstanceManager> GetManager(string globalServiceId, string storageConnectionString)
        {
            Debug.Assert(!string.IsNullOrEmpty(globalServiceId));

            var instance = new GossipTableInstanceManager(globalServiceId, storageConnectionString);
            try
            {
                await instance.storage.InitTableAsync()
                    .WithTimeout(initTimeout);
            }
            catch (TimeoutException te)
            {
                string errorMsg = String.Format("Unable to create or connect to the Azure table in {0}", initTimeout);
                instance.logger.Error(ErrorCode.AzureTable_32, errorMsg, te);
                throw new OrleansException(errorMsg, te);
            }
            catch (Exception ex)
            {
                string errorMsg = String.Format("Exception trying to create or connect to the Azure table: {0}", ex.Message);
                instance.logger.Error(ErrorCode.AzureTable_33, errorMsg, ex);
                throw new OrleansException(errorMsg, ex);
            }
            return instance;
        }


        internal async Task<List<Tuple<GossipTableEntry, string>>> FindAllGossipTableEntries()
        {
            var queryResults = await storage.ReadAllTableEntriesForPartitionAsync(this.GlobalServiceId);

            return queryResults.ToList();
        }


        internal Task<Tuple<GossipTableEntry, string>> ReadConfigurationEntryAsync()
        {
            return storage.ReadSingleTableEntryAsync(this.GlobalServiceId, GossipTableEntry.CONFIGURATION_ROW);
        }

        internal Task<Tuple<GossipTableEntry, string>> ReadGatewayEntryAsync(GatewayEntry gateway)
        {
            return storage.ReadSingleTableEntryAsync(this.GlobalServiceId, GossipTableEntry.ConstructRowKey(gateway.SiloAddress));
        }

        internal async Task<bool> TryCreateConfigurationEntryAsync(MultiClusterConfiguration configuration)
        {
            Debug.Assert(configuration != null);

            var entry = new GossipTableEntry
            {
                PartitionKey = GlobalServiceId,
                RowKey = GossipTableEntry.CONFIGURATION_ROW,
                GossipTimestamp = configuration.AdminTimestamp,
                Clusters = string.Join(",", configuration.Clusters)
            };

            return (await TryCreateTableEntryAsync("TryCreateConfigurationEntryAsync", entry) != null);
        }


        internal async Task<bool> TryUpdateConfigurationEntryAsync(MultiClusterConfiguration configuration, GossipTableEntry entry, string eTag)
        {
            Debug.Assert(configuration != null);

            Debug.Assert(entry.ETag == eTag);
            Debug.Assert(entry.PartitionKey == GlobalServiceId);
            Debug.Assert(entry.RowKey == GossipTableEntry.CONFIGURATION_ROW);
            entry.GossipTimestamp = configuration.AdminTimestamp;
            entry.Clusters = string.Join(",", configuration.Clusters);

            return (await TryUpdateTableEntryAsync("TryUpdateConfigurationEntryAsync", entry, eTag) != null);
        }

        internal async Task<bool> TryCreateGatewayEntryAsync(GatewayEntry entry)
        {
            var row = new GossipTableEntry()
            {
                PartitionKey = GlobalServiceId,
                RowKey = GossipTableEntry.ConstructRowKey(entry.SiloAddress),
                Status = entry.Status.ToString(),
                GossipTimestamp = entry.HeartbeatTimestamp
            };

            return (await TryCreateTableEntryAsync("TryCreateGatewayEntryAsync", row) != null);
        }


        internal async Task<bool> TryUpdateGatewayEntryAsync(GatewayEntry entry, GossipTableEntry row, string eTag)
        {
            Debug.Assert(row.ETag == eTag);
            Debug.Assert(row.PartitionKey == GlobalServiceId);
            Debug.Assert(row.RowKey == GossipTableEntry.ConstructRowKey(entry.SiloAddress));
            row.Status = entry.Status.ToString();
            row.GossipTimestamp = entry.HeartbeatTimestamp;

            return (await TryUpdateTableEntryAsync("TryUpdateGatewayEntryAsync", row, eTag) != null);
        }

        internal async Task<bool> TryDeleteGatewayEntryAsync(GossipTableEntry row, string eTag)
        {
            Debug.Assert(row.ETag == eTag);
            Debug.Assert(row.PartitionKey == GlobalServiceId);
            Debug.Assert(row.RowKey == GossipTableEntry.ConstructRowKey(row.SiloAddress));


            return (await TryDeleteTableEntryAsync("TryDeleteGatewayEntryAsync", row, eTag) != null);
        }

        internal async Task<int> DeleteTableEntries()
        {

            var entries = await storage.ReadAllTableEntriesForPartitionAsync(GlobalServiceId);
            var entriesList = new List<Tuple<GossipTableEntry, string>>(entries);
            if (entriesList.Count <= AzureTableDefaultPolicies.MAX_BULK_UPDATE_ROWS)
            {
                await storage.DeleteTableEntriesAsync(entriesList);
            }
            else
            {
                List<Task> tasks = new List<Task>();
                foreach (var batch in entriesList.BatchIEnumerable(AzureTableDefaultPolicies.MAX_BULK_UPDATE_ROWS))
                {
                    tasks.Add(storage.DeleteTableEntriesAsync(batch));
                }
                await Task.WhenAll(tasks);
            }
            return entriesList.Count();
        }


        /// <summary>
        /// Try once to conditionally update a data entry in the Azure table. Returns null if etag does not match.
        /// </summary>
        public async Task<string> TryUpdateTableEntryAsync(string who, GossipTableEntry data, string dataEtag)
        {
            try
            {
                return await storage.UpdateTableEntryAsync(data, dataEtag);
            }
            catch (Exception exc)
            {
                HttpStatusCode httpStatusCode;
                string restStatus;
                if (!AzureStorageUtils.EvaluateException(exc, out httpStatusCode, out restStatus)) throw;

                if (logger.IsVerbose2) logger.Verbose2(who + " failed with httpStatusCode={0}, restStatus={1}", httpStatusCode, restStatus);
                if (AzureStorageUtils.IsContentionError(httpStatusCode)) return null;

                throw;
            }
        }

        /// <summary>
        /// Try once to insert a new data entry in the Azure table. Returns null if etag does not match.
        /// </summary>
        public async Task<string> TryCreateTableEntryAsync(string who, GossipTableEntry data)
        {
            try
            {
                return await storage.CreateTableEntryAsync(data);
            }
            catch (Exception exc)
            {
                HttpStatusCode httpStatusCode;
                string restStatus;
                if (!AzureStorageUtils.EvaluateException(exc, out httpStatusCode, out restStatus)) throw;

                if (logger.IsVerbose2) logger.Verbose2(who + " failed with httpStatusCode={0}, restStatus={1}", httpStatusCode, restStatus);
                if (AzureStorageUtils.IsContentionError(httpStatusCode)) return null;

                throw;
            }
        }
        /// <summary>
        /// Try once to delete an existing data entry in the Azure table. Returns false if etag does not match.
        /// </summary>
        public async Task<bool> TryDeleteTableEntryAsync(string who, GossipTableEntry data, string etag)
        {
            try
            {
                await storage.DeleteTableEntryAsync(data, etag);
                return true;
            }
            catch (Exception exc)
            {
                HttpStatusCode httpStatusCode;
                string restStatus;
                if (!AzureStorageUtils.EvaluateException(exc, out httpStatusCode, out restStatus)) throw;

                if (logger.IsVerbose2) logger.Verbose2(who + " failed with httpStatusCode={0}, restStatus={1}", httpStatusCode, restStatus);
                if (AzureStorageUtils.IsContentionError(httpStatusCode)) return false;

                throw;
            }
        }

    }
}
