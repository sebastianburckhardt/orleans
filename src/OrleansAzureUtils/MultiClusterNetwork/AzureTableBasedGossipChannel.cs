using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans.Runtime.Configuration;
using Orleans.MultiCluster;

namespace Orleans.Runtime.MultiClusterNetwork
{
    /// <summary>
    /// An implementation of a gossip channel based on a standard Orleans Azure table.
    /// Multiple gossip networks can use the same table, and are separated by partition key = GlobalServiceId
    /// </summary>
    internal class AzureTableBasedGossipChannel : IGossipChannel
    {
        private TraceLogger logger;
        private GossipTableInstanceManager tableManager;
        private static int sequenceNumber;

        public string Name { get; private set; }

        public async Task Initialize(GlobalConfiguration globalconfig, string connectionstring)
        {
            Name = "AzureTableBasedGossipChannel-" + ++sequenceNumber;
            logger = TraceLogger.GetLogger(Name, TraceLogger.LoggerType.Runtime);

            logger.Info("Initializing Gossip Channel for ServiceId={0} using connection: {1}, SeverityLevel={2}",
                globalconfig.GlobalServiceId, ConfigUtilities.RedactConnectionStringInfo(connectionstring), logger.SeverityLevel);

            tableManager = await GossipTableInstanceManager.GetManager(globalconfig.GlobalServiceId, connectionstring, logger);
        }

        // used by unit tests
        public Task DeleteAllEntries()
        {
            logger.Info("DeleteAllEntries");
            return tableManager.DeleteTableEntries();
        }

      
        private static void UpdateDictionaryRightValue<TKey, TLeft, TRight>(Dictionary<TKey, KeyValuePair<TLeft, TRight>> dict, TKey key, TRight rightValue)
        {
            if (dict.ContainsKey(key))
                dict[key] = new KeyValuePair<TLeft, TRight>(dict[key].Key, rightValue);
            else
                dict.Add(key, new KeyValuePair<TLeft, TRight>(default(TLeft), rightValue));
        }

        // IGossipChannel
        public async Task Push(MultiClusterData data)
        {
            logger.Verbose("-Push data:{0}", data);

            var retrievalTasks = new List<Task<GossipTableEntry>>();
            if (data.Configuration != null)
            {
                retrievalTasks.Add(tableManager.ReadConfigurationEntryAsync());
            }

            foreach (var gateway in data.Gateways.Values)
            {
                retrievalTasks.Add(tableManager.ReadGatewayEntryAsync(gateway));
            }

            await Task.WhenAll(retrievalTasks);

            var entriesFromStorage = retrievalTasks.Select(t => t.Result).Where(entry => entry != null);
            await DiffAndWriteBack(data, entriesFromStorage); 
        }

        // IGossipChannel
        public async Task<MultiClusterData> PushAndPull(MultiClusterData pushed)
        {
            logger.Verbose("-PushAndPull pushed:{0}", pushed);

            try
            {
                var entriesFromStorage = await tableManager.FindAllGossipTableEntries();
                var delta = await DiffAndWriteBack(pushed, entriesFromStorage);

                logger.Verbose("-PushAndPull pulled delta:{0}", delta);

                return delta;
            }
            catch (Exception e)
            {
                logger.Info("-PushAndPull encountered exception {0}", e);

                throw e;
            }
        }

        internal async Task<MultiClusterData> DiffAndWriteBack(MultiClusterData dataToPush, IEnumerable<GossipTableEntry> entriesFromStorage)
        {
            GossipTableEntry configRow = null;
            var gateways = new Dictionary<SiloAddress, KeyValuePair<GatewayEntry, GossipTableEntry>>();
            MultiClusterConfiguration returnedConfiguration = null;

            // collect left-hand side data
            MultiClusterConfiguration configToPush = dataToPush.Configuration;
            foreach (var gatewayEntry in dataToPush.Gateways.Values)
            {
                if (!gatewayEntry.Expired)
                    gateways[gatewayEntry.SiloAddress] = new KeyValuePair<GatewayEntry, GossipTableEntry>(gatewayEntry, null);
            }

            foreach (var tableEntry in entriesFromStorage)
            {
                if (tableEntry.RowKey.Equals(GossipTableEntry.CONFIGURATION_ROW))
                {
                    configRow = tableEntry;
                    // interpret empty admin timestamp by taking the azure table timestamp instead
                    // this allows an admin to inject a configuration by editing table more easily
                    if (configRow.GossipTimestamp == default(DateTime))
                        configRow.GossipTimestamp = configRow.Timestamp.UtcDateTime;
                }
                else
                {
                    try
                    {
                        tableEntry.UnpackRowKey();
                        UpdateDictionaryRightValue(gateways, tableEntry.SiloAddress, tableEntry);
                    }
                    catch (Exception exc)
                    {
                        logger.Error(
                            ErrorCode.AzureTable_61,
                            string.Format("Intermediate error parsing GossipTableEntry: {0}. Ignoring this entry.", tableEntry),
                            exc);
                    }
                }
            }

            var writeback = new List<Task>();
            var sendback = new Dictionary<SiloAddress,GatewayEntry>();

            if (configToPush != null &&
                (configRow == null || configRow.GossipTimestamp < configToPush.AdminTimestamp))
            {
                // push configuration
                if (configRow == null)
                    writeback.Add(tableManager.TryCreateConfigurationEntryAsync(configToPush));
                else
                    writeback.Add(tableManager.TryUpdateConfigurationEntryAsync(configToPush, configRow, configRow.ETag));
            }
            else if (configRow != null &&
                 (configToPush == null || configToPush.AdminTimestamp < configRow.GossipTimestamp))
            {
                // pull configuration
                returnedConfiguration = configRow.ToConfiguration();
            }

            foreach (var gatewayEntryPair in gateways.Values)
            {
                GatewayEntry gatewayEntry = gatewayEntryPair.Key;
                GossipTableEntry tableEntry = gatewayEntryPair.Value;

                if ((gatewayEntry != null && !gatewayEntry.Expired)
                     && (tableEntry == null || tableEntry.GossipTimestamp < gatewayEntry.HeartbeatTimestamp))
                {
                    // push gateway entry, since we have a newer value
                    if (tableEntry == null)
                    {
                        writeback.Add(tableManager.TryCreateGatewayEntryAsync(gatewayEntry));
                    }
                    else
                    {
                        writeback.Add(tableManager.TryUpdateGatewayEntryAsync(gatewayEntry, tableEntry, tableEntry.ETag));
                    }
                }
                else if (tableEntry != null &&
                        (gatewayEntry == null || gatewayEntry.HeartbeatTimestamp < tableEntry.GossipTimestamp))
                {
                    // pull or remove gateway entry
                    gatewayEntry = tableEntry.ToGatewayEntry();
                    if (gatewayEntry.Expired)
                    {
                        writeback.Add(tableManager.TryDeleteGatewayEntryAsync(tableEntry, tableEntry.ETag));
                    }
                    else
                    {
                        // gets sent back
                        sendback.Add(tableEntry.SiloAddress, tableEntry.ToGatewayEntry());
                    }
                }
            }

            await Task.WhenAll(writeback);

            return new MultiClusterData(sendback, returnedConfiguration);
        }
    }
}
