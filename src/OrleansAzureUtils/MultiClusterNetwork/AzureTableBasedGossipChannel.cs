using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans.Runtime.Configuration;
using Orleans.MultiCluster;

namespace Orleans.Runtime.MultiClusterNetwork
{
    // An implementation of a gossip channel based on a  standard orleans azure table
    // multiple gossipnetworks can use the same table, and are separated by pkey = GlobalServiceId
    internal class AzureTableBasedGossipChannel : IGossipChannel
    {

        private TraceLogger logger;
        private GossipTableInstanceManager tableManager;
        private static int seqno;

        public string Name { get; private set; }

        public async Task Initialize(GlobalConfiguration globalconfig, string connectionstring)
        {
            Name = "AzureTableBasedGossipChannel-" + ++seqno;
            logger = TraceLogger.GetLogger(Name, TraceLogger.LoggerType.Runtime);

            logger.Info("Initializing Gossip Channel for ServiceId={0} using connection: {1}, SeverityLevel={2}",
                globalconfig.GlobalServiceId, ConfigUtilities.RedactConnectionStringInfo(connectionstring), logger.SeverityLevel);

            tableManager =
                await GossipTableInstanceManager.GetManager(globalconfig.GlobalServiceId, connectionstring, logger);
        }

        // used by unit tests
        public Task DeleteAllEntries()
        {
            logger.Info("DeleteAllEntries");
            return tableManager.DeleteTableEntries();
        }

        public struct Pair<L,R>
        {
            public L Item1;
            public R Item2;
        }
        private static void UpdateDictionaryRight<T, L, R>(Dictionary<T, Pair<L, R>> d, T key, R val)
        {
            Pair<L, R> current;
            d.TryGetValue(key, out current);
            current.Item2 = val;
            d[key] = current;
        }

        
        // IGossipChannel
        public async Task Push(MultiClusterData data)
        {
            logger.Verbose("-Push data:{0}", data);

            var retrievaltasks = new List<Task<Tuple<GossipTableEntry,string>>>();
            if (data.Configuration != null)
                retrievaltasks.Add(tableManager.ReadConfigurationEntryAsync());
            foreach(var gateway in data.Gateways.Values)
                retrievaltasks.Add(tableManager.ReadGatewayEntryAsync(gateway));

            await Task.WhenAll(retrievaltasks);

            var entriesfromstorage = retrievaltasks.Select(t => t.Result).Where(tuple => tuple != null);
            await DiffAndWriteBack(data, entriesfromstorage); 
        }

        // IGossipChannel
        public async Task<MultiClusterData> PushAndPull(MultiClusterData pushed)
        {
            logger.Verbose("-PushAndPull pushed:{0}", pushed);

            try
            {

                var entriesfromstorage = await tableManager.FindAllGossipTableEntries();
                var delta = await DiffAndWriteBack(pushed, entriesfromstorage);

                logger.Verbose("-PushAndPull pulled delta:{0}", delta);

                return delta;
            }
            catch (Exception e)
            {
                logger.Info("-PushAndPull encountered exception {0}", e);

                throw e;
            }

        }

        internal async Task<MultiClusterData> DiffAndWriteBack(MultiClusterData pushed, IEnumerable<Tuple<GossipTableEntry, string>> entriesfromstorage)
        {

            MultiClusterConfiguration conf1;
            Tuple<GossipTableEntry, string> conf2 = null;
            var gateways = new Dictionary<SiloAddress, Pair<GatewayEntry, Tuple<GossipTableEntry, string>>>();
            MultiClusterConfiguration returnedconf = null;

            // collect left-hand side data
            conf1 = pushed.Configuration;
            foreach (var e in pushed.Gateways)
               if (!e.Value.Expired)
                   gateways[e.Key] = new Pair<GatewayEntry, Tuple<GossipTableEntry, string>> { Item1 = e.Value };

            foreach (var tuple in entriesfromstorage)
            {
                var tableEntry = tuple.Item1;
                if (tableEntry.RowKey.Equals(GossipTableEntry.CONFIGURATION_ROW))
                {
                    conf2 = tuple;
                    // interpret empty admin timestamp by taking the azure table timestamp instead
                    // this allows an admin to inject a configuration by editing table more easily
                    if (conf2.Item1.GossipTimestamp == default(DateTime))
                        conf2.Item1.GossipTimestamp = conf2.Item1.Timestamp.UtcDateTime;
                }
                else
                {
                    try
                    {
                        tableEntry.UnpackRowKey();
                        UpdateDictionaryRight(gateways, tableEntry.SiloAddress, tuple);
                    }
                    catch (Exception exc)
                    {
                        logger.Error(ErrorCode.AzureTable_61, String.Format(
                            "Intermediate error parsing GossipTableEntry: {0}. Ignoring this entry.",
                            tableEntry), exc);
                    }
                }
            }

            var writeback = new List<Task>();
            var sendback = new Dictionary<SiloAddress,GatewayEntry>();

            // push configuration
            if (conf1 != null &&
                (conf2 == null || conf2.Item1.GossipTimestamp < conf1.AdminTimestamp))
            {
                if (conf2 == null)
                    writeback.Add(tableManager.TryCreateConfigurationEntryAsync(conf1));
                else
                    writeback.Add(tableManager.TryUpdateConfigurationEntryAsync(conf1, conf2.Item1, conf2.Item2));
            }
           // pull configuration
            else if (conf2 != null &&
                 (conf1 == null || conf1.AdminTimestamp < conf2.Item1.GossipTimestamp))
            {
                returnedconf = conf2.Item1.ToConfiguration();
            }

            foreach (var pair in gateways)
            {
                var left = pair.Value.Item1;
                var right = pair.Value.Item2;

                // push gateway entry
                if ((left != null && !left.Expired)
                     && (right == null || right.Item1.GossipTimestamp < left.HeartbeatTimestamp))
                {
                    if (right == null)
                        writeback.Add(tableManager.TryCreateGatewayEntryAsync(left));
                    else
                        writeback.Add(tableManager.TryUpdateGatewayEntryAsync(left, right.Item1, right.Item2));
                }
                // pull or remove gateway entry
                else if (right != null &&
                        (left == null || left.HeartbeatTimestamp < right.Item1.GossipTimestamp))
                {
                    var gatewayentry = right.Item1.ToGatewayEntry();
                    if (gatewayentry.Expired)
                        writeback.Add(tableManager.TryDeleteGatewayEntryAsync(right.Item1, right.Item2));
                    else
                        sendback.Add(right.Item1.SiloAddress, right.Item1.ToGatewayEntry()); // gets sent back
                }

            }

            await Task.WhenAll(writeback);

            return new MultiClusterData(sendback, returnedconf);
        }
    }
}
