using System;
using System.Collections.Generic;
using System.Data.Services.Common;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.StorageClient;
using Orleans.Counters;

namespace Orleans.AzureUtils
{
    [Serializable]
    [DataServiceKey("PartitionKey", "RowKey")]
    internal class StatsTableData : TableServiceEntity
    {
        public DateTime Time { get; set; }
        public string Address { get; set; }
        public string Name { get; set; }
        public string HostName { get; set; }

        public string Statistic { get; set; }
        public string StatValue { get; set; }
        public bool IsDelta { get; set; }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("StatsTableData[");
            sb.Append(" DeploymentId=").Append(PartitionKey);
            sb.Append(" RowKey=").Append(RowKey);
            sb.Append(" Time=").Append(Time);
            sb.Append(" Address=").Append(Address);
            sb.Append(" Name=").Append(Name);
            sb.Append(" HostName=").Append(HostName);
            sb.Append(" Statistic=").Append(Statistic);
            sb.Append(" StatValue=").Append(StatValue);
            sb.Append(" IsDelta=").Append(IsDelta);
            sb.Append(" ]");
            return sb.ToString();
        }
    }

    internal class StatsTableDataManager : AzureTableDataManager<StatsTableData>
    {
        private readonly string DeploymentId;
        private readonly string Address;
        private readonly string Name;
        private readonly bool IsSilo;
        private readonly long ClientEpoch;
        private int Counter;
        private readonly string myHostName;
        private static readonly string DateFormat = "yyyy-MM-dd";

        private StatsTableDataManager(bool isSilo, string storageConnectionString, string deploymentId, string address, string siloName, string hostName)
            : base(isSilo ? "OrleansSiloStatistics" : "OrleansClientStatistics", storageConnectionString)
        {
            DeploymentId = deploymentId;
            Address = address;
            Name = siloName;
            myHostName = hostName;
            IsSilo = isSilo;
            if (!IsSilo)
            {
                ClientEpoch = SiloAddress.AllocateNewGeneration();
            }
            Counter = 0;
        }

        internal static async Task<StatsTableDataManager> GetManager(bool isSilo, string storageConnectionString, string deploymentId, string address, string siloName, string hostName)
        {
            StatsTableDataManager instance = new StatsTableDataManager(isSilo, storageConnectionString, deploymentId, address, siloName, hostName);
            await instance.InitTableAsync().WithTimeout(AzureTableDefaultPolicies.TableCreation_TIMEOUT);
            return instance;
        }

        internal Task ReportStats(List<IOrleansCounter> statsCounters)
        {
            List<Task> bulkPromises = new List<Task>();
            List<StatsTableData> data = new List<StatsTableData>();
            foreach (IOrleansCounter counter in statsCounters.Where(cs => cs.Storage == CounterStorage.LogAndTable).OrderBy(cs => cs.Name))
            {
                if (data.Count >= AzureTableDefaultPolicies.MAX_BULK_UPDATE_ROWS)
                {
                    // Write intermediate batch
                    bulkPromises.Add(BulkInsertTableEntries(data));
                    data.Clear();
                }

                StatsTableData statsTableEntry = PopulateStatsTableDataEntry(counter);
                if (statsTableEntry == null) continue; // Skip blank entries
                if (logger.IsVerbose2) logger.Verbose2("Preparing to bulk insert {1} stats table entry: {0}", statsTableEntry, IsSilo ? "silo" : "");
                data.Add(statsTableEntry);
            }
            if (data.Count > 0)
            {
                // Write final batch
                bulkPromises.Add(BulkInsertTableEntries(data));
            }
            return Task.WhenAll(bulkPromises);
        }

        private StatsTableData PopulateStatsTableDataEntry(IOrleansCounter statsCounter)
        {
            StatsTableData entry = new StatsTableData();

            entry.StatValue = statsCounter.GetValueString();
            if ("0".Equals(entry.StatValue))
            {
                // Skip writing empty records
                return null;
            }

            Counter++;
            DateTime time = DateTime.UtcNow;
            string timeStr = time.ToString(DateFormat, CultureInfo.InvariantCulture);
            entry.PartitionKey = this.DeploymentId + ":" + timeStr;
            string counterStr = String.Format("{0:000000}", Counter);
            if (IsSilo)
            {
                entry.RowKey = this.Address + ":" + counterStr;
            }
            else
            {
                entry.RowKey = this.Name + ":" + ClientEpoch + ":" + counterStr;
            }
            entry.Time = time;
            entry.Address = this.Address;
            entry.Name = this.Name;
            entry.HostName = myHostName;
            entry.Statistic = statsCounter.Name;
            entry.IsDelta = statsCounter.IsValueDelta;
            return entry;
        }
    }
}
