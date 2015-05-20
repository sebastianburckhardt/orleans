using System;
using System.Collections.Generic;
using System.Data.Services.Client;
using System.Data.Services.Common;
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
    internal class SiloMetricsData : TableServiceEntity
    {
        public string HostName { get; set; }
        public string Address { get; set; }
        public int Port { get; set; }
        public string GatewayAddress { get; set; }
        public int GatewayPort { get; set; }
        public int Generation { get; set; }

        public double CPU { get; set; }
        public long Memory { get; set; }
        public int Activations { get; set; }
        public int SendQueue { get; set; }
        public int ReceiveQueue { get; set; }
        public long RequestQueue { get; set; }
        public long SentMessages { get; set; }
        public long ReceivedMessages { get; set; }
        public bool LoadShedding { get; set; }
        public long ClientCount { get; set; }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("OrleansSiloMetricsData[");

            sb.Append(" DeploymentId=").Append(PartitionKey);
            sb.Append(" SiloId=").Append(RowKey);

            sb.Append(" Host=").Append(HostName);
            sb.Append(" Endpoint=").Append(Address + ":" + Port);
            sb.Append(" Generation=").Append(Generation);

            sb.Append(" CPU=").Append(CPU);
            sb.Append(" Memory=").Append(Memory);
            sb.Append(" Activations=").Append(Activations);
            sb.Append(" SendQueue=").Append(SendQueue);
            sb.Append(" ReceiveQueue=").Append(ReceiveQueue);
            sb.Append(" RequestQueue=").Append(RequestQueue);
            sb.Append(" SentMessages=").Append(SentMessages);
            sb.Append(" ReceivedMessages=").Append(ReceivedMessages);
            sb.Append(" LoadShedding=").Append(LoadShedding);
            sb.Append(" Clients=").Append(ClientCount);

            sb.Append(" ]");
            return sb.ToString();
        }
    }

    //internal class SiloMetricsDataReader : SiloMetricsTableDataManager
    //{
    //    public SiloMetricsDataReader(string deploymentId, string storageConnectionString)
    //        : base(deploymentId, storageConnectionString)
    //    {
    //    }

    //    public AsyncValue<IEnumerable<SiloMetricsData>> GetSiloMetrics()
    //    {
    //        // Get everything
    //        var dataPromise = ReadTableEntriesAndEtags(instance => instance.PartitionKey == this.DeploymentId);
    //        return dataPromise.ContinueWith((IEnumerable<Tuple<SiloMetricsData, string>> data) => data.Select(tuple => tuple.Item1));
    //    }
    //}

    internal class SiloMetricsTableDataManager : AzureTableDataManager<SiloMetricsData>, ISiloMetricsDataPublisher
    {
        private const string INSTANCE_TABLE_NAME = "OrleansSiloMetrics";
        private readonly string DeploymentId;
        private readonly string SiloId;
        private readonly SiloAddress SiloAddress;
        private readonly IPEndPoint Gateway;
        private readonly string myHostName;
        private readonly SiloMetricsData metricsDataObject = new SiloMetricsData();

        private SiloMetricsTableDataManager(string deploymentId, string storageConnectionString, string siloId, SiloAddress siloAddress, IPEndPoint gateway, string hostName)
            : base(INSTANCE_TABLE_NAME, storageConnectionString)
        {
            this.DeploymentId = deploymentId;
            this.SiloId = siloId;
            this.SiloAddress = siloAddress;
            this.Gateway = gateway;
            this.myHostName = hostName;
        }

        public static async Task<SiloMetricsTableDataManager> GetManager(string deploymentId, string storageConnectionString, string siloId, SiloAddress siloAddress, IPEndPoint gateway, string hostName)
        {
            SiloMetricsTableDataManager instance = new SiloMetricsTableDataManager(deploymentId, storageConnectionString, siloId, siloAddress, gateway, hostName);
            await instance.InitTableAsync().WithTimeout(AzureTableDefaultPolicies.TableCreation_TIMEOUT);
            return instance;
        }

        #region IMetricsDataPublisher methods

        public Task ReportMetrics(ISiloPerformanceMetrics metricsData)
        {
            var siloMetricsTableEntry = PopulateSiloMetricsDataTableEntry(metricsData);

            if (logger.IsVerbose) logger.Verbose("Updating silo metrics table entry: {0}", siloMetricsTableEntry);

            return UpsertTableEntryAsync(siloMetricsTableEntry);
        }

        #endregion

        private SiloMetricsData PopulateSiloMetricsDataTableEntry(ISiloPerformanceMetrics metricsData)
        {
            // NOTE: Repeatedly re-uses a single SiloMetricsData object, updated with the latest current data

            // Add data row header info
            metricsDataObject.PartitionKey = this.DeploymentId;
            metricsDataObject.RowKey = this.SiloId;
            metricsDataObject.Timestamp = DateTime.UtcNow;

            metricsDataObject.Address = this.SiloAddress.Endpoint.Address.ToString();
            metricsDataObject.Port = this.SiloAddress.Endpoint.Port;
            if (this.Gateway != null)
            {
                metricsDataObject.GatewayAddress = this.Gateway.Address.ToString();
                metricsDataObject.GatewayPort = this.Gateway.Port;
            }
            metricsDataObject.Generation = this.SiloAddress.Generation;
            metricsDataObject.HostName = myHostName;

            // Add metrics data
            metricsDataObject.CPU = metricsData.CpuUsage;
            metricsDataObject.Memory = metricsData.MemoryUsage;
            metricsDataObject.Activations = metricsData.ActivationCount;
            metricsDataObject.SendQueue = metricsData.SendQueueLength;
            metricsDataObject.ReceiveQueue = metricsData.ReceiveQueueLength;
            metricsDataObject.RequestQueue = metricsData.RequestQueueLength;
            metricsDataObject.SentMessages = metricsData.SentMessages;
            metricsDataObject.ReceivedMessages = metricsData.ReceivedMessages;
            metricsDataObject.LoadShedding = metricsData.IsOverloaded;
            metricsDataObject.ClientCount = metricsData.ClientCount;
            return metricsDataObject;
        }
    }
}
