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
    internal class ClientMetricsData : TableServiceEntity
    {
        public string HostName { get; set; }
        public string Address { get; set; }

        public double CPU { get; set; }
        public long Memory { get; set; }
        public int SendQueue { get; set; }
        public int ReceiveQueue { get; set; }
        public long SentMessages { get; set; }
        public long ReceivedMessages { get; set; }
        public long ConnectedGWCount { get; set; }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("OrleansClientMetricsData[");

            sb.Append(" DeploymentId=").Append(PartitionKey);
            sb.Append(" ClientId=").Append(RowKey);

            sb.Append(" Host=").Append(HostName);
            sb.Append(" Address=").Append(Address);
            sb.Append(" CPU=").Append(CPU);
            sb.Append(" Memory=").Append(Memory);
            sb.Append(" SendQueue=").Append(SendQueue);
            sb.Append(" ReceiveQueue=").Append(ReceiveQueue);
            sb.Append(" SentMessages=").Append(SentMessages);
            sb.Append(" ReceivedMessages=").Append(ReceivedMessages);
            sb.Append(" Clients=").Append(ConnectedGWCount);

            sb.Append(" ]");
            return sb.ToString();
        }
    }

    internal class ClientMetricsTableDataManager : AzureTableDataManager<ClientMetricsData>, IClientMetricsDataPublisher
    {
        protected const string INSTANCE_TABLE_NAME = "OrleansClientMetrics";

        private readonly string DeploymentId;
        private readonly string ClientId;
        private readonly IPAddress Address;
        private readonly string myHostName;

        private ClientMetricsTableDataManager(ClientConfiguration config, string clientId, IPAddress address)
            : base(INSTANCE_TABLE_NAME, config.DataConnectionString)
        {
            this.DeploymentId = config.DeploymentId;
            this.ClientId = clientId;
            this.Address = address;
            this.myHostName = config.DNSHostName;
        }

        public static async Task<ClientMetricsTableDataManager> GetManager(ClientConfiguration config, string clientId, IPAddress address)
        {
            ClientMetricsTableDataManager instance = new ClientMetricsTableDataManager(config, clientId, address);
            await instance.InitTableAsync().WithTimeout(AzureTableDefaultPolicies.TableCreation_TIMEOUT);
            return instance;
        }

        #region IMetricsDataPublisher methods

        public Task ReportMetrics(IClientPerformanceMetrics metricsData)
        {
            var clientMetricsTableEntry = PopulateClientMetricsDataTableEntry(metricsData);

            if (logger.IsVerbose) logger.Verbose("Updating client metrics table entry: {0}", clientMetricsTableEntry);

            return UpsertTableEntryAsync(clientMetricsTableEntry);
        }

        #endregion

        private ClientMetricsData PopulateClientMetricsDataTableEntry(IClientPerformanceMetrics metricsData)
        {
            ClientMetricsData metricsDataObject = new ClientMetricsData();

            // Add data row header info
            metricsDataObject.PartitionKey = this.DeploymentId;
            metricsDataObject.RowKey = this.ClientId;

            metricsDataObject.Address = this.Address.ToString();
            metricsDataObject.HostName = myHostName;

            // Add metrics data
            metricsDataObject.CPU = metricsData.CpuUsage;
            metricsDataObject.Memory = metricsData.MemoryUsage;
            metricsDataObject.SendQueue = metricsData.SendQueueLength;
            metricsDataObject.ReceiveQueue = metricsData.ReceiveQueueLength;
            metricsDataObject.SentMessages = metricsData.SentMessages;
            metricsDataObject.ReceivedMessages = metricsData.ReceivedMessages;
            metricsDataObject.ConnectedGWCount = metricsData.ConnectedGWCount;

            return metricsDataObject;
        }
    }
}
