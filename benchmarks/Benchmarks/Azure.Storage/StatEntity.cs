using System;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System.Text;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using Newtonsoft.Json.Linq;
using System.Linq;

namespace Azure.Storage
{
    [Serializable]
    public class StatEntity : TableEntity
    {
        // assume no . in partition key

        public string date { get; set; }
        public string throughput { get; set; }
        public System.Collections.Generic.Dictionary<string,Common.LatencyDistribution> latency { get; set; }
        public string benchmarkName { get; set; }
        public string scenarioName { get; set; }

        public StatEntity()
        {

        }

        public StatEntity(string pBenchmarkName, string pScenarioName, DateTime pDate, string pThroughput,
                System.Collections.Generic.Dictionary<string, Common.LatencyDistribution> pLatency) {
            this.benchmarkName = pBenchmarkName;
            this.scenarioName = pScenarioName;
            this.date = pDate.ToString();
            this.throughput = pThroughput;
            this.latency = pLatency;
            this.PartitionKey = AzureCommon.ToAzureKeyString(benchmarkName);
            this.RowKey = AzureCommon.ToAzureKeyString(scenarioName + pDate.ToString());
        }


       
    }




}