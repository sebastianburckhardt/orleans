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
        public Common.LatencyDistribution latency { get; set; }        
        public string benchmarkName { get; set; }
        public string scenarioName { get; set; }

        public StatEntity()
        {

        }

        public StatEntity(string pBenchmarkName, string pScenarioName, DateTime pDate, string pThroughput, Common.LatencyDistribution pLatency) {
            this.benchmarkName = pBenchmarkName;
            this.scenarioName = pScenarioName;
            this.date = pDate.ToString();
            this.throughput = pThroughput;
            this.latency = pLatency;
            this.PartitionKey = Common.AzureUtils.ToAzureKeyString(benchmarkName);
            this.RowKey = Common.AzureUtils.ToAzureKeyString(scenarioName + pDate.ToString());
        }




        public override System.Collections.Generic.IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            var results = base.WriteEntity(operationContext);

            if (this.latency != null)
            {
                var counts = latency.Counts;
                results.Add("MaxLatency", new EntityProperty(latency.Max));
                results.Add("MinLatency", new EntityProperty(latency.Min));
                results.Add("TotalLatency", new EntityProperty(latency.Total));
                for (int i = 0; i < counts.Length; i++)
                {
                    results.Add("L_"+i.ToString(), new EntityProperty(counts[i]));
                }
                /*using (MemoryStream ms = new MemoryStream())
                {
                    BinaryFormatter bf = new BinaryFormatter();
                    bf.Serialize(ms, this.latency);
                    results.Add("Latencies", new EntityProperty(ms.ToArray()));
                }*/
            }
            return results;
        }

        public override void ReadEntity(System.Collections.Generic.IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            base.ReadEntity(properties, operationContext);
            this.latency = new Common.LatencyDistribution();
            latency.Init();
            try
            {
                latency.Max = properties["MaxLatency"].Int64Value.GetValueOrDefault(-1);
                latency.Min = properties["MinLatency"].Int64Value.GetValueOrDefault(-1);
                latency.Total = properties["TotalLatency"].Int32Value.GetValueOrDefault(-1);

                /*results.Add(, new EntityProperty(latency.Max));
                results.Add("", new EntityProperty(latency.Min));
                results.Add("", new EntityProperty(latency.Total));*/

                foreach (var prop in properties)
                {
                    if (prop.Key.StartsWith("L_"))
                    {
                        int upos = prop.Key.IndexOf("_");
                        int idx = int.Parse(prop.Key.Substring(upos + 1));
                        latency.Counts[idx] = prop.Value.Int32Value.GetValueOrDefault(-1);
                    }
                }
            }
            catch(Exception)
            {
                //?
            }
            /*var latencyEntity = properties["Latencies"];
            if (latencyEntity != null)
            {
                using (MemoryStream ms = new MemoryStream(latencyEntity.BinaryValue))
                {
                    BinaryFormatter bf = new BinaryFormatter();
                    this.latency = (Common.LatencyDistribution) bf.Deserialize(ms);
                }
            }*/
        }

    }




}