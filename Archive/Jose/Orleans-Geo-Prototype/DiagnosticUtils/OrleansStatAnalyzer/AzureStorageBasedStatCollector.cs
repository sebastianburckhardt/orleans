using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace OrleansStatAnalyzer
{

    internal class OrleansSiloEntity : TableEntity
    {
        public OrleansSiloEntity(string testName, string key)
        {
            this.PartitionKey = testName;
            this.RowKey = key;
        }

        public OrleansSiloEntity() { }

        public string Name { get; set; }
        public string Statistic { get; set; }
        public string StatValue { get; set; }
        public DateTime Time { get; set; }
    }

    public class AzureStorageBasedStatCollector : StatCollector
    {
        public string ConnectionString { get; set; }
        public string TableName { get; set; }
        public string PartitionKey { get; set; }

        public override Dictionary<string, SiloInstance> RetreiveData(HashSet<string> counters)
        {
            CloudTable table = ConnectToAzureDataStore();
            return CollectData(counters, table);
        }


        private CloudTable ConnectToAzureDataStore()
        {
            CloudStorageAccount orleansAccount = CloudStorageAccount.Parse(ConnectionString);
            CloudTableClient tableClient = orleansAccount.CreateCloudTableClient();
            return tableClient.GetTableReference(TableName);
        }


        private string BuildQuery(HashSet<string> counters)
        {
            string query = string.Format("PartitionKey eq '{0}'", PartitionKey);
            return query;
        }


        //private string BuildQuery(HashSet<string> counters)
        //{
        //    int count = counters.Count();

        //    if (count > 0)
        //    {
        //        string query = string.Format("PartitionKey eq '{0}' and (", PartitionKey);

        //        foreach (string statname in counters)
        //        {
        //            count--;

        //            if (count == 0)
        //            {
        //                string s = string.Format("Statistic eq '{0}')", statname);
        //                query = query + s;
        //            }
        //            else
        //            {

        //                string s = string.Format("Statistic eq '{0}' or ", statname);
        //                query = query + s;
        //            }
        //        }
        //        return query;
        //    }
        //    else
        //    {
        //        Console.WriteLine("Need to provide at least one statistic");
        //        return null;
        //    }
            
        //}


        private Dictionary<string, SiloInstance> CollectData(HashSet<string> counters, CloudTable table)
        {
            // First setup the inmemory structure to keep data

            Dictionary<string, SiloInstance> silos = new Dictionary<string, SiloInstance>();

            // Do one pass of Data storage and extratct the entities for this partition
            string statQuery = BuildQuery(counters);

            TableQuery<OrleansSiloEntity> query = new TableQuery<OrleansSiloEntity>().Where(statQuery);
            foreach(OrleansSiloEntity entity in table.ExecuteQuery(query))
            {
                counters.Add(entity.Statistic);

                // Add the silo name if it is not in the list
                if(!silos.ContainsKey(entity.Name))
                {
                    SiloInstance silo = new SiloInstance(entity.Name);
                    silos.Add(entity.Name, silo);
                }

                // Add the statistic if it is not in this silos's stat list
                if (!(silos[entity.Name].SiloStatistics.ContainsKey(entity.Statistic)))
                { 
                    OrleanStatistic orleanStat = new OrleanStatistic(entity.Statistic);
                    silos[entity.Name].SiloStatistics.Add(entity.Statistic, orleanStat);   
                }

                silos[entity.Name].SiloStatistics[entity.Statistic].TimeVals.Add(entity.Time, Convert.ToDouble(entity.StatValue));
            }

            // This is required to plot the surface graphs
            foreach (KeyValuePair<string, SiloInstance> pair in silos)
            {
                pair.Value.SetEarliestTime();
                pair.Value.SetLatestTime();
            }

            return silos;
        }
    }
}
