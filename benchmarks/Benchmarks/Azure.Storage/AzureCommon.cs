using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure;

namespace Azure.Storage
{
    /// <summary>
    /// Utility class for Azure Storage
    /// </summary>
    class AzureCommon
    {
        /// <summary>
        /// Returns Storage account associated with specific StorageConnectionString
        /// </summary>
        /// <returns></returns>
        public static  CloudStorageAccount getStorageAccount()
        {
            string connectionKey = CloudConfigurationManager.GetSetting("StorageConnectionString");
            if (connectionKey == null)
            {
                throw new Exception("No connection key specified");
            }
            return CloudStorageAccount.Parse(connectionKey);
        }

        public static CloudTableClient getTableClient(CloudStorageAccount pAccount)
        {
            return pAccount.CreateCloudTableClient();
        }

        public static CloudTable createTable(CloudTableClient pClient, string pName)
        {
            CloudTable table = pClient.GetTableReference(pName);
            table.CreateIfNotExists();
            return table;
        }


        public static void deleteTable(CloudTableClient pClient, string pName)
        {
            CloudTable table = pClient.GetTableReference(pName);
            table.Delete();
        }

        public static TableResult insertEntity(CloudTableClient pClient, string pTableName,
                                        TableEntity pEntity)
        {
            CloudTable table = pClient.GetTableReference(pTableName);
            if (table == null)
            {
                //TODO: throw exception?
                return null;
            }
            var retValue = table.Execute(TableOperation.Insert(pEntity));
            return retValue;
        }

        public static TableResult updateEntity(CloudTableClient pClient, string pTableName,
                                       TableEntity pEntity)
        {
            CloudTable table = pClient.GetTableReference(pTableName);
            if (table == null)
            {
                //TODO: throw exception?
                return null;
            }
            var retValue = table.Execute(TableOperation.Replace(pEntity));
            return retValue;
        }

        public static IList<TableResult> insertEntities(CloudTableClient pClient, string pTableName,
                                                IList<TableEntity> pEntityList)
        {
            CloudTable table = pClient.GetTableReference(pTableName);
            if (table == null)
            {
                //TODO: throw exception?
                return null;
            }
            TableBatchOperation batch = new TableBatchOperation();
            foreach (TableEntity ent in pEntityList) {
                batch.Insert(ent);
            }

            return table.ExecuteBatch(batch);
        }


        public static IEnumerable<TableEntity> findEntitiesInPartition(CloudTableClient pClient, string pName, string pPartitionKey)
        {
            CloudTable table = pClient.GetTableReference(pName);
            TableQuery<TableEntity> rangeQuery = new TableQuery<TableEntity>().Where(
                        TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, pPartitionKey));
            return table.ExecuteQuery(rangeQuery);
        }

        public static TableResult findEntity(CloudTableClient pClient, string pName, string pPartitionKey, string pRowKey)
        {
            CloudTable table = pClient.GetTableReference(pName);
            TableOperation op = TableOperation.Retrieve<TableEntity>(pPartitionKey, pRowKey);
            return table.Execute(op);
        }


        public static TableResult deleteEntity(CloudTableClient pClient, string pName, string pPartitionKey, string pRowKey)
        {
            CloudTable table = pClient.GetTableReference(pName);
            TableOperation op = TableOperation.Retrieve<TableEntity>(pPartitionKey, pRowKey);
            TableEntity entityToDelete =  (TableEntity) table.Execute(op).Result;
            if (entityToDelete != null)
            {
               return table.Execute(TableOperation.Delete(entityToDelete));
            }
            else
            {
                return null;
            }
        }

        public static IEnumerable<T> findEntitiesProjection<T>(CloudTableClient pClient, string pName, string[] properties, EntityResolver<T> pEntityResolver)
        {
            CloudTable table = pClient.GetTableReference(pName);
            TableQuery<DynamicTableEntity> projectionQuery = new TableQuery<DynamicTableEntity>().Select(properties);
            return table.ExecuteQuery(projectionQuery, pEntityResolver);
        }


    }
}
