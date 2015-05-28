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
    public class AzureCommon
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

        public static CloudTableClient getTableClient()
        {
            string connectionKey = CloudConfigurationManager.GetSetting("StorageConnectionString");
            if (connectionKey == null)
            {
                throw new Exception("No connection key specified");
            }
            CloudStorageAccount account = CloudStorageAccount.Parse(connectionKey);
            return account.CreateCloudTableClient();
        }

        public static CloudTable createTable(CloudTableClient pClient, string pName)
        {
            CloudTable table = pClient.GetTableReference(pName);
            table.CreateIfNotExists();
            return table;
        }

        public static bool createTableCheck(CloudTableClient pClient, string pName)
        {
            CloudTable table = pClient.GetTableReference(pName);
            bool ret = table.CreateIfNotExists();
            return ret;
        }


        public static void deleteTable(CloudTableClient pClient, string pName)
        {
            CloudTable table = pClient.GetTableReference(pName);
            table.Delete();
        }

        public static Task<TableResult> insertEntity(CloudTableClient pClient, string pTableName,
                                        TableEntity pEntity)
        {
            CloudTable table = pClient.GetTableReference(pTableName);
            if (table == null)
            {
                //TODO: throw exception?
                return null;
            }
            var retValue = table.ExecuteAsync(TableOperation.Insert(pEntity));
            return retValue;
        }

        public static Task<TableResult> updateEntity(CloudTableClient pClient, string pTableName,
                                       TableEntity pEntity)
        {
            CloudTable table = pClient.GetTableReference(pTableName);
            if (table == null)
            {
                //TODO: throw exception?
                return null;
            }
            var retValue = table.ExecuteAsync(TableOperation.InsertOrReplace(pEntity));
            return retValue;
        }

        public static Task<IList<TableResult>> insertEntities(CloudTableClient pClient, string pTableName,
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

            return table.ExecuteBatchAsync(batch);
        }


        public static IEnumerable<TableEntity> findEntitiesInPartition(CloudTableClient pClient, string pName, string pPartitionKey)
        {
            CloudTable table = pClient.GetTableReference(pName);
            TableQuery<TableEntity> rangeQuery = new TableQuery<TableEntity>().Where(
                        TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, pPartitionKey));
            return table.ExecuteQuery(rangeQuery);
        }

        public static Task<TableResult> findEntity(CloudTableClient pClient, string pName, string pPartitionKey, string pRowKey)
        {
            CloudTable table = pClient.GetTableReference(pName);
            TableOperation op = TableOperation.Retrieve<TableEntity>(pPartitionKey, pRowKey);
            return table.ExecuteAsync(op);
        }


        public static Task<TableResult> deleteEntity(CloudTableClient pClient, string pName, string pPartitionKey, string pRowKey)
        {
            CloudTable table = pClient.GetTableReference(pName);
            TableOperation op = TableOperation.Retrieve<TableEntity>(pPartitionKey, pRowKey);
            TableResult result =  table.Execute(op);
            if (result.HttpStatusCode == 404)
            {
                return null;
            }
            else
            {
                ByteEntity entityToDelete = (ByteEntity)result.Result;
                if (entityToDelete != null)
                {
                    return table.ExecuteAsync(TableOperation.Delete(entityToDelete));
                }
                else
                {
                    return null;
                }
            }
        }

        public static IEnumerable<T> findEntitiesProjection<T>(CloudTableClient pClient, string pName, string[] properties, EntityResolver<T> pEntityResolver)
        {
            CloudTable table = pClient.GetTableReference(pName);
            TableQuery<DynamicTableEntity> projectionQuery = new TableQuery<DynamicTableEntity>().Select(properties);
            return table.ExecuteQuery(projectionQuery, pEntityResolver);
        }

        public enum OperationType
        {
            CREATE,
            READ,
            READ_BATCH,
            READ_RANGE,
            INSERT,
            INSERT_BATCH,
            UPDATE,
            UPDATE_BATCH,
            DELETE
        }


    }
}
