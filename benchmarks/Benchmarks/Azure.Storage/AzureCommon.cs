using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure;
using Microsoft.Azure;

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
        public static CloudStorageAccount getStorageAccount()
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


   

        public static CloudTableClient getTableClient(string pConnectionKey)
        {
            string connectionKey = CloudConfigurationManager.GetSetting(pConnectionKey);
            if (connectionKey == null)
            {
                connectionKey = "UseDevelopmentStorage=true";
            }
            else
            {
                Console.Write("Connection Key {0} \n ", connectionKey);
            }
            CloudStorageAccount account = CloudStorageAccount.Parse(connectionKey);
            return account.CreateCloudTableClient();
        }


        public static CloudTableClient getTableClient()
        {
            string connectionKey = CloudConfigurationManager.GetSetting("StorageConnectionString");
            if (connectionKey == null)
            {
                if (!Common.Util.RunningInAzureSimulator())
                    throw new Exception("No connection key specified");
                else connectionKey = "UseDevStorage=true";
            }
            else
            {
                Console.Write("Connection Key {0} \n ", connectionKey);
            }
            CloudStorageAccount account = CloudStorageAccount.Parse(connectionKey);
            return account.CreateCloudTableClient();
        }

        public static CloudTable createTable(CloudTableClient pClient, string pName)
        {
            IEnumerable<CloudTable> tables = pClient.ListTables();

            
            foreach (CloudTable t in tables) {
                
                Console.WriteLine(t);
            }
            CloudTable table = pClient.GetTableReference(pName);
            if (!table.CreateIfNotExists())
            {
           //     throw new Exception("Table already existed");
            }
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

        public static Task<TableResult> insertEntity<T>(CloudTableClient pClient, string pTableName,
                                        TableEntity pEntity) where T : TableEntity, new()
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

        public static Task<TableResult> updateEntity<T>(CloudTableClient pClient, string pTableName,
                                       TableEntity pEntity) where T : TableEntity, new()
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

        public static Task<IList<TableResult>> insertEntities<T>(CloudTableClient pClient, string pTableName,
                                                IList<T> pEntityList) where T : TableEntity, new()
        {
            CloudTable table = pClient.GetTableReference(pTableName);
            if (table == null)
            {
                //TODO: throw exception?
                return null;
            }
            TableBatchOperation batch = new TableBatchOperation();
            foreach (T ent in pEntityList)
            {
                batch.Insert(ent);
            }

            return table.ExecuteBatchAsync(batch);
        }


        public static IEnumerable<DynamicTableEntity> findEntitiesInPartition(CloudTableClient pClient, string pName, string pPartitionKey)
        {
            CloudTable table = pClient.GetTableReference(pName);
            TableQuery<DynamicTableEntity> rangeQuery = new TableQuery<DynamicTableEntity>().Where(
                        TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, pPartitionKey));
            return table.ExecuteQuery(rangeQuery);
        }

        public static Task<TableResult> findEntity<T>(CloudTableClient pClient, string pName, string pPartitionKey, string pRowKey) where T : TableEntity, new()
        {
            CloudTable table = pClient.GetTableReference(pName);
            TableOperation op = TableOperation.Retrieve<T>(pPartitionKey, pRowKey);
            return table.ExecuteAsync(op);
        }

        public static TableResult findEntitySync<T>(CloudTableClient pClient, string pName, string pPartitionKey, string pRowKey) where T : TableEntity, new()
        {
            try
            {
                CloudTable table = pClient.GetTableReference(pName);
                TableOperation op = TableOperation.Retrieve<T>(pPartitionKey, pRowKey);
                return table.Execute(op);
            }
            catch (Exception e)
            {
                Console.WriteLine("Execption {0} \n ", e.ToString());
            }
            return null;
        }



        public static Task<TableResult> deleteEntity<T>(CloudTableClient pClient, string pName, string pPartitionKey, string pRowKey) where T : TableEntity, new()
        {
            CloudTable table = pClient.GetTableReference(pName);
            TableOperation op = TableOperation.Retrieve<T>(pPartitionKey, pRowKey);
            TableResult result = table.Execute(op);
            if (result.HttpStatusCode == 404)
            {
                return null;
            }
            else
            {
                T entityToDelete = (T)result.Result;
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

        public static IEnumerable<T> findEntitiesProjection<T>(CloudTableClient pClient, string pName, string[] properties, EntityResolver<T> pEntityResolver) where T : TableEntity, new()
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


        public static string generateKey(int pByteLength)
        {
            Random rnd = new Random();
            byte[] bytes = new byte[pByteLength];
            rnd.NextBytes(bytes);
            return ToAzureKeyString(Encoding.ASCII.GetString(bytes));
        }

        public static string ToAzureKeyString(string str)
        {
            var sb = new StringBuilder();
            foreach (var c in str.Where(c => c != '/'
                            && c != '\\'
                            && c != '#'
                            && c != '/'
                            && c != '?'
                            && !char.IsControl(c)))
                sb.Append(c);
            return sb.ToString();
        }
    }
}
