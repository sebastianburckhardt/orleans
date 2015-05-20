using System;
using System.Collections.Generic;
using System.Data.Services.Client;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.StorageClient;
using Orleans.Counters;


namespace Orleans.AzureUtils
{
    internal class Async_AzureTableDataManager<T> where T : TableServiceEntity, new()
    {
        public string TableName { get; private set; }
        protected string ConnectionString { get; set; }
        internal readonly Logger logger;
        
        private readonly CloudTableClient tableOperationsClient;

        protected const string ANY_ETAG = null; // GK: Any Tag value is NULL and not "*" in WCF APIs (it is "*" in REST APIs);
        // See http://msdn.microsoft.com/en-us/library/windowsazure/dd894038.aspx
        
        private readonly CounterStatistic numServerBusy = CounterStatistic.FindOrCreate(StatNames.STAT_AZURE_SERVER_BUSY, true);

        internal Async_AzureTableDataManager(string tableName, string storageConnectionString)
        {
            this.logger = Logger.GetLogger(this.GetType().Name, Logger.LoggerType.Runtime);
            this.TableName = tableName;
            this.ConnectionString = storageConnectionString;
            this.tableOperationsClient = GetCloudTableOperationsClient();            
        }

        private Type ResolveEntityType(string name) 
        { 
            return typeof(T); 
        }

        internal async Task InitTable_Async()
        {
            const string operation = "InitTable_Async";
            DateTime startTime = DateTime.UtcNow;
     
            try
            {
                CloudTableClient tableCreationClient = GetCloudTableCreationClient();

                bool didCreate = await Task<bool>.Factory.FromAsync(
                     tableCreationClient.BeginCreateTableIfNotExist, 
                     tableCreationClient.EndCreateTableIfNotExist, 
                     TableName, 
                     null);

                logger.Info(ErrorCode.AzureTable_01, "{0} Azure storage table {1}", (didCreate ? "Created" : "Attached to"), TableName);

                await InitializeTableSchemaFromEntity(tableCreationClient, TableName, new T());

                logger.Info(ErrorCode.AzureTable_36, "Initialized schema for Azure storage table {0}", TableName);
            }
            catch (Exception exc)
            {
                logger.Error(ErrorCode.AzureTable_02, String.Format("Could not initialize connection to storage table {0}", TableName), exc);
                throw;
            }
            finally
            {
                CheckAlertSlowAccess(startTime, operation);
            }
        }

        internal async Task DeleteTable()
        {
            const string operation = "DeleteTable";
            DateTime startTime = DateTime.UtcNow;

            try
            {
                CloudTableClient tableCreationClient = GetCloudTableCreationClient();

                bool didDelete = await Task<bool>.Factory.FromAsync(
                        tableCreationClient.BeginDeleteTableIfExist, 
                        tableCreationClient.EndDeleteTableIfExist, 
                        TableName, 
                        null);

                if (didDelete)
                {
                    logger.Info(ErrorCode.AzureTable_03, "Deleted Azure storage table {0}", TableName);
                }
            }
            catch (Exception exc)
            {
                logger.Error(ErrorCode.AzureTable_04, "Could not delete storage table {0}", exc);
                throw;
            }
            finally
            {
                CheckAlertSlowAccess(startTime, operation);
            }
        }

        internal async Task<string> CreateTableEntry(T data)
        {
            const string operation = "CreateTableEntry";
            DateTime startTime = DateTime.UtcNow;
           
            if (logger.IsVerbose2) logger.Verbose2("Creating {0} table entry: {1}", TableName, data);

            try
            {
                TableServiceContext svc = tableOperationsClient.GetDataServiceContext();
                svc.AddObject(TableName, data);

                try
                {
                    DataServiceResponse response = await Task<DataServiceResponse>.Factory.FromAsync(
                            svc.BeginSaveChangesWithRetries, 
                            svc.EndSaveChangesWithRetries, 
                            SaveChangesOptions.None, 
                            null);

                    return svc.GetEntityDescriptor(data).ETag;
                }
                catch (Exception exc)
                {
                    logger.Warn(ErrorCode.AzureTable_05, String.Format("Intermediate error creating entry {0} in the table {1}",
                                (data == null ? "null" : data.ToString()), TableName), exc);
                    throw;
                }
            }
            finally
            {
                CheckAlertSlowAccess(startTime, operation);
            }
        }

        //// Upsert (Insert Or Replace) Entity operation replaces an existing entity or inserts a new entity if it does not exist in the table.
        //// Would NOT throw EntityAlreadyExists (Conflict) or NotFound.
        //// In order to InsertOrMergeTableEntry: svc.AttachTo(TableName, data); svc.UpdateObject(TableName, data); svc.SaveChangesWithRetries(SaveChangesOptions.None);
        //// http://msdn.microsoft.com/en-us/library/hh452242.aspx
        //// http://stackoverflow.com/questions/4466764/add-or-replace-entity-in-azure-table-storage
        internal async Task<string> UpsertTableEntry(T data)
        {
            const string operation = "UpsertTableEntry";
            DateTime startTime = DateTime.UtcNow;
            if (logger.IsVerbose2) logger.Verbose2("{0} entry {1} into table {2}", operation, data, TableName);

            try
            {
                TableServiceContext svc = tableOperationsClient.GetDataServiceContext();
                try
                {
                    Task<DataServiceResponse> savePromise;

                    Func<int, Task<DataServiceResponse>> DoSaveChanges = retryNum =>
                    {
                        if (retryNum > 0) svc.Detach(data);

                        // Try to do update first
                        svc.AttachTo(TableName, data, ANY_ETAG);
                        svc.UpdateObject(data);

                        Task<DataServiceResponse> saveChangesWithRetries = Task<DataServiceResponse>.Factory.FromAsync(
                                svc.BeginSaveChangesWithRetries,
                                svc.EndSaveChangesWithRetries,
                                SaveChangesOptions.ReplaceOnUpdate,
                                null);

                        return saveChangesWithRetries;
                    };


                    if (AzureTableDefaultPolicies.MaxBusyRetries > 0)
                    {
                        IBackoffProvider backoff = new FixedBackoff(AzureTableDefaultPolicies.PauseBetweenBusyRetries);
                        savePromise = AsyncExecutorWithRetries.ExecuteWithRetries(
                            retryNum => DoSaveChanges(retryNum),
                            AzureTableDefaultPolicies.MaxBusyRetries,
                            (exc, retryNum) => IsServerBusy(exc),
                            AzureTableDefaultPolicies.BusyRetries_TIMEOUT,
                            backoff);
                    }
                    else
                    {
                        savePromise = DoSaveChanges(0);
                    }
                    await savePromise;
                    return svc.GetEntityDescriptor(data).ETag;
                }
                catch (Exception exc)
                {
                    logger.Warn(ErrorCode.AzureTable_06, String.Format("Intermediate error upserting entry {0} to the table {1}",
                        (data == null ? "null" : data.ToString()), TableName), exc);
                    throw;
                }
            }
            finally
            {
                CheckAlertSlowAccess(startTime, operation);
            }
        }

        private bool IsServerBusy(Exception exc)
        {
            string strCode = AzureStorageUtils.ExtractRestErrorCode(exc);
            bool serverBusy = StorageErrorCodeStrings.ServerBusy.Equals(strCode);
            if (serverBusy) numServerBusy.Increment();
            return serverBusy;
        }

        internal async Task BulkInsertTableEntries(IReadOnlyCollection<T> data)
        {
            const string operation = "BulkInsertTableEntries";
            if (data == null) throw new ArgumentNullException("data");
            if (data.Count > AzureTableDefaultPolicies.MAX_BULK_UPDATE_ROWS)
            {
                throw new ArgumentOutOfRangeException("data", data.Count, 
                        "Too many rows for bulk update - max " + AzureTableDefaultPolicies.MAX_BULK_UPDATE_ROWS);
            }

            DateTime startTime = DateTime.UtcNow;
            if (logger.IsVerbose2) logger.Verbose2("Bulk inserting {0} entries to {1} table", data.Count, TableName);

            try
            {
                TableServiceContext svc = tableOperationsClient.GetDataServiceContext();
                foreach (T entry in data)
                {
                    svc.AttachTo(TableName, entry);
                    svc.UpdateObject(entry);
                }

                bool fallbackToInsertOneByOne = false;
                try
                {
                    // SaveChangesOptions.None == Insert-or-merge operation, SaveChangesOptions.Batch == Batch transaction
                    // http://msdn.microsoft.com/en-us/library/hh452241.aspx
                    DataServiceResponse response = await Task<DataServiceResponse>.Factory.FromAsync(
                        svc.BeginSaveChangesWithRetries, 
                        svc.EndSaveChangesWithRetries, 
                        SaveChangesOptions.None | SaveChangesOptions.Batch, 
                        null);
                    
                    return; // Done
                }
                catch (Exception exc)
                {            
                    logger.Warn(ErrorCode.AzureTable_37, String.Format("Intermediate error bulk inserting {0} entries in the table {1}",
                        data.Count, TableName), exc);

                    DataServiceRequestException dsre = exc.GetBaseException() as DataServiceRequestException;
                    if (dsre != null)
                    {
                        DataServiceClientException dsce = dsre.GetBaseException() as DataServiceClientException;
                        if (dsce != null)
                        {
                            // Fallback to insert rows one by one
                            fallbackToInsertOneByOne = true;
                        }
                    }

                    if (!fallbackToInsertOneByOne) throw;
                }

                // Bulk insert failed, so try to insert rows one by one instead
                List<Task> promises = new List<Task>();
                foreach (T entry in data)
                {
                    promises.Add(UpsertTableEntry(entry));
                }
                await Task.WhenAll(promises);
            }
            finally
            {
                CheckAlertSlowAccess(startTime, operation);
            }
        }

        internal async Task<string> MergeTableEntry(T data)
        {
            const string operation = "MergeTableEntry";
            DateTime startTime = DateTime.UtcNow;
            if (logger.IsVerbose2) logger.Verbose2("{0} entry {1} into table {2}", operation, data, TableName);

            try
            {
                TableServiceContext svc = tableOperationsClient.GetDataServiceContext();
                svc.AttachTo(TableName, data, ANY_ETAG);
                svc.UpdateObject(data);

                try
                {
                    DataServiceResponse response = await Task<DataServiceResponse>.Factory.FromAsync(
                        svc.BeginSaveChangesWithRetries, 
                        svc.EndSaveChangesWithRetries, 
                        SaveChangesOptions.None, 
                        null);

                    return svc.GetEntityDescriptor(data).ETag;
                }
                catch (Exception exc)
                {
                    logger.Warn(ErrorCode.AzureTable_07, String.Format("Intermediate error merging entry {0} to the table {1}",
                        (data == null ? "null" : data.ToString()), TableName), exc);
                    throw;
                }
            }
            finally
            {
                CheckAlertSlowAccess(startTime, operation);
            }
        }

        // Insert a new data entity if it does not exist in the table. May throw EntityAlreadyExists (Conflict) if already exists.
        // In addition, conditionally update table version.
        // http://msdn.microsoft.com/en-us/library/dd179433.aspx
        internal async Task<string> InsertTableEntryConditionally(T data, T tableVersion, string tableVersionEtag, bool updateTableVersion = true)
        {
            const string operation = "InsertTableEntryConditionally";
            string tableVersionData = (tableVersion == null ? "null" : tableVersion.ToString());
            DateTime startTime = DateTime.UtcNow;
            
            if (logger.IsVerbose2) logger.Verbose2("{0} into table {1} version {2} entry {3}", operation, TableName, tableVersionData, data);

            try
            {
                TableServiceContext svc = tableOperationsClient.GetDataServiceContext();
                // Only AddObject, do NOT AttachTo. If we did both UpdateObject and AttachTo, it would have been equivalent to InsertOrReplace.
                svc.AddObject(TableName, data);
                if (updateTableVersion)
                {
                    svc.AttachTo(TableName, tableVersion, tableVersionEtag);
                    svc.UpdateObject(tableVersion);
                }
                try
                {
                    DataServiceResponse response = await Task<DataServiceResponse>.Factory.FromAsync(
                        svc.BeginSaveChangesWithRetries, 
                        svc.EndSaveChangesWithRetries, 
                        SaveChangesOptions.ReplaceOnUpdate | SaveChangesOptions.Batch, 
                        null);

                    return svc.GetEntityDescriptor(data).ETag;
                }
                catch (Exception exc)
                {
                    CheckAlertWriteError(operation, data, tableVersionData, exc);
                    throw;
                }
            }
            finally
            {
                CheckAlertSlowAccess(startTime, operation);
            }
        }

        // Updates an existing entity in a table. The Update Entity operation replaces the entire entity.  May throw NotFound.
        // In addition, conditionally update table version.
        // http://msdn.microsoft.com/en-us/library/dd179427.aspx
        internal async Task<string> UpdateTableEntryConditionally(T data, string dataEtag, T tableVersion, string tableVersionEtag)
        {
            const string operation = "UpdateTableEntryConditionally";
            string tableVersionData = (tableVersion == null ? "null" : tableVersion.ToString());
            DateTime startTime = DateTime.UtcNow;
            if (logger.IsVerbose2) logger.Verbose2("{0} table {1} version {2} entry {3}", operation, TableName, tableVersionData, data);

            try
            {
                TableServiceContext svc = tableOperationsClient.GetDataServiceContext();
                svc.AttachTo(TableName, data, dataEtag);
                svc.UpdateObject(data);
                if (tableVersion != null && tableVersionEtag != null)
                {
                    svc.AttachTo(TableName, tableVersion, tableVersionEtag);
                    svc.UpdateObject(tableVersion);
                }

                try
                {
                    DataServiceResponse response = await Task<DataServiceResponse>.Factory.FromAsync(
                        svc.BeginSaveChangesWithRetries, 
                        svc.EndSaveChangesWithRetries, 
                        SaveChangesOptions.ReplaceOnUpdate | SaveChangesOptions.Batch, 
                        null);

                    return svc.GetEntityDescriptor(data).ETag;
                }
                catch (Exception exc)
                {
                    CheckAlertWriteError(operation, data, tableVersionData, exc);
                    throw;
                }
            }
            finally
            {
                CheckAlertSlowAccess(startTime, operation);
            }
        }

        internal Task DeleteTableEntry(T data, string eTag)
        {
            List<Tuple<T,string>> list = new List<Tuple<T,string>>();
            list.Add(new Tuple<T,string>(data, eTag));
            return DeleteTableEntries(list);
        }

        internal async Task DeleteTableEntries(IReadOnlyCollection<Tuple<T, string>> list)
        {
            const string operation = "DeleteTableEntries";
            DateTime startTime = DateTime.UtcNow;
            if (logger.IsVerbose2) logger.Verbose2("Deleting {0} table entries: {1}", TableName, Utils.IEnumerableToString(list));

            try
            {
                TableServiceContext svc = tableOperationsClient.GetDataServiceContext();
                foreach (var tuple in list)
                {
                    svc.AttachTo(TableName, tuple.Item1, tuple.Item2);
                    svc.DeleteObject(tuple.Item1);
                }
                try
                {
                    DataServiceResponse response = await Task<DataServiceResponse>.Factory.FromAsync(
                        svc.BeginSaveChangesWithRetries, 
                        svc.EndSaveChangesWithRetries,
                        SaveChangesOptions.ReplaceOnUpdate | SaveChangesOptions.Batch, 
                        null);
                }
                catch (Exception exc)
                {
                    logger.Warn(ErrorCode.AzureTable_08,
                        String.Format("Intermediate error deleting entries {0} from the table {1}.",
                            Utils.IEnumerableToString(list), TableName), exc);
                    throw;
                }
            }
            finally
            {
                CheckAlertSlowAccess(startTime, operation);
            }
        }

        /// <summary>
        /// Read data entries and corresponding etag values for table rows matching the specified predicate
        /// Used http://convective.wordpress.com/2010/02/06/queries-in-azure-tables/
        /// to get the API details.
        /// </summary>
        /// <param name="predicate"></param>
        /// <returns></returns>
        internal async Task<IEnumerable<Tuple<T, string>>> ReadTableEntriesAndEtags(Expression<Func<T, bool>> predicate)
        {
            const string operation = "ReadTableEntriesAndEtags";
            DateTime startTime = DateTime.UtcNow;

            try
            {
                TableServiceContext svc = tableOperationsClient.GetDataServiceContext();
                // Improve performance when table name differs from class name
                // http://www.gtrifonov.com/2011/06/15/improving-performance-for-windows-azure-tables/
                svc.ResolveType = ResolveEntityType;
            
                //IQueryable<T> query = svc.CreateQuery<T>(TableName).Where(predicate);
                CloudTableQuery<T> cloudTableQuery = svc.CreateQuery<T>(TableName).Where(predicate).AsTableServiceQuery(); // turn IQueryable into CloudTableQuery

                try
                {
                    IBackoffProvider backoff = new FixedBackoff(AzureTableDefaultPolicies.PauseBetweenTableOperationRetries);
                    await AsyncExecutorWithRetries.ExecuteWithRetries((int counter) =>
                        {
                            // 1) First wrong sync way to read:
                            // List<T> queryResults = query.ToList(); // ToList will actually execute the query and add entities to svc. However, this will not handle continuation tokens.
                            // 2) Second correct sync way to read:
                            // http://convective.wordpress.com/2010/02/06/queries-in-azure-tables/
                            // CloudTableQuery.Execute will properly retrieve all the records from a table through the automatic handling of continuation tokens:
                            Task<ResultSegment<T>> firstSegmentPromise = Task<ResultSegment<T>>.Factory.FromAsync(
                                cloudTableQuery.BeginExecuteSegmented, 
                                cloudTableQuery.EndExecuteSegmented, 
                                null);
                            // 3) Third wrong async way to read:
                            // return firstSegmentPromise;
                            // 4) Forth correct async way to read - handles continuation tokens:
                            return GetAllResults_HandleContinuations(firstSegmentPromise);
                        },
                        AzureTableDefaultPolicies.MaxTableOperationRetries,
                        (Exception exc, int counter) =>
                        {
                            return AzureStorageUtils.AnalyzeReadException(exc.GetBaseException(), counter, TableName, logger);
                        },
                        AzureTableDefaultPolicies.TableOperation_TIMEOUT,
                        backoff
                    );

                    // Data was read successfully if we got to here
                    return svc.Entities.Select(entity => 
                        new Tuple<T, string>((T)entity.Entity, entity.ETag));
                }
                catch (Exception exc)
                {
                    // Out of retries...
                    var errorMsg = string.Format("Failed to read Azure storage table {0}: {1}", TableName, exc.Message);
                    if (!AzureStorageUtils.TableStorageDataNotFound(exc))
                    {
                        logger.Warn(ErrorCode.AzureTable_09, errorMsg, exc);
                    }
                    throw new DataServiceQueryException(errorMsg, exc);
                }
            }
            finally
            {
                CheckAlertSlowAccess(startTime, operation);
            }
        }

        private static async Task GetAllResults_HandleContinuations(Task<ResultSegment<T>> resultSegmentAsync)
        {
            while (true)
            {
                ResultSegment<T> resultSegment = await resultSegmentAsync;
                if (!resultSegment.HasMoreResults)
                {
                    // All data was read successfully if we got to here
                    return;
                }
                // ask to read the next segment
                Task<ResultSegment<T>> nextSegment = Task<ResultSegment<T>>.Factory.FromAsync(
                    resultSegment.BeginGetNext, 
                    resultSegment.EndGetNext, 
                    null);

                resultSegmentAsync = nextSegment;
            }
        }

        private void CheckAlertWriteError(string operation, object data, string tableVersionData, Exception exc)
        {
            HttpStatusCode httpStatusCode;
            string restStatus;
            if (AzureStorageUtils.EvaluateException(exc, out httpStatusCode, out restStatus) && AzureStorageUtils.IsContentionError(httpStatusCode))
            {
                // log at Verbose, since failure on conditional is not not an error. Will analyze and warn later, if required.
                if (logger.IsVerbose)
                {
                    logger.Verbose(ErrorCode.AzureTable_13,
                                   String.Format("Intermediate Azure table write error {0} to table {1} version {2} entry {3}",
                                       operation, TableName, (tableVersionData ?? "null"), (data ?? "null")), exc);
                }
            }
            else
            {
                logger.Error(ErrorCode.AzureTable_14,
                    string.Format("Azure table access write error {0} to table {1} entry {2}", operation, TableName, data), exc);
            }
        }

        private void CheckAlertSlowAccess(DateTime startOperation, string operation)
        {
            TimeSpan timeSpan = DateTime.UtcNow - startOperation;
            if (timeSpan > AzureTableDefaultPolicies.TableOperation_TIMEOUT)
            {
                logger.Warn(ErrorCode.AzureTable_15, "Slow access to Azure Table {0} for {1}, which took {2}.", TableName, operation, timeSpan);
            }
        }

        private CloudTableClient GetCloudTableOperationsClient()
        {
            try
            {
                CloudStorageAccount storageAccount = AzureStorageUtils.GetCloudStorageAccount(ConnectionString);
                CloudTableClient operationsClient = storageAccount.CreateCloudTableClient();
                operationsClient.RetryPolicy = AzureTableDefaultPolicies.TableOperationRetryPolicy;
                operationsClient.Timeout = AzureTableDefaultPolicies.TableOperation_TIMEOUT;
                return operationsClient;
            }
            catch (Exception exc)
            {
                logger.Error(ErrorCode.AzureTable_17, String.Format("Error creating CloudTableOperationsClient."), exc);
                throw;
            }
        }

        private CloudTableClient GetCloudTableCreationClient()
        {
            try
            {
                CloudStorageAccount storageAccount = AzureStorageUtils.GetCloudStorageAccount(ConnectionString);
                CloudTableClient client = storageAccount.CreateCloudTableClient();
                client.RetryPolicy = AzureTableDefaultPolicies.TableCreationRetryPolicy;
                client.Timeout = AzureTableDefaultPolicies.TableCreation_TIMEOUT;
                return client;
            }
            catch (Exception exc)
            {
                logger.Error(ErrorCode.AzureTable_18, String.Format("Error creating CloudTableCreationClient."), exc);
                throw;
            }
        }

        // Based on: http://blogs.msdn.com/b/cesardelatorre/archive/2011/03/12/typical-issue-one-of-the-request-inputs-is-not-valid-when-working-with-the-wa-development-storage.aspx
        private async Task InitializeTableSchemaFromEntity(
            CloudTableClient tableClient, string entityName,
            TableServiceEntity entity)
        {
            const string operation = "InitializeTableSchemaFromEntity";
            DateTime startTime = DateTime.UtcNow;

            entity.PartitionKey = Guid.NewGuid().ToString();
            entity.RowKey = Guid.NewGuid().ToString();
            Array.ForEach(
                entity.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance),
                p =>
                    {
                        if ((p.Name == "PartitionKey") || (p.Name == "RowKey") || (p.Name == "Timestamp")) return;

                        if (p.PropertyType == typeof(string))
                        {
                            p.SetValue(entity, Guid.NewGuid().ToString(),
                                       null);
                        }
                        else if (p.PropertyType == typeof(DateTime))
                        {
                            p.SetValue(entity, startTime, null);
                        }
                    });

            try
            {
                TableServiceContext svc = tableClient.GetDataServiceContext();
                svc.AddObject(entityName, entity);

                try
                {
                    DataServiceResponse response = await Task<DataServiceResponse>.Factory.FromAsync(
                        svc.BeginSaveChangesWithRetries, 
                        svc.EndSaveChangesWithRetries, 
                        SaveChangesOptions.None, 
                        null);
                }
                catch (Exception exc)
                {
                    CheckAlertWriteError(operation + "-Create", entity, null, exc);
                    throw;
                }

                try
                {
                    svc.DeleteObject(entity);
                    DataServiceResponse response = await Task<DataServiceResponse>.Factory.FromAsync(
                        svc.BeginSaveChangesWithRetries, 
                        svc.EndSaveChangesWithRetries, 
                        SaveChangesOptions.None, 
                        null);
                }
                catch (Exception exc)
                {
                    CheckAlertWriteError(operation + "-Delete", entity, null, exc);
                    throw;
                }
            }
            finally
            {
                CheckAlertSlowAccess(startTime, operation);
            }
        }
    }
}

//// Upsert (Insert Or Replace) Entity operation replaces an existing entity or inserts a new entity if it does not exist in the table.
//// Would NOT throw EntityAlreadyExists (Conflict) or NotFound.
//// In order to InsertOrMergeTableEntry: svc.AttachTo(TableName, data); svc.UpdateObject(TableName, data); svc.SaveChangesWithRetries(SaveChangesOptions.None);
//// http://msdn.microsoft.com/en-us/library/hh452242.aspx
//// http://stackoverflow.com/questions/4466764/add-or-replace-entity-in-azure-table-storage
//protected void UpsertTableEntry_2(T data)
//{
//    if (logger.IsVerbose2) logger.Verbose2("UpsertTableEntry entry {0} into table {1}", data, TableName);
//    try
//    {
//        var tableStore = GetCloudTableClient(false);
//        TableServiceContext svc = tableStore.GetDataServiceContext();

//        // The combination of AttachTo and AddObject with SaveChangesOptions.ReplaceOnUpdate tells it to operate on an existing object, if already exits,
//        // OR insert a new one if does not exist.
//        // Using ONLY AddObject would instruct tableContext to attempt to insert it, but not replace. In such a case, if already exists, it would throw Conflict.
//        svc.AttachTo(TableName, data);
//        svc.UpdateObject(data);
//        DataServiceResponse dataServiceResponse = svc.SaveChangesWithRetries(SaveChangesOptions.ReplaceOnUpdate);
//    }
//    catch (Exception exc)
//    {
//        logger.Warn(ErrorCode.AzureTable_05, String.Format("Intermediate error upserting entry {0} to the table {1}",
//            (data == null ? "null" : data.ToString()), TableName), exc);
//        throw;
//    }
//}
