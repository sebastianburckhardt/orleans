
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;
using Orleans.Serialization;
using Microsoft.WindowsAzure.Storage;

namespace Orleans.Transactions
{
    internal class AzureTransactionLog : TransactionLog
    {
        private const int CommitRecordsPerRow = 10;
        private const long CommitPartitionKey = 0;
        private const long StartPartitionKey = 1;

        //TODO: jbragg - Do not use serializationManager for persistent data!!
        private readonly SerializationManager serializationManager;
        private LogMode mode;
        private readonly bool clearOnInitialize;
        private long logSequenceNumber = 1;
        private long startedTransactionsCount;

        // Azure Tables objects for persistent storage
        private readonly string tableName;
        private readonly CloudTableClient azTableClient;

        // Log iteration indexes
        private TableQuerySegment<CommitRow> currentLogQuerySegment;
        private int rowInCurrentSegment;
        private List<CommitRecord> currentRowTransactions;
        private int recordInCurrentRow;
        private TableContinuationToken continuationToken;


        public AzureTransactionLog(SerializationManager serializationManager, string connectionString, string tableName, bool clear = false)
        {
            this.serializationManager = serializationManager;
            // Retrieve the storage account from the connection string.
            var storageAccount = CloudStorageAccount.Parse(connectionString);

            // Create the table client.
            azTableClient = storageAccount.CreateCloudTableClient();
            this.tableName = tableName;

            mode = LogMode.Uninitialized;
            clearOnInitialize = clear;
        }

        public override async Task Initialize()
        {
            CloudTable table = azTableClient.GetTableReference(tableName);

            if (clearOnInitialize)
            {
                await table.DeleteIfExistsAsync().ConfigureAwait(false);

                await Task.Delay(1000);
            }

            await table.CreateIfNotExistsAsync().ConfigureAwait(false);

            TableQuery<StartRow> query =
                new TableQuery<StartRow>().Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, "0"));
            var continuation = new TableContinuationToken();
            var result = await table.ExecuteQuerySegmentedAsync(query, continuation).ConfigureAwait(false);

            if (result.Results.Count == 0)
            {
                // This is a fresh deployment, the StartRecord isn't created yet.
                // Create it here.
                var row = new StartRow(0);
                var operation = TableOperation.Insert(row);

                await table.ExecuteAsync(operation).ConfigureAwait(false);
            }
            else
            {
                startedTransactionsCount = result.Results[0].TransactionCount;
            }

            mode = LogMode.RecoveryMode;
        }

        public override async Task<CommitRecord> GetFirstCommitRecord()
        {
            ThrowIfNotInMode(LogMode.RecoveryMode);

            continuationToken = null;

            await GetLogsFromTable(ToRowKey(0));

            if (currentLogQuerySegment.Results.Count == 0)
            {
                // The log has no log entries
                currentLogQuerySegment = null;
                return null;
            }

            currentRowTransactions = DeserializeCommitRecords(currentLogQuerySegment.Results[0].Transactions);

            // TODO: Assert not empty?

            logSequenceNumber = currentRowTransactions[recordInCurrentRow].LSN + 1;
            return currentRowTransactions[recordInCurrentRow++];
        }

        public override async Task<CommitRecord> GetNextCommitRecord()
        {
            ThrowIfNotInMode(LogMode.RecoveryMode);

            if (currentLogQuerySegment == null)
            {
                return null;
            }

            if (recordInCurrentRow == currentRowTransactions.Count)
            {
                rowInCurrentSegment++;
                recordInCurrentRow = 0;
                currentRowTransactions = null;
            }

            if (rowInCurrentSegment == currentLogQuerySegment.Results.Count)
            {
                // No more rows in our current segment, retrieve the next segment from the Table.
                if (continuationToken == null)
                {
                    currentLogQuerySegment = null;
                    return null;
                }

                await GetLogsFromTable(ToRowKey(0));
            }

            if (currentRowTransactions == null)
            {
                // TODO: assert recordInCurrentRow = 0?
                currentRowTransactions = DeserializeCommitRecords(currentLogQuerySegment.Results[rowInCurrentSegment].Transactions);
            }

            logSequenceNumber++;
            return currentRowTransactions[recordInCurrentRow++];
        }

        public override Task EndRecovery()
        {
            ThrowIfNotInMode(LogMode.RecoveryMode);
            mode = LogMode.AppendMode;

            return TaskDone.Done;
        }

        public override Task<long> GetStartRecord()
        {
            ThrowIfNotInMode(LogMode.AppendMode);

            return Task.FromResult(startedTransactionsCount);
        }

        public override async Task UpdateStartRecord(long transactionCount)
        {
            ThrowIfNotInMode(LogMode.AppendMode);

            if (transactionCount > startedTransactionsCount)
            {
                CloudTable table = azTableClient.GetTableReference(tableName);
                var op = TableOperation.Replace(new StartRow(transactionCount));
                await table.ExecuteAsync(op).ConfigureAwait(false);
                startedTransactionsCount = transactionCount;
            }

        }

        public override async Task Append(List<CommitRecord> transactions)
        {
            ThrowIfNotInMode(LogMode.AppendMode);

            CloudTable table = azTableClient.GetTableReference(tableName);
            var batchOperation = new TableBatchOperation();

            for (int nextRecord = 0; nextRecord < transactions.Count; nextRecord += CommitRecordsPerRow)
            {
                var recordCount = Math.Min(transactions.Count - nextRecord, CommitRecordsPerRow);
                var rowTransactions = transactions.GetRange(nextRecord, recordCount);
                var row = new CommitRow(logSequenceNumber);
                foreach (var rec in rowTransactions)
                {
                    rec.LSN = logSequenceNumber++;
                }

                row.Transactions = SerializeCommitRecords(rowTransactions);
                batchOperation.Insert(row);

                if (batchOperation.Count == 100)
                {
                    await table.ExecuteBatchAsync(batchOperation).ConfigureAwait(false);
                    batchOperation = new TableBatchOperation();
                }
            }

            if (batchOperation.Count > 0)
            {
                await table.ExecuteBatchAsync(batchOperation).ConfigureAwait(false);
            }
        }


        public override async Task TruncateLog(long LSN)
        {
            ThrowIfNotInMode(LogMode.AppendMode);

            CloudTable table = azTableClient.GetTableReference(tableName);
            TableContinuationToken token;
            do
            {
                TableQuery<CommitRow> query =
                    new TableQuery<CommitRow>().Where(TableQuery.CombineFilters(
                        TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, CommitPartitionKey.ToString()),
                        TableOperators.And,
                        TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual, ToRowKey(LSN))));
                var logSegment = await table.ExecuteQuerySegmentedAsync(query, null).ConfigureAwait(false);
                token = logSegment.ContinuationToken;

                if (logSegment.Results.Count > 0)
                {
                    var batchOperation = new TableBatchOperation();
                    foreach (var row in logSegment)
                    {
                        List<CommitRecord> transactions = DeserializeCommitRecords(row.Transactions);
                        if (transactions.Count > 0 && transactions[transactions.Count-1].LSN <= LSN)
                        {
                            batchOperation.Delete(row);
                            if (batchOperation.Count == 100)
                            {
                                // Azure has a limit of 100 operations per batch
                                await table.ExecuteBatchAsync(batchOperation).ConfigureAwait(false);
                                batchOperation = new TableBatchOperation();
                            }
                        }
                        else
                        {
                            break;
                        }
                    }

                    if (batchOperation.Count > 0)
                    {
                        await table.ExecuteBatchAsync(batchOperation).ConfigureAwait(false);
                    }
                }

            } while (token != null);
        }

        private async Task GetLogsFromTable(string keyLowerBound)
        {
            CloudTable table = azTableClient.GetTableReference(tableName);

            TableQuery<CommitRow> query =
                new TableQuery<CommitRow>().Where(TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, CommitPartitionKey.ToString()),
                    TableOperators.And,
                    TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, keyLowerBound)));
            currentLogQuerySegment = await table.ExecuteQuerySegmentedAsync(query, continuationToken).ConfigureAwait(false);

            // reset the indexes
            rowInCurrentSegment = 0;
            recordInCurrentRow = 0;
            continuationToken = currentLogQuerySegment.ContinuationToken;
        }

        private void ThrowIfNotInMode(LogMode logMode)
        {
            if (this.mode != logMode)
                throw new InvalidOperationException($"Log has to be in {logMode} mode");
        }

        private class CommitRow : TableEntity
        {
            public CommitRow(long firstLSN)
            {
                this.PartitionKey = CommitPartitionKey.ToString(); // all entities are in the same partition for atomic read/writes
                this.RowKey = ToRowKey(firstLSN);
            }

            public CommitRow()
            {
            }

            public string Transactions { get; set; }
        }

        private class StartRow : TableEntity
        {
            public StartRow(long transactionCount)
            {
                // only row in the table with this partition key
                this.PartitionKey = StartPartitionKey.ToString();
                this.RowKey = "0";
                base.ETag = "*";
                TransactionCount = transactionCount;
            }

            public StartRow()
            {
            }

            public long TransactionCount { get; }
        }

        private string SerializeCommitRecords(List<CommitRecord> records)
        {
            var serializableList = new List<Tuple<long, long, HashSet<ITransactionalResource>>>(records.Count);

            foreach (var r in records)
            {
                serializableList.Add(new Tuple<long, long, HashSet<ITransactionalResource>>(r.LSN, r.TransactionId, r.Resources));
            }

            var sw = new BinaryTokenStreamWriter();
            this.serializationManager.Serialize(serializableList, sw);

            return Convert.ToBase64String(sw.ToByteArray());
        }

        private List<CommitRecord> DeserializeCommitRecords(string base64)
        {
            var bytes = Convert.FromBase64String(base64);
            var sr = new BinaryTokenStreamReader(bytes);
            var l = this.serializationManager.Deserialize<List<Tuple<long, long, HashSet<ITransactionalResource>>>>(sr);
            var list = new List<CommitRecord>(l.Count);

            foreach (var r in l)
            {
                list.Add(new CommitRecord() { LSN = r.Item1, TransactionId = r.Item2, Resources = r.Item3 });
            }

            return list;
        }

        private static string ToRowKey(long lsn)
        {
            // Azure Table's keys are strings which complicate integer comparison
            string lsnStr = lsn.ToString();
            char prefix = (char)('a' + lsnStr.Length);
            return prefix + lsnStr;
        }

    }
}
