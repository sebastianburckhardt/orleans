using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Orleans.Transactions
{
    public class MemoryTransactionLogStorage : ITransactionLogStorage
    {
        private static readonly Task<CommitRecord> NullCommitRecordTask = Task.FromResult<CommitRecord>(null);

        private long startRecordValue;

        private readonly List<CommitRecord> log;

        private int lastLogRecordIndex;

        private long nextLogSequenceNumber;

        public MemoryTransactionLogStorage()
        {
            log = new List<CommitRecord>();

            startRecordValue = 0;
            lastLogRecordIndex = 0;
        }

        public Task Initialize()
        {
            return TaskDone.Done;
        }

        public Task<CommitRecord> GetFirstCommitRecord()
        {
            if (log.Count == 0)
            {
                //
                // Initialize LSN here, to be semantically correct with other providers.
                //

                nextLogSequenceNumber = 1;

                return NullCommitRecordTask;
            }

            //
            // If the log has records, then this method should not get called for the in memory provider.
            //

            throw new InvalidOperationException($"GetFirstCommitRecord was called while the log already has {log.Count} records.");
        }

        public Task<CommitRecord> GetNextCommitRecord()
        {
            if (log.Count <= lastLogRecordIndex)
            {
                return NullCommitRecordTask;
            }

            nextLogSequenceNumber++;

            return Task.FromResult(log[lastLogRecordIndex++]);
        }

        public Task<long> GetStartRecord()
        {
            startRecordValue = 50000;

            return Task.FromResult(startRecordValue);
        }

        public Task UpdateStartRecord(long transactionId)
        {
            startRecordValue = transactionId;

            return TaskDone.Done;
        }

        public Task Append(IEnumerable<CommitRecord> commitRecords)
        {
            lock (this)
            {
                foreach (var commitRecord in commitRecords)
                {
                    commitRecord.LSN = nextLogSequenceNumber++;
                }

                log.AddRange(commitRecords);
            }

            return TaskDone.Done;
        }

        public Task TruncateLog(long lsn)
        {
            lock (this)
            {
                var itemsToRemove = 0;

                for (itemsToRemove = 0; itemsToRemove < log.Count; itemsToRemove++)
                {
                    if (log[itemsToRemove].LSN > lsn)
                    {
                        break;
                    }
                }

                log.RemoveRange(0, itemsToRemove);
            }

            return TaskDone.Done;
        }
    }
}
