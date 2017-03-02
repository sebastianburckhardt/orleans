using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Transactions
{
    internal class MemoryTransactionLog : TransactionLog
    {
        private LogMode mode;
        private long logSequenceNumber = 0;
        private long startedTransactionsCount = 0;

        private List<CommitRecord> log;
        private int nextLogRecordIndex = 0;

        public MemoryTransactionLog()
        {
            mode = LogMode.Uninitialized;
            log = new List<CommitRecord>();
        }

        public override void Initialize()
        {
            mode = LogMode.RecoveryMode;
        }

        public override Task<CommitRecord> GetFirstCommitRecord()
        {
            ThrowIfNotInMode(LogMode.RecoveryMode);

            nextLogRecordIndex = 0;
            if (log.Count == 0)
                return Task.FromResult<CommitRecord>(null);

            return Task.FromResult<CommitRecord>(log[nextLogRecordIndex++]);
        }

        public override Task<CommitRecord> GetNextCommitRecord()
        {
            ThrowIfNotInMode(LogMode.RecoveryMode);

            if (log.Count <= nextLogRecordIndex)
                return Task.FromResult<CommitRecord>(null);

            return Task.FromResult<CommitRecord>(log[nextLogRecordIndex++]);
        }

        public override void EndRecovery()
        {
            ThrowIfNotInMode(LogMode.RecoveryMode);
            mode = LogMode.AppendMode;
        }

        public override long GetStartRecord()
        {
            ThrowIfNotInMode(LogMode.AppendMode);

            return startedTransactionsCount;
        }

        public override Task UpdateStartRecord(long transactionCount)
        {
            ThrowIfNotInMode(LogMode.AppendMode);

            if (transactionCount > startedTransactionsCount)
            {
                startedTransactionsCount = transactionCount;
            }

            return TaskDone.Done;
        }

        public override Task Append(List<CommitRecord> transactions)
        {
            lock (this)
            {
                log.AddRange(transactions);
                foreach (var rec in transactions)
                {
                    rec.LSN = ++logSequenceNumber;
                }
            }

            return TaskDone.Done;
        }

        public override Task TruncateLog(long LSN)
        {
            lock (this)
            {
                int count = 0;
                for (count = 0; count < log.Count; count++)
                {
                    if (log[count].LSN > LSN)
                        break;
                }

                log.RemoveRange(0, count);
            }

            return TaskDone.Done;
        }


        private void ThrowIfNotInMode(LogMode mode)
        {
            if (this.mode != mode)
                throw new InvalidOperationException("Log has to be in {0}" + mode.ToString());
        }

    }
}
