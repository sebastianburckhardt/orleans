
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Orleans.Transactions
{
    [Serializable]
    public class CommitRecord
    {
        public long TransactionId;
        public long LSN;
        public HashSet<ITransactionalGrain> Grains = new HashSet<ITransactionalGrain>();
    }

    /// <summary>
    /// This class represents the durable Transaction Log.
    /// Olreans Transaction Log has 2 types of entries:
    ///     1- StartRecord: There is exactly 1 entry of this type in the log. It logs the
    ///         number of started transactions so far.
    ///     2- CommitRecord: An entry is appended to the log when a transaction commits.
    ///     
    /// Usage:
    /// The log can be in 2 modes.
    ///     1- When first initialized the log is in Recovery Mode. In this mode the client calls
    ///         GetFirstCommitRecord followed by a sequence of GetNextCommitRecord() calls to 
    ///         retrieve the log entries. Finally the client calls EndRecovery().
    ///     2- The log becomes in Append Mode after the call to EndRecovery().
    ///         This is the normal mode of operation in which the caller can modify the log by
    ///         appending entries and removing entries that are no longer necessary.
    /// </summary>
    public abstract class TransactionLog
    {
        /// <summary>
        /// Initialize the log (in Recovery Mode). This method must be called before any other method
        /// is called on the log.
        /// </summary>
        /// <returns></returns>
        public abstract void Initialize();

        /// <summary>
        /// Gets the first CommitRecord in the log.
        /// </summary>
        /// <returns>
        /// The CommitRecord with the lowest LSN in the log, or null if there is none.
        /// </returns>
        public abstract Task<CommitRecord> GetFirstCommitRecord();

        /// <summary>
        /// Returns the CommitRecord with LSN following the LSN of record returned by the last
        /// GetFirstcommitRecord() or GetNextCommitRecord() call.
        /// </summary>
        /// <returns>
        /// The next CommitRecord, or null if there is none.
        /// </returns>
        public abstract Task<CommitRecord> GetNextCommitRecord();

        /// <summary>
        /// Exit recovery and enter Append Mode.
        /// </summary>
        public abstract void EndRecovery();

        public abstract long GetStartRecord();

        public abstract Task UpdateStartRecord(long transactionId);

        /// <summary>
        /// Append the given records to the log in order
        /// </summary>
        /// <param name="transactions">Commit Records</param>
        /// <remarks>
        /// If an exception is thrown it is possible that a prefix of the records are persisted
        /// to the log.
        /// </remarks>
        public abstract Task Append(List<CommitRecord> transactions);

        public abstract Task TruncateLog(long LSN);

        protected enum LogMode
        {
            Uninitialized = 0,
            RecoveryMode,
            AppendMode
        };
    }
}
