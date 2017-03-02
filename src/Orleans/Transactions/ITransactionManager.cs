
using System;
using System.Threading.Tasks;

namespace Orleans.Transactions
{
    public interface ITransactionManager
    {
        /// <summary>
        /// Start the TM
        /// </summary>
        /// <remarks>
        /// This must be called before any other method.
        /// </remarks>
        Task StartAsync();

        /// <summary>
        /// Start a new transaction.
        /// </summary>
        /// <param name="timeout">
        /// Transaction is automatically aborted if it does not complete within timeout
        /// </param>
        /// <returns>Id of the started transaction</returns>
        long StartTransaction(TimeSpan timeout);

        /// <summary>
        /// Commit Transaction.
        /// </summary>
        /// <param name="transactionInfo"></param>
        /// <returns>
        /// Transaction is committed if this method returns with no exception.
        /// </returns>
        /// <exception cref="OrleansTransactionAbortedException"></exception>
        Task CommitTransaction(TransactionInfo transactionInfo);

        /// <summary>
        /// Abort Transaction.
        /// </summary>
        /// <param name="transactionId"></param>
        /// <param name="reason"></param>
        /// <returns></returns>
        /// <remarks>
        /// If called after CommitTransaction was called for the transaction it will be ignored.
        /// </remarks>
        void AbortTransaction(long transactionId, OrleansTransactionAbortedException reason);

        /// <summary>
        /// Return a safe TransactionId for read-only snapshot isolation.
        /// </summary>
        long GetReadOnlyTransactionId();

    }
}
