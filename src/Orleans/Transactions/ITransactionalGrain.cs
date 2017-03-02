using System.Threading.Tasks;
using Orleans.Concurrency;

namespace Orleans.Transactions
{
    public interface ITransactionalGrain : IGrainWithIntegerKey
    {
        /// <summary>
        /// Perform the prepare phase of the commit protocol. To succeed the grain
        /// must have all the writes that were part of the transaction and is able
        /// to persist these writes to persistent storage.
        /// </summary>
        /// <param name="transactionId">Id of the transaction to prepare</param>
        /// <param name="writeCount">Number of writes to the grain done in the transaciton</param>
        /// <returns>whether prepare was performed successfully</returns>
        /// <remarks>
        /// It is possible for the grain to abort the transaction even after
        /// this call returns true, but only if it can determine that the
        /// coordinator is going to abort the transaction anyway (e.g. a
        /// dependent transaction aborts)
        /// </remarks>
        [AlwaysInterleave]
        [Transaction(TransactionOption.NotSupported)]
        Task<bool> Prepare(long transactionId, GrainVersion? writeVersion, GrainVersion? readVersion);

        /// <summary>
        /// Notification of a transaction abort.
        /// </summary>
        /// <param name="transactionId">Id of the aborted transaction</param>
        [AlwaysInterleave]
        [Transaction(TransactionOption.NotSupported)]
        Task Abort(long transactionId);

        /// <summary>
        /// Second phase of the commit protocol.
        /// </summary>
        /// <param name="transactionId">Id of the committed transaction</param>
        /// <remarks>
        /// If this method returns without throwing an exception the manager is
        /// allowed to forget about the transaction. This means that the grain
        /// must durably remember that this transaction committed so that it does
        /// not query for its status.
        /// </remarks>
        [AlwaysInterleave]
        [Transaction(TransactionOption.NotSupported)]
        Task Commit(long transactionId);
    }
}
