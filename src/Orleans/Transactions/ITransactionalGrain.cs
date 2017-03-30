﻿
using System;
using System.Threading.Tasks;
using Orleans.Concurrency;

namespace Orleans.Transactions
{
    /// <summary>
    /// Grain interface for grains that can take part in transaction orchestration.
    /// </summary>
    public interface ITransactionalGrain : IGrainWithIntegerKey
    {
        /// <summary>
        /// Perform the prepare phase of the commit protocol. To succeed the grain
        /// must have all the writes that were part of the transaction and is able
        /// to persist these writes to persistent storage.
        /// <param name="transactionId">Id of the transaction to prepare</param>
        /// <param name="writeVersion">version of state to prepare for write</param>
        /// <param name="readVersion">version of state to prepare for read</param>
        /// </summary>
        /// <returns>Whether prepare was performed successfully</returns>
        /// <remarks>
        /// It is possible for the grain to abort the transaction even after
        /// this call returns true, but only if it can determine that the
        /// coordinator is going to abort the transaction anyway (e.g. a
        /// dependent transaction aborts)
        /// </remarks>
        [AlwaysInterleave]
        [Transaction(TransactionOption.NotSupported)]
        Task<bool> Prepare(long transactionId, TransactionalUnitVersion? writeVersion, TransactionalUnitVersion? readVersion);

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

    internal static class TransactionalGrainExtensions
    {
        public static ITransactionalUnit AsUnit(this ITransactionalGrain grain)
        {
            return new TransactionalUnitGrainWrapper(grain);
        }

        [Serializable]
        [Immutable]
        internal class TransactionalUnitGrainWrapper : ITransactionalUnit
        {
            private readonly ITransactionalGrain grain;

            public TransactionalUnitGrainWrapper(ITransactionalGrain grain)
            {
                this.grain = grain;
            }

            public Task<bool> Prepare(long transactionId, TransactionalUnitVersion? writeVersion, TransactionalUnitVersion? readVersion)
            {
                return this.grain.Prepare(transactionId, writeVersion, readVersion);
            }

            public Task Abort(long transactionId)
            {
                return this.grain.Abort(transactionId);
            }

            public Task Commit(long transactionId)
            {
                return this.grain.Commit(transactionId);
            }
        }
    }
}
