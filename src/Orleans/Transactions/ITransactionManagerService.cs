
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Orleans.Transactions
{
    public interface ITransactionStartService
    {
        Task<StartTransactionsResponse> StartTransactions(List<TimeSpan> timeouts);
    }

    public interface ITransactionCommitService
    {
        Task<CommitTransactionsResponse> CommitTransactions(List<TransactionInfo> transactions);
    }

    public interface ITransactionManagerService : ITransactionStartService , ITransactionCommitService
    {
    }

    public interface ITransactionServiceFactory
    {
        Task<ITransactionStartService> GetTransactionStartService();
        Task<ITransactionCommitService> GetTransactionCommitService();
    }

    [Serializable]
    public abstract class TransactionManagerResponse
    {
        public long ReadOnlyTransactionId { get; set; }
        public long AbortLowerBound { get; set; }
    }

    [Serializable]
    public struct CommitResult
    {
        public bool Success { get; set; }

        public OrleansTransactionAbortedException AbortingException { get; set; }
    }

    [Serializable]
    public class CommitTransactionsResponse : TransactionManagerResponse
    {
        public List<CommitResult> CommitResult { get; set; }
    }

    [Serializable]
    public class StartTransactionsResponse : TransactionManagerResponse
    {
        public List<long> TransactionId { get; set; }
    }
}
