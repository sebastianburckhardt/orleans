using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Transactions
{
    public class TransactionManagerService : ITransactionManagerService
    {
        private readonly ITransactionManager tm;

        public TransactionManagerService(ITransactionManager tm)
        {
            this.tm = tm;
        }

        public Task<StartTransactionsResponse> StartTransactions(List<TimeSpan> timeouts)
        {
            var result = new StartTransactionsResponse { TransactionId = new List<long>() };

            foreach (var timeout in timeouts)
            {
                result.TransactionId.Add(tm.StartTransaction(timeout));
            }

            result.ReadOnlyTransactionId = tm.GetReadOnlyTransactionId();
            result.AbortLowerBound = result.ReadOnlyTransactionId;

            return Task.FromResult(result);
        }

        public async Task<CommitTransactionsResponse> CommitTransactions(List<TransactionInfo> transactions)
        {
            List<Task> tasks = new List<Task>();

            var result = new CommitTransactionsResponse { CommitResult = new List<CommitResult>() };

            foreach (var ti in transactions)
            {
                tasks.Add(tm.CommitTransaction(ti));
            }

            foreach (var t in tasks)
            {
                try
                {
                    await t;
                    result.CommitResult.Add(new CommitResult() { Success = true });
                }
                catch (OrleansTransactionAbortedException e)
                {
                    result.CommitResult.Add(new CommitResult() { Success = false, AbortingException = e });
                }
            }

            result.ReadOnlyTransactionId = tm.GetReadOnlyTransactionId();
            result.AbortLowerBound = result.ReadOnlyTransactionId;

            return result;
        }
    }
}
