
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Orleans.Transactions
{
    /// <summary>
    /// Implementation of the pseudo-grain used by the TransactionAgent on each silo to communicate
    /// with the Transaction Manager.
    /// </summary>
    public class TransactionManagerProxy : ITransactionManagerProxy
    {
        private readonly ITransactionManagerService transactionManagerService;

        public TransactionManagerProxy(ITransactionManager transactionManager)
        {
            this.transactionManagerService = new TransactionManagerService(transactionManager);
        }

        public Task<StartTransactionsResponse> StartTransactions(List<TimeSpan> timeouts)
        {
            return this.transactionManagerService.StartTransactions(timeouts);
        }

        public Task<CommitTransactionsResponse> CommitTransactions(List<TransactionInfo> transactions)
        {
            return this.transactionManagerService.CommitTransactions(transactions);
        }
    }
}
