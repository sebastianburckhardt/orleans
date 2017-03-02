
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Concurrency;

namespace Orleans.Transactions
{
    public interface ITransactionManagerGrain : ITransactionManagerService, IGrainWithIntegerKey
    {
    }

    [Reentrant]
    public class TransactionManagerGrain : Grain, ITransactionManagerGrain
    {
        private ITransactionManagerService transactionManagerService;

        /// <summary>
        /// This method is called at the end of the process of activating a grain.
        /// It is called before any messages have been dispatched to the grain.
        /// For grains with declared persistent state, this method is called after the State property has been populated.
        /// </summary>
        public override async Task OnActivateAsync()
        {
            ITransactionManager transactionManager = this.ServiceProvider.GetRequiredService<InClusterTransactionManager>();
            await transactionManager.StartAsync();
            transactionManagerService = new TransactionManagerService(transactionManager);
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
