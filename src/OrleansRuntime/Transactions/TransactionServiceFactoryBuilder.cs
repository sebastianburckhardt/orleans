
using Orleans.Runtime.Configuration;
using Orleans.Factory;

namespace Orleans.Transactions
{
    internal class TransactionServiceFactoryBuilder : FactoryBuilder<string, ITransactionServiceFactory>
    {
        public TransactionServiceFactoryBuilder(TransactionsConfiguration transactionConfig, NodeConfiguration nodeConfig, IGrainFactory grainFactory)
        {
            this.Add(TransactionsConfiguration.OrleansTransactionManagerType.GrainBased.ToString(), () => new TransactionServiceGrainFactory(grainFactory));
            this.Add(TransactionsConfiguration.OrleansTransactionManagerType.ClientService.ToString(), () => new TransactionServiceProxyFactory(transactionConfig, nodeConfig, grainFactory));
        }
    }
}
