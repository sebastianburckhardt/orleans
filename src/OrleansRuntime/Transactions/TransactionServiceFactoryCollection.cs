
using System;
using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Runtime;

namespace Orleans.Transactions
{
    internal class TransactionServiceFactoryCollection : IKeyedServiceCollection<string, ITransactionServiceFactory>
    {
        private readonly IServiceProvider serviceProvider;
        private readonly Dictionary<string,Type> transactionServiceFactories;

        public TransactionServiceFactoryCollection(IServiceProvider serviceProvider)
        {
            this.serviceProvider = serviceProvider;
            transactionServiceFactories = new Dictionary<string, Type>
            {
                {
                    TransactionsConfiguration.OrleansTransactionManagerType.GrainBased.ToString(),
                    typeof(TransactionServiceGrainFactory)
                },
                {
                    TransactionsConfiguration.OrleansTransactionManagerType.ClientService.ToString(),
                    typeof(TransactionServiceProxyFactory)
                }
            };
        }

        public ITransactionServiceFactory GetService(string key)
        {
            Type transactionServiceFactoryType;
            return this.transactionServiceFactories.TryGetValue(key, out transactionServiceFactoryType)
                ? ActivatorUtilities.CreateInstance(serviceProvider,transactionServiceFactoryType) as ITransactionServiceFactory
                : default(ITransactionServiceFactory);
        }
    }
}
