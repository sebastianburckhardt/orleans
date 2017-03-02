using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;
using Orleans.Concurrency;
using Orleans.Transactions;

namespace UnitTests.GrainInterfaces
{
    public interface ISimpleTransactionalGrain : ITransactionalGrain
    {
        Task Add(int n);
        Task<int> GetLatest();
        [ReadOnly]
        Task<int> Get();
        
    }

    public interface ITransactionCoordinatorGrain : IGrainWithIntegerKey
    {
        [Transaction(TransactionOption.RequiresNew)]
        Task MultiGrainTransaction(List<ISimpleTransactionalGrain> grains, int numberToAdd);

        [Transaction(TransactionOption.RequiresNew)]
        Task OrphanCallTransaction(ISimpleTransactionalGrain grain);

        [Transaction(TransactionOption.RequiresNew)]
        Task ExceptionThrowingTransaction(ISimpleTransactionalGrain grain);

        [ReadOnly]
        [Transaction(TransactionOption.RequiresNew)]
        Task WriteInReadOnlyTransaction(ISimpleTransactionalGrain grain);

    }
}
