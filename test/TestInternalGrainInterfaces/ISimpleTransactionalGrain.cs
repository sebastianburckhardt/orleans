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
        // the following tests start transactions via declarative transaction attributes

        [Transaction(TransactionOption.RequiresNew)]
        Task MultiGrainTransaction(List<ISimpleTransactionalGrain> grains, int numberToAdd);

        [Transaction(TransactionOption.RequiresNew)]
        Task OrphanCallTransaction(ISimpleTransactionalGrain grain);

        [Transaction(TransactionOption.RequiresNew)]
        Task ExceptionThrowingTransaction(ISimpleTransactionalGrain grain);

        [ReadOnly]
        [Transaction(TransactionOption.RequiresNew)]
        Task WriteInReadOnlyTransaction(ISimpleTransactionalGrain grain);


        // the following tests start transactions explicitly, calling RunTransaction during execution

        Task ExplicitlyScopedMultiGrainTransaction(List<ISimpleTransactionalGrain> grains, int numberToAdd);

        Task ExplicitlyScopedOrphanCallTransaction(ISimpleTransactionalGrain grain);

        Task ExplicitlyScopedExceptionThrowingTransaction(ISimpleTransactionalGrain grain);

        Task ExplicitlyScopedWriteInReadOnlyTransaction(ISimpleTransactionalGrain grain);

        Task<int> ExplicitlyScopedReadOnlyTransaction(List<ISimpleTransactionalGrain> grains);

        Task NestedScopes(List<ISimpleTransactionalGrain> grains, int numberToAdd);

        Task NestedScopesInnerAbort(ISimpleTransactionalGrain grain);

        Task NestedScopesOuterAbort(ISimpleTransactionalGrain grain);

        Task NestedScopesRejection(ISimpleTransactionalGrain grain);

    }
}
