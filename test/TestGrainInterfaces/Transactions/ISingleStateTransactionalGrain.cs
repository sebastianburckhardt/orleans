
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;
using Orleans.Concurrency;

namespace UnitTests.GrainInterfaces
{
    public interface ISingleStateTransactionalGrain : IGrainWithGuidKey
    {
        [Transaction(TransactionOption.Required)]
        Task Set(int newValue);

        [Transaction(TransactionOption.Required)]
        Task Add(int numberToAdd);

        [Transaction(TransactionOption.Required)]
        Task<int> Get();

        [Transaction(TransactionOption.Required)]
        Task<int> AddAndThrow(int numberToAdd);
    }

    public interface ISingleStateTransactionCoordinatorGrain : IGrainWithGuidKey
    {
        [Transaction(TransactionOption.RequiresNew)]
        Task MultiGrainSet(List<ISingleStateTransactionalGrain> grains, int numberToAdd);

        [Transaction(TransactionOption.RequiresNew)]
        Task MultiGrainAdd(List<ISingleStateTransactionalGrain> grains, int numberToAdd);

        [Transaction(TransactionOption.RequiresNew)]
        Task MultiGrainDouble(List<ISingleStateTransactionalGrain> grains);

        [Transaction(TransactionOption.RequiresNew)]
        Task OrphanCallTransaction(ISingleStateTransactionalGrain grain);

        [Transaction(TransactionOption.RequiresNew)]
        Task AddAndThrow(ISingleStateTransactionalGrain grain, int numberToAdd);

        [Transaction(TransactionOption.RequiresNew)]
        Task MultiGrainAddAndThrow(ISingleStateTransactionalGrain grain, List<ISingleStateTransactionalGrain> grains, int numberToAdd);
    }
}
