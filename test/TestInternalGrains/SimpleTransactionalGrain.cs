using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;
using Orleans.Transactions;
using UnitTests.GrainInterfaces;

namespace UnitTests.Grains
{
    [Serializable]
    public class TransactionalGrainState
    {
        public int Value { get; set; }
    }

    [Orleans.Providers.StorageProvider(ProviderName = "MemoryStore")]
    public class SimpleTransactionalGrain : TransactionalGrain<TransactionalGrainState>, ISimpleTransactionalGrain
    {
        public Task Add(int n)
        {
            this.State.Value += n;
            this.SaveState();
            return TaskDone.Done;
        }

        public Task<int> Get()
        {
            return Task.FromResult<int>(this.State.Value);
        }

        public Task<int> GetLatest()
        {
            return Task.FromResult<int>(this.State.Value);
        }
    }

    public class TransactionCoordinatorGrain : Grain, ITransactionCoordinatorGrain
    {
        public async Task MultiGrainTransaction(List<ISimpleTransactionalGrain> grains, int numberToAdd)
        {
            foreach (var g in grains)
            {
                await g.Add(numberToAdd);
            }
        }

        public Task OrphanCallTransaction(ISimpleTransactionalGrain grain)
        {
            Task t = grain.Add(1000);
            return TaskDone.Done;
        }

        public async Task ExceptionThrowingTransaction(ISimpleTransactionalGrain grain)
        {
            await grain.Add(1000);
            throw new Exception("This should abort the transaction");
        }

        public async Task WriteInReadOnlyTransaction(ISimpleTransactionalGrain grain)
        {
            await grain.Add(5);
        }
    }
}
