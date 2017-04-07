
using System;
using System.Collections.Generic;
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

    [Serializable]
    public class Thingy<T>
    {
        public T Value { get; set; }
    }

    public class FacetedTransactionCoordinatorGrain : Grain, IFacetedTransactionCoordinatorGrain
    {

        private readonly ITransactionalState<Thingy<int>> intTransactionalState;
        private readonly ITransactionalState<Thingy<object>> objTransactionalState;

        public FacetedTransactionCoordinatorGrain(
            [TransactionalState("MemoryStore", "bob")]
            ITransactionalState<Thingy<int>> intTransactionalState,
            ITransactionalState<Thingy<object>> objTransactionalState)
        {
            this.intTransactionalState = intTransactionalState;
            this.objTransactionalState = objTransactionalState;
        }

        public async Task MultiGrainTransaction(List<ISimpleTransactionalGrain> grains, int numberToAdd)
        {
            intTransactionalState.State.Value += numberToAdd;
            intTransactionalState.Save();
            objTransactionalState.State.Value = numberToAdd.ToString();
            objTransactionalState.Save();
            foreach (var g in grains)
            {
                await g.Add(numberToAdd);
            }
        }
    }
}
