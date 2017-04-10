
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans;
using Orleans.Transactions;
using Test.TransactionsTests;
using UnitTests.GrainInterfaces;

namespace UnitTests.Grains
{
    [Serializable]
    public class GrainData
    {
        public int Value { get; set; }
    }

    public class SingleStateTransactionalGrain : Grain, ISingleStateTransactionalGrain
    {
        private readonly ITransactionalState<GrainData> data;

        public SingleStateTransactionalGrain(
            [TransactionalState(TransactionTestConstants.TransactionStore)]
            ITransactionalState<GrainData> data)
        {
            this.data = data;
        }

        public Task Set(int newValue)
        {
            this.data.State.Value = newValue;
            this.data.Save();
            return TaskDone.Done;
        }

        public Task Add(int numberToAdd)
        {
            this.data.State.Value += numberToAdd;
            this.data.Save();
            return TaskDone.Done;
        }

        public Task<int> Get()
        {
            return Task.FromResult<int>(this.data.State.Value);
        }

        public Task<int> AddAndThrow(int numberToAdd)
        {
            this.data.State.Value += numberToAdd;
            this.data.Save();
            throw new Exception($"{GetType().Name} test exception");
        }
    }

    public class SingleStateTransactionCoordinatorGrain : Grain, ISingleStateTransactionCoordinatorGrain
    {
        public Task MultiGrainSet(List<ISingleStateTransactionalGrain> grains, int newValue)
        {
            return Task.WhenAll(grains.Select(g => g.Set(newValue)));
        }

        public Task MultiGrainAdd(List<ISingleStateTransactionalGrain> grains, int numberToAdd)
        {
            return Task.WhenAll(grains.Select(g => g.Add(numberToAdd)));
        }

        public Task MultiGrainDouble(List<ISingleStateTransactionalGrain> grains)
        {
            return Task.WhenAll(grains.Select(Double));
        }

        public Task OrphanCallTransaction(ISingleStateTransactionalGrain grain)
        {
            Task t = grain.Add(1000);
            return TaskDone.Done;
        }

        public async Task AddAndThrow(ISingleStateTransactionalGrain grain, int numberToAdd)
        {
            await grain.Add(numberToAdd);
            throw new Exception("This should abort the transaction");
        }

        public async Task MultiGrainAddAndThrow(ISingleStateTransactionalGrain throwGrain, List<ISingleStateTransactionalGrain> grains, int numberToAdd)
        {
            await Task.WhenAll(grains.Select(g => g.Add(numberToAdd)));
            await throwGrain.AddAndThrow(numberToAdd);
        }

        private async Task Double(ISingleStateTransactionalGrain grain)
        {
            int value = await grain.Get();
            await grain.Add(value);
        }
    }
}
