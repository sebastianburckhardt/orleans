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

        public async Task ExplicitlyScopedMultiGrainTransaction(List<ISimpleTransactionalGrain> grains, int numberToAdd)
        {
            await RunTransaction(async () =>
            {
                foreach (var g in grains)
                {
                    await g.Add(numberToAdd);
                }
            });
        }

        public async Task ExplicitlyScopedExceptionThrowingTransaction(ISimpleTransactionalGrain grain)
        {
            await RunTransaction(async () =>
            {
                await grain.Add(1000);

                throw new Exception("This should abort the transaction");
            });
        }

        public async Task ExplicitlyScopedOrphanCallTransaction(ISimpleTransactionalGrain grain)
        {
            await RunTransaction(() =>
            {
                Task t = grain.Add(1000); // not awaited - that's a mistake
                return TaskDone.Done;
            });
        }

        public Task<int> ExplicitlyScopedReadOnlyTransaction(List<ISimpleTransactionalGrain> grains)
        {
            return RunTransaction(new TransactionOptions() { ReadOnly = true }, async () =>
            {
                var sum = 0;
                foreach (var g in grains)
                   sum += await g.Get();
                return sum;
            });
        }


        public async Task ExplicitlyScopedWriteInReadOnlyTransaction(ISimpleTransactionalGrain grain)
        {
            await RunTransaction(new TransactionOptions() { ReadOnly = true }, async () =>
            {
                await grain.Add(5); // throws exception because it modifies the state
            });
        }

        public async Task NestedScopes(List<ISimpleTransactionalGrain> grains, int numberToAdd)
        {
            await RunTransaction(async () =>
            {
                var tasks = new List<Task>();

                foreach (var g in grains)
                {
                    tasks.Add(RunTransaction(async () =>
                    {
                        await g.Add(numberToAdd);
                    }));
                }

                await Task.WhenAll(tasks);
            });
        }

        public async Task NestedScopesInnerAbort(ISimpleTransactionalGrain grain)
        {
            await RunTransaction(async () =>
            {
                await RunTransaction(async () =>
                     {
                         await grain.Add(1000);
                         throw new InvalidOperationException("aborting");
                    });
            });
        }

        public async Task NestedScopesOuterAbort(ISimpleTransactionalGrain grain)
        {
            await RunTransaction(async () =>
            {
                await RunTransaction(async () =>
                {
                    await grain.Add(1000);
                });
                throw new InvalidOperationException("aborting");
            });
        }

        public async Task NestedScopesRejection(ISimpleTransactionalGrain grain)
        {
            await RunTransaction(async () =>
            {
                await RunTransaction(new TransactionOptions() { MustBeOutermostScope = true }, async () =>
                {
                    await grain.Add(1000);
                });
            });
        }

    }
}
