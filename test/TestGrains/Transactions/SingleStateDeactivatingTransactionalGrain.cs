
using System.Threading.Tasks;
using Orleans;
using Orleans.Transactions;
using Test.TransactionsTests;
using Tester.TransactionsTests;
using UnitTests.GrainInterfaces;

namespace UnitTests.Grains
{
    public class SingleStateDeactivatingTransactionalGrain : Grain, IDeactivatingTransactionTestGrain
    {
        private readonly IDeactivatingTransactionState<GrainData> data;

        public SingleStateDeactivatingTransactionalGrain(
            [TransactionalState(TransactionTestConstants.TransactionStore)]
            IDeactivatingTransactionState<GrainData> data)
        {
            this.data = data;
        }

        public Task Set(int newValue)
        {
            this.data.State.Value = newValue;
            this.data.Save();
            return TaskDone.Done;
        }

        public Task Add(int numberToAdd, TransactionDeactivationPhase deactivationPhase = TransactionDeactivationPhase.None)
        {
            this.data.DeactivationPhase = deactivationPhase;
            this.data.State.Value += numberToAdd;
            this.data.Save();
            return TaskDone.Done;
        }

        public Task<int> Get()
        {
            return Task.FromResult<int>(this.data.State.Value);
        }

        public Task Deactivate()
        {
            this.DeactivateOnIdle();
            return TaskDone.Done;
        }
    }
}