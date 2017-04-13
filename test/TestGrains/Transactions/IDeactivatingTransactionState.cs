
using Orleans.Transactions;
using UnitTests.GrainInterfaces;

namespace Tester.TransactionsTests
{
    public interface IDeactivatingTransactionState<out TState> : ITransactionalState<TState> where TState : class, new()
    {
        TransactionDeactivationPhase DeactivationPhase { get; set; }
    }
}