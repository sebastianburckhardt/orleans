
using System.Threading.Tasks;
using Orleans;

namespace UnitTests.GrainInterfaces
{
    public enum TransactionDeactivationPhase
    {
        None,
        AfterCall,
        AfterPrepare,
        AfterCommit,
    }

    public interface IDeactivatingTransactionTestGrain : IGrainWithGuidKey
    {
        [Transaction(TransactionOption.Required)]
        Task Set(int newValue);

        [Transaction(TransactionOption.Required)]
        Task Add(int numberToAdd, TransactionDeactivationPhase deactivationPhase = TransactionDeactivationPhase.None);

        [Transaction(TransactionOption.Required)]
        Task<int> Get();

        Task Deactivate();
    }
}