
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;

namespace UnitTests.GrainInterfaces
{
    public interface IDeactivatingTransactionCoordinatorGrain : IGrainWithGuidKey
    {
        [Transaction(TransactionOption.RequiresNew)]
        Task MultiGrainSet(List<IDeactivatingTransactionTestGrain> grains, int numberToAdd);

        [Transaction(TransactionOption.RequiresNew)]
        Task MultiGrainAddAndDeactivate(List<IDeactivatingTransactionTestGrain> grains, int numberToAdd, TransactionDeactivationPhase deactivationPhase = TransactionDeactivationPhase.None);
    }
}
