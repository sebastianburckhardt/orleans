
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans;
using UnitTests.GrainInterfaces;

namespace UnitTests.Grains
{
    public class DeactivatingTransactionCoordinatorGrain : Grain, IDeactivatingTransactionCoordinatorGrain
    {
        public Task MultiGrainSet(List<IDeactivatingTransactionTestGrain> grains, int newValue)
        {
            return Task.WhenAll(grains.Select(g => g.Set(newValue)));
        }

        public Task MultiGrainAddAndDeactivate(List<IDeactivatingTransactionTestGrain> grains, int numberToAdd, TransactionDeactivationPhase deactivationPhase = TransactionDeactivationPhase.None)
        {
            return Task.WhenAll(grains.Select(g => g.Add(numberToAdd, deactivationPhase)));
        }
    }
}