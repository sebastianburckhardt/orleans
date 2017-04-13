
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans;
using Orleans.Transactions;
using Test.TransactionsTests;
using UnitTests.GrainInterfaces;
using Xunit;
using Xunit.Abstractions;

namespace Tester.TransactionsTests
{
    public class GrainDeactivationTransactionTestRunner
    {
        private readonly IGrainFactory grainFactory;
        private readonly ITestOutputHelper output;

        public GrainDeactivationTransactionTestRunner(IGrainFactory grainFactory, ITestOutputHelper output)
        {
            this.grainFactory = grainFactory;
            this.output = output;
        }

        public async Task SingleGrainReadTransaction()
        {
            const int expected = 5;

            IDeactivatingTransactionTestGrain grain = grainFactory.GetGrain<IDeactivatingTransactionTestGrain>(Guid.NewGuid());
            await grain.Set(expected);
            int actual = await grain.Get();
            Assert.Equal(expected, actual);
            await grain.Deactivate();
            actual = await grain.Get();
            Assert.Equal(expected, actual);
        }

        public async Task SingleGrainWriteTransaction()
        {
            const int delta = 5;
            IDeactivatingTransactionTestGrain grain = this.grainFactory.GetGrain<IDeactivatingTransactionTestGrain>(Guid.NewGuid());
            int original = await grain.Get();
            await grain.Add(delta);
            await grain.Deactivate();
            int expected = original + delta;
            int actual = await grain.Get();
            Assert.Equal(expected, actual);
        }

        public async Task MultiGrainWriteTransaction_DeactivateAfterCall()
        {
            const int setval = 5;
            const int addval = 7;
            const int expected = setval; // addval should be lost, because we deactivate before prepare
            const int grainCount = TransactionTestConstants.MaxCoordinatedTransactions;

            List<IDeactivatingTransactionTestGrain> grains =
                Enumerable.Range(0, grainCount)
                    .Select(i => this.grainFactory.GetGrain<IDeactivatingTransactionTestGrain>(Guid.NewGuid()))
                    .ToList();

            IDeactivatingTransactionCoordinatorGrain coordinator = this.grainFactory.GetGrain<IDeactivatingTransactionCoordinatorGrain>(Guid.NewGuid());

            await coordinator.MultiGrainSet(grains, setval);
            // prepare should fail because state was lost
            await Assert.ThrowsAsync<OrleansPrepareFailedException>(() => coordinator.MultiGrainAddAndDeactivate(grains, addval, TransactionDeactivationPhase.AfterCall));

            foreach (var grain in grains)
            {
                int actual = await grain.Get();
                Assert.Equal(expected, actual);
            }
        }

        public async Task MultiGrainWriteTransaction_DeactivateAfterPerpare()
        {
            const int setval = 5;
            const int addval = 7;
            const int expected = setval + addval;
            const int grainCount = TransactionTestConstants.MaxCoordinatedTransactions;

            List<IDeactivatingTransactionTestGrain> grains =
                Enumerable.Range(0, grainCount)
                    .Select(i => this.grainFactory.GetGrain<IDeactivatingTransactionTestGrain>(Guid.NewGuid()))
                    .ToList();

            IDeactivatingTransactionCoordinatorGrain coordinator = this.grainFactory.GetGrain<IDeactivatingTransactionCoordinatorGrain>(Guid.NewGuid());

            await coordinator.MultiGrainSet(grains, setval);
            await coordinator.MultiGrainAddAndDeactivate(grains, addval, TransactionDeactivationPhase.AfterPrepare);

            foreach (var grain in grains)
            {
                int actual = await grain.Get();
                Assert.Equal(expected, actual);
            }
        }

        public async Task MultiGrainWriteTransaction_DeactivateAfterCommit()
        {
            const int setval = 5;
            const int addval = 7;
            const int expected = setval + addval;
            const int grainCount = TransactionTestConstants.MaxCoordinatedTransactions;

            List<IDeactivatingTransactionTestGrain> grains =
                Enumerable.Range(0, grainCount)
                    .Select(i => this.grainFactory.GetGrain<IDeactivatingTransactionTestGrain>(Guid.NewGuid()))
                    .ToList();

            IDeactivatingTransactionCoordinatorGrain coordinator = this.grainFactory.GetGrain<IDeactivatingTransactionCoordinatorGrain>(Guid.NewGuid());

            await coordinator.MultiGrainSet(grains, setval);
            await coordinator.MultiGrainAddAndDeactivate(grains, addval, TransactionDeactivationPhase.AfterCommit);

            foreach (var grain in grains)
            {
                int actual = await grain.Get();
                Assert.Equal(expected, actual);
            }
        }
    }
}
