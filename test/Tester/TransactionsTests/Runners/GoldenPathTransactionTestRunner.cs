
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans;
using Test.TransactionsTests;
using UnitTests.GrainInterfaces;
using Xunit;
using Xunit.Abstractions;

namespace Tester.TransactionsTests
{
    public class GoldenPathTransactionTestRunner
    {
        private readonly IGrainFactory grainFactory;
        private readonly ITestOutputHelper output;

        public GoldenPathTransactionTestRunner(IGrainFactory grainFactory, ITestOutputHelper output)
        {
            this.output = output;
            this.grainFactory = grainFactory;
        }

        public async Task SingleGrainReadTransaction()
        {
            const int expected = 0;

            ITransactionTestGrain grain = grainFactory.GetGrain<ITransactionTestGrain>(Guid.NewGuid());
            int actual = await grain.Get();
            Assert.Equal(expected, actual);
        }

        public async Task SingleGrainWriteTransaction()
        {
            const int delta = 5;
            ITransactionTestGrain grain = this.grainFactory.GetGrain<ITransactionTestGrain>(Guid.NewGuid());
            int original = await grain.Get();
            await grain.Add(delta);
            int expected = original + delta;
            int actual = await grain.Get();
            Assert.Equal(expected, actual);
        }

        public async Task MultiGrainWriteTransaction()
        {
            const int expected = 5;
            const int grainCount = TransactionTestConstants.MaxCoordinatedTransactions;

            List<ITransactionTestGrain> grains =
                Enumerable.Range(0, grainCount)
                    .Select(i => this.grainFactory.GetGrain<ITransactionTestGrain>(Guid.NewGuid()))
                    .ToList();

            ITransactionCoordinatorGrain coordinator = this.grainFactory.GetGrain<ITransactionCoordinatorGrain>(Guid.NewGuid());

            await coordinator.MultiGrainAdd(grains, expected);

            foreach (var grain in grains)
            {
                int actual = await grain.Get();
                Assert.Equal(5, actual);
            }
        }

        public async Task MultiGrainReadWriteTransaction()
        {
            const int delta = 5;
            const int grainCount = TransactionTestConstants.MaxCoordinatedTransactions;

            List<ITransactionTestGrain> grains =
                Enumerable.Range(0, grainCount)
                    .Select(i => this.grainFactory.GetGrain<ITransactionTestGrain>(Guid.NewGuid()))
                    .ToList();

            ITransactionCoordinatorGrain coordinator = this.grainFactory.GetGrain<ITransactionCoordinatorGrain>(Guid.NewGuid());

            await coordinator.MultiGrainSet(grains, delta);
            await coordinator.MultiGrainDouble(grains);

            int expected = delta + delta;
            foreach (var grain in grains)
            {
                int actual = await grain.Get();
                Assert.Equal(expected, actual);
            }
        }

        public async Task MultiWriteToSingleGrainTransaction()
        {
            const int delta = 5;
            const int concurrentWrites = 3;

            ITransactionTestGrain grain = this.grainFactory.GetGrain<ITransactionTestGrain>(Guid.NewGuid());
            List<ITransactionTestGrain> grains = Enumerable.Repeat(grain, concurrentWrites).ToList();

            ITransactionCoordinatorGrain coordinator = this.grainFactory.GetGrain<ITransactionCoordinatorGrain>(Guid.NewGuid());

            await coordinator.MultiGrainAdd(grains, delta);

            int expected = delta * concurrentWrites;
            int actual = await grains[0].Get();
            Assert.Equal(expected, actual);
        }
    }
}
