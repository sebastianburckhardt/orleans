
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
    public class GoldenPathSingleStateTransactionTestRunner
    {
        private readonly IGrainFactory grainFactory;
        private readonly ITestOutputHelper output;

        public GoldenPathSingleStateTransactionTestRunner(IGrainFactory grainFactory, ITestOutputHelper output)
        {
            this.output = output;
            this.grainFactory = grainFactory;
        }

        public async Task SingleGrainReadTransaction()
        {
            output.WriteLine("************************ SingleGrainReadTransaction *********************************");
            const int expected = 0;

            ISingleStateTransactionalGrain grain = grainFactory.GetGrain<ISingleStateTransactionalGrain>(Guid.NewGuid());
            int actual = await grain.Get();
            Assert.Equal(expected, actual);
        }

        public async Task SingleGrainWriteTransaction()
        {
            this.output.WriteLine("************************ SingleGrainWriteTransaction *********************************");
            const int delta = 5;
            ISingleStateTransactionalGrain grain = this.grainFactory.GetGrain<ISingleStateTransactionalGrain>(Guid.NewGuid());
            int original = await grain.Get();
            await grain.Add(delta);
            int expected = original + delta;
            int actual = await grain.Get();
            Assert.Equal(expected, actual);
        }

        public async Task MultiGrainWriteTransaction()
        {
            this.output.WriteLine("************************ MultiGrainWriteTransaction *********************************");
            const int expected = 5;
            const int grainCount = TransactionTestConstants.MaxCoordinatedTransactions;

            List<ISingleStateTransactionalGrain> grains =
                Enumerable.Range(0, grainCount)
                    .Select(i => this.grainFactory.GetGrain<ISingleStateTransactionalGrain>(Guid.NewGuid()))
                    .ToList();

            ISingleStateTransactionCoordinatorGrain coordinator = this.grainFactory.GetGrain<ISingleStateTransactionCoordinatorGrain>(Guid.NewGuid());

            await coordinator.MultiGrainAdd(grains, expected);

            foreach (var grain in grains)
            {
                int actual = await grain.Get();
                Assert.Equal(5, actual);
            }
        }

        public async Task MultiGrainReadWriteTransaction()
        {
            this.output.WriteLine("************************ MultiGrainReadWriteTransaction *********************************");
            const int delta = 5;
            const int grainCount = TransactionTestConstants.MaxCoordinatedTransactions;

            List<ISingleStateTransactionalGrain> grains =
                Enumerable.Range(0, grainCount)
                    .Select(i => this.grainFactory.GetGrain<ISingleStateTransactionalGrain>(Guid.NewGuid()))
                    .ToList();

            ISingleStateTransactionCoordinatorGrain coordinator = this.grainFactory.GetGrain<ISingleStateTransactionCoordinatorGrain>(Guid.NewGuid());

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
            this.output.WriteLine("************************ MultiWriteToSingleGrainTransaction *********************************");
            const int delta = 5;
            const int concurrentWrites = 3;

            ISingleStateTransactionalGrain grain = this.grainFactory.GetGrain<ISingleStateTransactionalGrain>(Guid.NewGuid());
            List<ISingleStateTransactionalGrain> grains = Enumerable.Repeat(grain, concurrentWrites).ToList();

            ISingleStateTransactionCoordinatorGrain coordinator = this.grainFactory.GetGrain<ISingleStateTransactionCoordinatorGrain>(Guid.NewGuid());

            await coordinator.MultiGrainAdd(grains, delta);

            int expected = delta * concurrentWrites;
            int actual = await grains[0].Get();
            Assert.Equal(expected, actual);
        }
    }
}
