
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
    public class GrainFaultTransactionTestRunner
    {
        private readonly IGrainFactory grainFactory;
        private readonly ITestOutputHelper output;

        public GrainFaultTransactionTestRunner(IGrainFactory grainFactory, ITestOutputHelper output)
        {
            this.grainFactory = grainFactory;
            this.output = output;
        }
        
        public async Task AbortTransactionOnExceptions()
        {
            const int expected = 5;

            ITransactionTestGrain grain = this.grainFactory.GetGrain<ITransactionTestGrain>(Guid.NewGuid());
            ITransactionCoordinatorGrain coordinator = this.grainFactory.GetGrain<ITransactionCoordinatorGrain>(Guid.NewGuid());

            await coordinator.MultiGrainSet(new List<ITransactionTestGrain> { grain }, expected);
            await Assert.ThrowsAsync<Exception>(() => coordinator.AddAndThrow(grain, expected));

            int actual = await grain.Get();
            Assert.Equal(expected, actual);
        }

        public async Task MultiGrainAbortTransactionOnExceptions()
        {
            const int grainCount = TransactionTestConstants.MaxCoordinatedTransactions-1;
            const int expected = 5;

            ITransactionTestGrain throwGrain = this.grainFactory.GetGrain<ITransactionTestGrain>(Guid.NewGuid());
            List<ITransactionTestGrain> grains =
                Enumerable.Range(0, grainCount)
                    .Select(i => this.grainFactory.GetGrain<ITransactionTestGrain>(Guid.NewGuid()))
                    .ToList();
            ITransactionCoordinatorGrain coordinator = this.grainFactory.GetGrain<ITransactionCoordinatorGrain>(Guid.NewGuid());

            await throwGrain.Set(expected);
            await coordinator.MultiGrainSet(grains, expected);
            await Assert.ThrowsAsync<Exception>(() => coordinator.MultiGrainAddAndThrow(throwGrain, grains, expected));

            grains.Add(throwGrain);
            foreach (var grain in grains)
            {
                int actual = await grain.Get();
                Assert.Equal(expected, actual);
            }
        }

        public async Task AbortTransactionOnOrphanCalls()
        {
            const int expected = 5;

            ITransactionTestGrain grain = this.grainFactory.GetGrain<ITransactionTestGrain>(Guid.NewGuid());
            ITransactionCoordinatorGrain coordinator = this.grainFactory.GetGrain<ITransactionCoordinatorGrain>(Guid.NewGuid());

            await grain.Set(expected);
            await Assert.ThrowsAsync<OrleansOrphanCallException>(() => coordinator.OrphanCallTransaction(grain));

            int actual = await grain.Get();
            Assert.Equal(expected, actual);
        }
    }
}
