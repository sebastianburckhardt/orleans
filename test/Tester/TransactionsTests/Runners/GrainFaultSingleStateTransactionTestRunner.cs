
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
    public class GrainFaultSingleStateTransactionTestRunner
    {
        private readonly IGrainFactory grainFactory;
        private readonly ITestOutputHelper output;

        public GrainFaultSingleStateTransactionTestRunner(IGrainFactory grainFactory, ITestOutputHelper output)
        {
            this.grainFactory = grainFactory;
            this.output = output;
        }
        
        public async Task AbortTransactionOnExceptions()
        {
            this.output.WriteLine("************************ AbortTransactionOnExceptions *********************************");
            const int expected = 5;

            ISingleStateTransactionalGrain grain = this.grainFactory.GetGrain<ISingleStateTransactionalGrain>(Guid.NewGuid());
            ISingleStateTransactionCoordinatorGrain coordinator = this.grainFactory.GetGrain<ISingleStateTransactionCoordinatorGrain>(Guid.NewGuid());

            await coordinator.MultiGrainSet(new List<ISingleStateTransactionalGrain> { grain }, expected);
            await Assert.ThrowsAsync<Exception>(() => coordinator.AddAndThrow(grain, expected));

            int actual = await grain.Get();
            Assert.Equal(expected, actual);
        }

        public async Task MultiGrainAbortTransactionOnExceptions()
        {
            this.output.WriteLine("************************ AbortTransactionOnExceptions *********************************");
            const int grainCount = TransactionTestConstants.MaxCoordinatedTransactions-1;
            const int expected = 5;

            ISingleStateTransactionalGrain throwGrain = this.grainFactory.GetGrain<ISingleStateTransactionalGrain>(Guid.NewGuid());
            List<ISingleStateTransactionalGrain> grains =
                Enumerable.Range(0, grainCount)
                    .Select(i => this.grainFactory.GetGrain<ISingleStateTransactionalGrain>(Guid.NewGuid()))
                    .ToList();
            ISingleStateTransactionCoordinatorGrain coordinator = this.grainFactory.GetGrain<ISingleStateTransactionCoordinatorGrain>(Guid.NewGuid());

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
            this.output.WriteLine("************************ AbortTransactionOnExceptions *********************************");
            const int expected = 5;

            ISingleStateTransactionalGrain grain = this.grainFactory.GetGrain<ISingleStateTransactionalGrain>(Guid.NewGuid());
            ISingleStateTransactionCoordinatorGrain coordinator = this.grainFactory.GetGrain<ISingleStateTransactionCoordinatorGrain>(Guid.NewGuid());

            await grain.Set(expected);
            await Assert.ThrowsAsync<OrleansOrphanCallException>(() => coordinator.OrphanCallTransaction(grain));

            int actual = await grain.Get();
            Assert.Equal(expected, actual);
        }
    }
}
