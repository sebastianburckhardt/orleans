using Orleans.Transactions;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using UnitTests.GrainInterfaces;
using Xunit;
using Orleans.TestingHost;
using Orleans.Runtime.Configuration;
using TestExtensions;

namespace UnitTests.TransactionsTests
{
    public class BasicTransactionsTests : TestClusterPerTest
    {
        public override TestCluster CreateTestCluster()
        {
            var options = new TestClusterOptions();
            options.ClusterConfiguration.AddMemoryStorageProvider("Default");
            options.ClusterConfiguration.AddMemoryStorageProvider("MemoryStore");
            options.EnableTransactions = true;
            return new TestCluster(options);
        }

        [Fact, TestCategory("Transactions")]
        public async Task BasicTransactionTest()
        {
            ISimpleTransactionalGrain grain = GrainFactory.GetGrain<ISimpleTransactionalGrain>(0);
            var old = await grain.GetLatest();
            await grain.Add(5);
            Assert.Equal(old + 5, await grain.GetLatest());
        }

        [Fact, TestCategory("Transactions")]
        public async Task MultiGrainTransactionTest()
        {
            List<ISimpleTransactionalGrain> grains = new List<ISimpleTransactionalGrain>();

            for (int i = 0; i < 5; i++)
            {
                grains.Add(GrainFactory.GetGrain<ISimpleTransactionalGrain>(i));
            }

            ITransactionCoordinatorGrain coordinator = GrainFactory.GetGrain<ITransactionCoordinatorGrain>(0);

            await coordinator.MultiGrainTransaction(grains, 5);

            foreach (var g in grains)
            {
                Assert.Equal(5, await g.GetLatest());
            }
        }

        [Fact, TestCategory("Transactions")]
        public async Task MultiWriteToSingleGrainTransactionTest()
        {
            List<ISimpleTransactionalGrain> grains = new List<ISimpleTransactionalGrain>();

            for (int i = 0; i < 3; i++)
            {
                grains.Add(GrainFactory.GetGrain<ISimpleTransactionalGrain>(0));
            }

            ITransactionCoordinatorGrain coordinator = GrainFactory.GetGrain<ITransactionCoordinatorGrain>(0);

            await coordinator.MultiGrainTransaction(grains, 5);

            Assert.Equal(15, await grains[0].GetLatest());
        }

        [Fact, TestCategory("Transactions")]
        public async Task AbortTransactionOnExceptionsTest()
        {
            var grain = GrainFactory.GetGrain<ISimpleTransactionalGrain>(0);
            var coordinator = GrainFactory.GetGrain<ITransactionCoordinatorGrain>(0);

            try
            {
                await coordinator.ExceptionThrowingTransaction(grain);
                Assert.True(false);
            }
            catch (Exception)
            {
            }

            Assert.Equal(0, await grain.GetLatest());
        }

        [Fact, TestCategory("Transactions")]
        public async Task AbortTransactionOnOrphanCallsTest()
        {
            var grain = GrainFactory.GetGrain<ISimpleTransactionalGrain>(0);
            var coordinator = GrainFactory.GetGrain<ITransactionCoordinatorGrain>(0);

            try
            {
                await coordinator.OrphanCallTransaction(grain);
            }
            catch (OrleansOrphanCallException)
            {
                Assert.Equal(0, await grain.GetLatest());
                return;
            }
            Assert.True(false);

        }

        [Fact, TestCategory("Transactions")]
        public async Task AbortReadOnlyTransactionOnWriteTest()
        {
            var grain = GrainFactory.GetGrain<ISimpleTransactionalGrain>(0);
            var coordinator = GrainFactory.GetGrain<ITransactionCoordinatorGrain>(0);
            
            try
            {
                await coordinator.WriteInReadOnlyTransaction(grain);
            }
            catch (OrleansReadOnlyViolatedException)
            {
                return;
            }
            Assert.True(false, "Write operation should cause ReadOnlyViolatedException");
        }
    }
}
