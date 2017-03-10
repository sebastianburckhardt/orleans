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

        public enum TransactionScoping { Attributes, Explicit }

        [Theory, TestCategory("Transactions")]
        [InlineData(TransactionScoping.Attributes)]
        [InlineData(TransactionScoping.Explicit)]
        public async Task MultiGrainTransactionTest(TransactionScoping scoping)
        {
            List<ISimpleTransactionalGrain> grains = new List<ISimpleTransactionalGrain>();

            for (int i = 0; i < 5; i++)
            {
                grains.Add(GrainFactory.GetGrain<ISimpleTransactionalGrain>(i));
            }

            ITransactionCoordinatorGrain coordinator = GrainFactory.GetGrain<ITransactionCoordinatorGrain>(0);

            if (scoping == TransactionScoping.Explicit)
                await coordinator.ExplicitlyScopedMultiGrainTransaction(grains, 5);
            else
                await coordinator.MultiGrainTransaction(grains, 5);

            foreach (var g in grains)
            {
                Assert.Equal(5, await g.GetLatest());
            }
        }

        [Theory, TestCategory("Transactions")]
        [InlineData(TransactionScoping.Attributes)]
        [InlineData(TransactionScoping.Explicit)]
        public async Task MultiWriteToSingleGrainTransactionTest(TransactionScoping scoping)
        {
            List<ISimpleTransactionalGrain> grains = new List<ISimpleTransactionalGrain>();

            for (int i = 0; i < 3; i++)
            {
                grains.Add(GrainFactory.GetGrain<ISimpleTransactionalGrain>(0));
            }

            ITransactionCoordinatorGrain coordinator = GrainFactory.GetGrain<ITransactionCoordinatorGrain>(0);

            if (scoping == TransactionScoping.Explicit)
                await coordinator.ExplicitlyScopedMultiGrainTransaction(grains, 5);
            else
                await coordinator.MultiGrainTransaction(grains, 5);

            Assert.Equal(15, await grains[0].GetLatest());
        }

        [Theory, TestCategory("Transactions")]
        [InlineData(TransactionScoping.Attributes)]
        [InlineData(TransactionScoping.Explicit)]
        public async Task AbortTransactionOnExceptionsTest(TransactionScoping scoping)
        {
            var grain = GrainFactory.GetGrain<ISimpleTransactionalGrain>(0);
            var coordinator = GrainFactory.GetGrain<ITransactionCoordinatorGrain>(0);

            try
            {
                if (scoping == TransactionScoping.Explicit)
                    await coordinator.ExplicitlyScopedExceptionThrowingTransaction(grain);
                else
                    await coordinator.ExceptionThrowingTransaction(grain);

                Assert.True(false);
            }
            catch (Exception)
            {
            }

            Assert.Equal(0, await grain.GetLatest());
        }


        [Theory, TestCategory("Transactions")]
        [InlineData(TransactionScoping.Attributes)]
        [InlineData(TransactionScoping.Explicit)]
        public async Task AbortTransactionOnOrphanCallsTest(TransactionScoping scoping)
        {
            var grain = GrainFactory.GetGrain<ISimpleTransactionalGrain>(0);
            var coordinator = GrainFactory.GetGrain<ITransactionCoordinatorGrain>(0);

            try
            {
                if (scoping == TransactionScoping.Explicit)
                    await coordinator.ExplicitlyScopedOrphanCallTransaction(grain);
                else
                    await coordinator.OrphanCallTransaction(grain);
            }
            catch (OrleansOrphanCallException)
            {
                Assert.Equal(0, await grain.GetLatest());
                return;
            }
            Assert.True(false);

        }

        [Theory, TestCategory("Transactions")]
        [InlineData(TransactionScoping.Attributes)]
        [InlineData(TransactionScoping.Explicit)]
        public async Task AbortReadOnlyTransactionOnWriteTest(TransactionScoping scoping)
        {
            var grain = GrainFactory.GetGrain<ISimpleTransactionalGrain>(0);
            var coordinator = GrainFactory.GetGrain<ITransactionCoordinatorGrain>(0);

            try
            {
                if (scoping == TransactionScoping.Explicit)
                    await coordinator.ExplicitlyScopedWriteInReadOnlyTransaction(grain);
                else
                    await coordinator.WriteInReadOnlyTransaction(grain);
            }
            catch (OrleansReadOnlyViolatedException)
            {
                return;
            }
            Assert.True(false, "Write operation should cause ReadOnlyViolatedException");
        }

        [Fact, TestCategory("Transactions")]
        public async Task NestedScopes()
        {
            List<ISimpleTransactionalGrain> grains = new List<ISimpleTransactionalGrain>();

            for (int i = 0; i < 3; i++)
            {
                grains.Add(GrainFactory.GetGrain<ISimpleTransactionalGrain>(i));
            }

            ITransactionCoordinatorGrain coordinator = GrainFactory.GetGrain<ITransactionCoordinatorGrain>(0);

            await coordinator.NestedScopes(grains, 5);

            for (int i = 0; i < 3; i++)
            {
                Assert.Equal(5, await grains[i].GetLatest());
            }
        }


        [Fact, TestCategory("Transactions")]
        public async Task MultiGrainReadOnlyTransaction()
        {
            List<ISimpleTransactionalGrain> grains = new List<ISimpleTransactionalGrain>();

            for (int i = 0; i < 3; i++)
            {
                grains.Add(GrainFactory.GetGrain<ISimpleTransactionalGrain>(i));
            }

            ITransactionCoordinatorGrain coordinator = GrainFactory.GetGrain<ITransactionCoordinatorGrain>(0);

            var value = await coordinator.ExplicitlyScopedReadOnlyTransaction(grains);

            Assert.Equal(0, value);

        }

        [Fact, TestCategory("Transactions")]
        public async Task NestedScopesInnerAbort()
        {
            var grain = GrainFactory.GetGrain<ISimpleTransactionalGrain>(0);
            var coordinator = GrainFactory.GetGrain<ITransactionCoordinatorGrain>(0);

            try
            {
                await coordinator.NestedScopesInnerAbort(grain);

                Assert.True(false);
            }
            catch (Exception)
            {
            }

            Assert.Equal(0, await grain.GetLatest());
        }

       
    }
}
