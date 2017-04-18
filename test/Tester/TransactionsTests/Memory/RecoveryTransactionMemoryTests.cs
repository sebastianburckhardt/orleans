
using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Runtime.Configuration;
using Orleans.TestingHost;
using Orleans.Transactions;
using Test.TransactionsTests;
using TestExtensions;
using Xunit;
using Xunit.Abstractions;

namespace Tester.TransactionsTests
{
    [TestCategory("BVT"), TestCategory("Transactions")]
    public class RecoveryTransactionMemoryTests : OrleansTestingBase, IClassFixture<RecoveryTransactionMemoryTests.Fixture>
    {
        private readonly GrainDeactivationTransactionTestRunner grainDeactivationTestRunner;

        public class Fixture : BaseTestClusterFixture
        {
            protected override TestCluster CreateTestCluster()
            {
                var options = new TestClusterOptions();
                options.ClusterConfiguration.UseStartupType<TestStartup>();
                options.ClusterConfiguration.AddMemoryStorageProvider(TransactionTestConstants.TransactionStore);
                options.ClusterConfiguration.UseMemoryTransactionLog();
                return new TestCluster(options);
            }
        }

        public RecoveryTransactionMemoryTests(Fixture fixture, ITestOutputHelper output)
        {
            this.grainDeactivationTestRunner = new GrainDeactivationTransactionTestRunner(fixture.GrainFactory, output);
        }

        [Fact]
        public Task SingleState_SingleGrainReadTransaction()
        {
            return grainDeactivationTestRunner.SingleGrainReadTransaction();
        }

        [Fact]
        public Task SingleState_SingleGrainWriteTransaction()
        {
            return grainDeactivationTestRunner.SingleGrainWriteTransaction();
        }

        [Fact]
        public Task SingleState_MultiGrainWriteTransaction_DeactivateAfterCall()
        {
            return grainDeactivationTestRunner.MultiGrainWriteTransaction_DeactivateAfterCall();
        }

        [Fact]
        public Task SingleState_MultiGrainWriteTransaction_DeactivateAfterPrepare()
        {
            return grainDeactivationTestRunner.MultiGrainWriteTransaction_DeactivateAfterPerpare();
        }

        [Fact]
        public Task SingleState_MultiGrainWriteTransaction_DeactivateAfterCommit()
        {
            return grainDeactivationTestRunner.MultiGrainWriteTransaction_DeactivateAfterCommit();
        }

        private class TestStartup
        {
            public IServiceProvider ConfigureServices(IServiceCollection services)
            {
                services.AddTransient(typeof(IDeactivatingTransactionState<>), typeof(DeactivatingTransactionState<>));
                return services.BuildServiceProvider();
            }
        }
    }
}
