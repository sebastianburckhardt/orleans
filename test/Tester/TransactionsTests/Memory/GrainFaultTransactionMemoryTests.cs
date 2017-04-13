
using System.Threading.Tasks;
using Orleans.Runtime.Configuration;
using Orleans.TestingHost;
using Test.TransactionsTests;
using TestExtensions;
using Xunit;
using Xunit.Abstractions;

namespace Tester.TransactionsTests
{
    [TestCategory("BVT"), TestCategory("Transactions")]
    public class GrainFaultTransactionMemoryTests : OrleansTestingBase, IClassFixture<GrainFaultTransactionMemoryTests.Fixture>
    {
        private readonly GrainFaultTransactionTestRunner grainFaultTestRunner;

        public class Fixture : BaseTestClusterFixture
        {
            protected override TestCluster CreateTestCluster()
            {
                var options = new TestClusterOptions();
                options.ClusterConfiguration.AddMemoryStorageProvider(TransactionTestConstants.TransactionStore);
                return new TestCluster(options);
            }
        }

        public GrainFaultTransactionMemoryTests(Fixture fixture, ITestOutputHelper output)
        {
            this.grainFaultTestRunner = new GrainFaultTransactionTestRunner(fixture.GrainFactory, output);
        }

        [Fact]
        public Task AbortTransactionOnExceptions()
        {
            return grainFaultTestRunner.AbortTransactionOnExceptions();
        }

        [Fact]
        public Task MultiGrainAbortTransactionOnExceptions()
        {
            return grainFaultTestRunner.MultiGrainAbortTransactionOnExceptions();
        }

        [Fact]
        public Task AbortTransactionOnOrphanCalls()
        {
            return grainFaultTestRunner.AbortTransactionOnOrphanCalls();
        }
    }
}
