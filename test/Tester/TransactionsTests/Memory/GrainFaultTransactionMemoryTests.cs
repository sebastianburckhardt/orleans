
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
        private readonly GrainFaultSingleStateTransactionTestRunner singleStateRunner;

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
            this.singleStateRunner = new GrainFaultSingleStateTransactionTestRunner(fixture.GrainFactory, output);
        }

        [Fact]
        public Task AbortTransactionOnExceptions()
        {
            return singleStateRunner.AbortTransactionOnExceptions();
        }

        [Fact]
        public Task MultiGrainAbortTransactionOnExceptions()
        {
            return singleStateRunner.MultiGrainAbortTransactionOnExceptions();
        }

        [Fact]
        public Task AbortTransactionOnOrphanCalls()
        {
            return singleStateRunner.AbortTransactionOnOrphanCalls();
        }
    }
}
