
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
    public class GoldenPathTransactionMemoryTests : OrleansTestingBase, IClassFixture<GoldenPathTransactionMemoryTests.Fixture>
    {
        private readonly GoldenPathSingleStateTransactionTestRunner singleStateRunner;

        public class Fixture : BaseTestClusterFixture
        {
            protected override TestCluster CreateTestCluster()
            {
                var options = new TestClusterOptions();
                options.ClusterConfiguration.AddMemoryStorageProvider(TransactionTestConstants.TransactionStore);
                return new TestCluster(options);
            }
        }

        public GoldenPathTransactionMemoryTests(Fixture fixture, ITestOutputHelper output)
        {
            this.singleStateRunner = new GoldenPathSingleStateTransactionTestRunner(fixture.GrainFactory, output);
        }

        [Fact]
        public Task SingleGrainReadTransaction()
        {
            return singleStateRunner.SingleGrainReadTransaction();
        }

        [Fact]
        public Task SingleGrainWriteTransaction()
        {
            return singleStateRunner.SingleGrainWriteTransaction();
        }

        [Fact]
        public Task MultiGrainWriteTransaction()
        {
            return singleStateRunner.MultiGrainWriteTransaction();
        }

        [Fact]
        public Task MultiGrainReadWriteTransaction()
        {
            return singleStateRunner.MultiGrainReadWriteTransaction();
        }

        [Fact]
        public Task MultiWriteToSingleGrainTransaction()
        {
            return singleStateRunner.MultiWriteToSingleGrainTransaction();
        }
    }
}
