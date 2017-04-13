
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
        private readonly GoldenPathTransactionTestRunner goldenPathTestRunner;

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
            this.goldenPathTestRunner = new GoldenPathTransactionTestRunner(fixture.GrainFactory, output);
        }

        [Fact]
        public Task SingleState_SingleGrainReadTransaction()
        {
            return goldenPathTestRunner.SingleGrainReadTransaction();
        }

        [Fact]
        public Task SingleState_SingleGrainWriteTransaction()
        {
            return goldenPathTestRunner.SingleGrainWriteTransaction();
        }

        [Fact]
        public Task SingleState_MultiGrainWriteTransaction()
        {
            return goldenPathTestRunner.MultiGrainWriteTransaction();
        }

        [Fact]
        public Task SingleState_MultiGrainReadWriteTransaction()
        {
            return goldenPathTestRunner.MultiGrainReadWriteTransaction();
        }

        [Fact]
        public Task SingleState_MultiWriteToSingleGrainTransaction()
        {
            return goldenPathTestRunner.MultiWriteToSingleGrainTransaction();
        }
    }
}
