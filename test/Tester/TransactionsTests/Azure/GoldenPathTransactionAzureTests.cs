
using System.Threading.Tasks;
using Orleans.Runtime.Configuration;
using Orleans.TestingHost;
using Test.TransactionsTests;
using TestExtensions;
using UnitTests.Grains;
using Xunit;
using Xunit.Abstractions;

namespace Tester.TransactionsTests
{
    [TestCategory("Functional"), TestCategory("Transactions"), TestCategory("Azure")]
    public class GoldenPathTransactionAzureTests : OrleansTestingBase, IClassFixture<GoldenPathTransactionAzureTests.Fixture>
    {
        private readonly GoldenPathTransactionTestRunner goldenPathTestRunner;

        public class Fixture : BaseAzureTestClusterFixture
        {
            protected override TestCluster CreateTestCluster()
            {
                var options = new TestClusterOptions();
                options.ClusterConfiguration.AddAzureTableStorageProvider(TransactionTestConstants.TransactionStore, TestDefaultConfiguration.DataConnectionString);
                return new TestCluster(options);
            }
        }

        public GoldenPathTransactionAzureTests(Fixture fixture, ITestOutputHelper output)
        {
            fixture.EnsurePreconditionsMet();
            this.goldenPathTestRunner = new GoldenPathTransactionTestRunner(fixture.GrainFactory, output);
        }

        [SkippableFact]
        public Task SingleGrainReadTransaction()
        {
            return goldenPathTestRunner.SingleGrainReadTransaction();
        }

        [SkippableFact]
        public Task SingleGrainWriteTransaction()
        {
            return goldenPathTestRunner.SingleGrainWriteTransaction();
        }

        [SkippableFact]
        public Task MultiGrainWriteTransaction()
        {
            return goldenPathTestRunner.MultiGrainWriteTransaction();
        }

        [SkippableFact]
        public Task MultiGrainReadWriteTransaction()
        {
            return goldenPathTestRunner.MultiGrainReadWriteTransaction();
        }

        [SkippableFact]
        public Task MultiWriteToSingleGrainTransaction()
        {
            return goldenPathTestRunner.MultiWriteToSingleGrainTransaction();
        }
    }
}
