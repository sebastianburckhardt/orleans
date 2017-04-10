
using System.Threading.Tasks;
using Orleans.Runtime.Configuration;
using Orleans.TestingHost;
using Test.TransactionsTests;
using TestExtensions;
using Xunit;
using Xunit.Abstractions;

namespace Tester.TransactionsTests
{
    [TestCategory("Functional"), TestCategory("Transactions"), TestCategory("Azure")]
    public class GoldenPathTransactionAzureTests : OrleansTestingBase, IClassFixture<GoldenPathTransactionAzureTests.Fixture>
    {
        private readonly GoldenPathSingleStateTransactionTestRunner singleStateRunner;

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
            this.singleStateRunner = new GoldenPathSingleStateTransactionTestRunner(fixture.GrainFactory, output);
        }

        [SkippableFact]
        public Task SingleGrainReadTransaction()
        {
            return singleStateRunner.SingleGrainReadTransaction();
        }

        [SkippableFact]
        public Task SingleGrainWriteTransaction()
        {
            return singleStateRunner.SingleGrainWriteTransaction();
        }

        [SkippableFact]
        public Task MultiGrainWriteTransaction()
        {
            return singleStateRunner.MultiGrainWriteTransaction();
        }

        [SkippableFact]
        public Task MultiGrainReadWriteTransaction()
        {
            return singleStateRunner.MultiGrainReadWriteTransaction();
        }

        [SkippableFact]
        public Task MultiWriteToSingleGrainTransaction()
        {
            return singleStateRunner.MultiWriteToSingleGrainTransaction();
        }
    }
}
