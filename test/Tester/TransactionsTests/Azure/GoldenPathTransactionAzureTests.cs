using Orleans.Runtime.Configuration;
using Orleans.TestingHost;
using Test.TransactionsTests;
using TestExtensions;
using Xunit;
using Xunit.Abstractions;

namespace Tester.TransactionsTests
{
    [TestCategory("Functional"), TestCategory("Transactions"), TestCategory("Azure")]
    public class GoldenPathTransactionAzureTests : GoldenPathTransactionTestRunner, IClassFixture<GoldenPathTransactionAzureTests.Fixture>
    {
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
            : base(fixture.GrainFactory, output)
        {
            fixture.EnsurePreconditionsMet();
        }
    }
}
