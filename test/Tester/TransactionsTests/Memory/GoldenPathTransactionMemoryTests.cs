using Orleans.Runtime.Configuration;
using Orleans.TestingHost;
using Test.TransactionsTests;
using TestExtensions;
using Xunit;
using Xunit.Abstractions;

namespace Tester.TransactionsTests
{
    [TestCategory("BVT"), TestCategory("Transactions")]
    public class GoldenPathTransactionMemoryTests : GoldenPathTransactionTestRunner, IClassFixture<GoldenPathTransactionMemoryTests.Fixture>
    {
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
            : base(fixture.GrainFactory, output)
        {
        }
    }
}
