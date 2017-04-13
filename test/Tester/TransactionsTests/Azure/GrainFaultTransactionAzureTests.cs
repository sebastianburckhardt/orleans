
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
    public class GrainFaultTransactionAzureTests : OrleansTestingBase, IClassFixture<GrainFaultTransactionAzureTests.Fixture>
    {
        private readonly GrainFaultTransactionTestRunner grainFaultTestRunner;

        public class Fixture : BaseAzureTestClusterFixture
        {
            protected override TestCluster CreateTestCluster()
            {
                var options = new TestClusterOptions();
                options.ClusterConfiguration.AddAzureTableStorageProvider(TransactionTestConstants.TransactionStore, TestDefaultConfiguration.DataConnectionString);
                return new TestCluster(options);
            }
        }

        public GrainFaultTransactionAzureTests(Fixture fixture, ITestOutputHelper output)
        {
            fixture.EnsurePreconditionsMet();
            this.grainFaultTestRunner = new GrainFaultTransactionTestRunner(fixture.GrainFactory, output);
        }

        [SkippableFact]
        public Task AbortTransactionOnExceptions()
        {
            return grainFaultTestRunner.AbortTransactionOnExceptions();
        }

        [SkippableFact]
        public Task MultiGrainAbortTransactionOnExceptions()
        {
            return grainFaultTestRunner.MultiGrainAbortTransactionOnExceptions();
        }

        [SkippableFact]
        public Task AbortTransactionOnOrphanCalls()
        {
            return grainFaultTestRunner.AbortTransactionOnOrphanCalls();
        }
    }
}
