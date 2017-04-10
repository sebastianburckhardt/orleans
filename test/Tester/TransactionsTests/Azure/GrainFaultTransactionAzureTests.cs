
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
        private readonly GrainFaultSingleStateTransactionTestRunner singleStateRunner;

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
            this.singleStateRunner = new GrainFaultSingleStateTransactionTestRunner(fixture.GrainFactory, output);
        }

        [SkippableFact]
        public Task AbortTransactionOnExceptions()
        {
            return singleStateRunner.AbortTransactionOnExceptions();
        }

        [SkippableFact]
        public Task MultiGrainAbortTransactionOnExceptions()
        {
            return singleStateRunner.MultiGrainAbortTransactionOnExceptions();
        }

        [SkippableFact]
        public Task AbortTransactionOnOrphanCalls()
        {
            return singleStateRunner.AbortTransactionOnOrphanCalls();
        }
    }
}
