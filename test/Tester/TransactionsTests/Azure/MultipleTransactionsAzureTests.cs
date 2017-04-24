using Orleans.Runtime.Configuration;
using Orleans.TestingHost;
using Orleans.Transactions;
using Test.TransactionsTests;
using TestExtensions;
using Xunit;
using Xunit.Abstractions;

namespace Tester.TransactionsTests
{
    [TestCategory("BVT"), TestCategory("Transactions")]
    public class MultipleTransactionAzureTests : MultipleTransactionsTestRunner, IClassFixture<GoldenPathTransactionAzureTests.Fixture>
    {
         
        public MultipleTransactionAzureTests(GoldenPathTransactionAzureTests.Fixture fixture, ITestOutputHelper output)
            : base(fixture.GrainFactory, output)
        {
        } 
    }
}
