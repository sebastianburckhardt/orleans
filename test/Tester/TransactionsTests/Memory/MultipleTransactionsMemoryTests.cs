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
    public class MultipleTransactionMemoryTests : MultipleTransactionsTestRunner, IClassFixture<GoldenPathTransactionMemoryTests.Fixture>
    {
         
        public MultipleTransactionMemoryTests(GoldenPathTransactionMemoryTests.Fixture fixture, ITestOutputHelper output)
            : base(fixture.GrainFactory, output)
        {
        } 
    }
}
