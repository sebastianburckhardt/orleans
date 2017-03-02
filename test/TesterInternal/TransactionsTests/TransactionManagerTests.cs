using System;
using System.Threading.Tasks;
using Orleans.Transactions;
using System.Threading;
using Xunit;

namespace UnitTests.TransactionsTests
{
    /// <summary>
    /// Tests for operation of Orleans Transaction Manager
    /// </summary>
    public class TransactionManagerTests
    {
        private ITransactionManager tm;

        public TransactionManagerTests()
        {
            tm = new TransactionManager(new TransactionsConfiguration());
            CancellationTokenSource cts = new CancellationTokenSource();
            tm.StartAsync().Wait(cts.Token);
        }

        [Fact, TestCategory("Transactions")]
        public async Task StartCommitTransactionTest()
        {
            var id = tm.StartTransaction(TimeSpan.FromMinutes(1));
            var info = new TransactionInfo(id);
            await tm.CommitTransaction(info);
        }

        [Fact, TestCategory("Transactions")]
        public async Task TransactionTimeoutTest()
        {
            var id = tm.StartTransaction(TimeSpan.FromSeconds(1));
            var info = new TransactionInfo(id);
            Thread.Sleep(3000);

            try
            {
                await tm.CommitTransaction(info);
                Assert.True(false, "Transaction commit succeeded when it should have timed out");
            }
            catch (OrleansTransactionAbortedException e)
            {
                Assert.True((e is OrleansTransactionTimeoutException) || e.Message == "Transaction presumed to be aborted");
            }
        }

        [Fact, TestCategory("Transactions")]
        public async Task DependentTransactionTest()
        {
            var id1 = tm.StartTransaction(TimeSpan.FromMinutes(1));
            var id2 = tm.StartTransaction(TimeSpan.FromMinutes(2));

            var info = new TransactionInfo(id1);
            await tm.CommitTransaction(info);

            var info2 = new TransactionInfo(id2);
            info2.DependentTransactions.Add(id1);
            await tm.CommitTransaction(info2);
        }

        [Fact, TestCategory("Transactions")]
        public async Task OutOfOrderCommitTransactionTest()
        {
            var id1 = tm.StartTransaction(TimeSpan.FromMinutes(1));
            var id2 = tm.StartTransaction(TimeSpan.FromMinutes(2));

            var info2 = new TransactionInfo(id2);
            info2.DependentTransactions.Add(id1);
            Task t = tm.CommitTransaction(info2);
            Assert.False(t.IsCompleted);

            var info = new TransactionInfo(id1);
            await tm.CommitTransaction(info);

            await t;
        }

        [Fact, TestCategory("Transactions")]
        public async Task CascadingAbortTransactionTest()
        {
            var id1 = tm.StartTransaction(TimeSpan.FromMinutes(1));
            var id2 = tm.StartTransaction(TimeSpan.FromMinutes(2));

            var info2 = new TransactionInfo(id2);
            info2.DependentTransactions.Add(id1);
            Task t = tm.CommitTransaction(info2);
            Assert.False(t.IsCompleted);

            tm.AbortTransaction(id1, new OrleansTransactionAbortedException(id1));

            try
            {
                await t;
                Assert.True(false, "Transaction was not aborted");
            }
            catch (OrleansCascadingAbortException e)
            {
                Assert.True(e.TransactionId == id2);
                Assert.True(e.DependentTransactionId == id1);
            }
        }
    }
}
