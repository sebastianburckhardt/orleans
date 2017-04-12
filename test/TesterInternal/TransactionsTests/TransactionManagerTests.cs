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
    [TestCategory("Transactions")]
    public class TransactionManagerTests
    {
        private ITransactionManager tm;

        public TransactionManagerTests()
        {
            // TODO: need to clean up the Transaction Manager after the test finishes
            tm = new TransactionManager(new TransactionsConfiguration());
            CancellationTokenSource cts = new CancellationTokenSource();
            tm.StartAsync().Wait(cts.Token);
        }

        [Fact]
        public async Task StartCommitTransaction()
        {
            var id = tm.StartTransaction(TimeSpan.FromMinutes(1));
            var info = new TransactionInfo(id);
            tm.CommitTransaction(info);
            await WaitForTransactionCommit(id);
        }

        
        [Fact]
        public async Task TransactionTimeout()
        {
            var id = tm.StartTransaction(TimeSpan.FromSeconds(1));
            var info = new TransactionInfo(id);
            await Task.Delay(3000);

            try
            {
                tm.CommitTransaction(info);
                await WaitForTransactionCommit(id);
                Assert.True(false, "Transaction commit succeeded when it should have timed out");
            }
            catch (OrleansTransactionAbortedException e)
            {
                Assert.True((e is OrleansTransactionTimeoutException) || e.Message == "Transaction presumed to be aborted");
            }
        }

        [Fact]
        public async Task DependentTransaction()
        {
            var id1 = tm.StartTransaction(TimeSpan.FromMinutes(1));
            var id2 = tm.StartTransaction(TimeSpan.FromMinutes(2));

            var info = new TransactionInfo(id1);
            tm.CommitTransaction(info);
            await WaitForTransactionCommit(id1);

            var info2 = new TransactionInfo(id2);
            info2.DependentTransactions.Add(id1);
            tm.CommitTransaction(info2);
            await WaitForTransactionCommit(id2);
        }

        [Fact]
        public async Task OutOfOrderCommitTransaction()
        {
            var id1 = tm.StartTransaction(TimeSpan.FromMinutes(1));
            var id2 = tm.StartTransaction(TimeSpan.FromMinutes(2));

            var info2 = new TransactionInfo(id2);
            info2.DependentTransactions.Add(id1);

            tm.CommitTransaction(info2);
            OrleansTransactionAbortedException e;
            Assert.True(tm.GetTransactionStatus(id2, out e) == TransactionStatus.InProgress);

            var info = new TransactionInfo(id1);
            tm.CommitTransaction(info);

            await WaitForTransactionCommit(id2);
        }

        [Fact]
        public async Task CascadingAbortTransaction()
        {
            var id1 = tm.StartTransaction(TimeSpan.FromMinutes(1));
            var id2 = tm.StartTransaction(TimeSpan.FromMinutes(2));

            var info2 = new TransactionInfo(id2);
            info2.DependentTransactions.Add(id1);

            tm.CommitTransaction(info2);
            OrleansTransactionAbortedException abort;
            Assert.True(tm.GetTransactionStatus(id2, out abort) == TransactionStatus.InProgress);

            tm.AbortTransaction(id1, new OrleansTransactionAbortedException(id1));

            try
            {
                await WaitForTransactionCommit(id2);
                Assert.True(false, "Transaction was not aborted");
            }
            catch (OrleansCascadingAbortException e)
            {
                Assert.True(e.TransactionId == id2);
                Assert.True(e.DependentTransactionId == id1);
            }
        }

        private async Task WaitForTransactionCommit(long transactionId)
        {
            TimeSpan timeout = TimeSpan.FromSeconds(15);

            var endTime = DateTime.UtcNow + timeout;
            while (DateTime.UtcNow < endTime)
            {
                OrleansTransactionAbortedException e;
                var result = tm.GetTransactionStatus(transactionId, out e);
                switch (result)
                {
                    case TransactionStatus.Committed:
                        return;
                    case TransactionStatus.Aborted:
                        throw e;
                    case TransactionStatus.Unknown:
                        throw new OrleansTransactionInDoubtException(transactionId);
                    default:
                        Assert.True(result == TransactionStatus.InProgress);
                        await Task.Delay(100);
                        break;
                }
            }

            throw new TimeoutException("Timed out waiting for the transaction to complete");
        }
    }
}
