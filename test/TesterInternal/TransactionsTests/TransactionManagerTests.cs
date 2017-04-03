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
        public void StartCommitTransactionTest()
        {
            var id = tm.StartTransaction(TimeSpan.FromMinutes(1));
            var info = new TransactionInfo(id);
            tm.CommitTransaction(info);
            WaitForTransactionCommit(id);
        }

        
        [Fact, TestCategory("Transactions")]
        public async Task TransactionTimeoutTest()
        {
            var id = tm.StartTransaction(TimeSpan.FromSeconds(1));
            var info = new TransactionInfo(id);
            Thread.Sleep(3000);

            try
            {
                tm.CommitTransaction(info);
                WaitForTransactionCommit(id);
                Assert.True(false, "Transaction commit succeeded when it should have timed out");
            }
            catch (OrleansTransactionAbortedException e)
            {
                Assert.True((e is OrleansTransactionTimeoutException) || e.Message == "Transaction presumed to be aborted");
            }
        }

        [Fact, TestCategory("Transactions")]
        public void DependentTransactionTest()
        {
            var id1 = tm.StartTransaction(TimeSpan.FromMinutes(1));
            var id2 = tm.StartTransaction(TimeSpan.FromMinutes(2));

            var info = new TransactionInfo(id1);
            tm.CommitTransaction(info);
            WaitForTransactionCommit(id1);

            var info2 = new TransactionInfo(id2);
            info2.DependentTransactions.Add(id1);
            tm.CommitTransaction(info2);
            WaitForTransactionCommit(id2);
        }

        [Fact, TestCategory("Transactions")]
        public void OutOfOrderCommitTransactionTest()
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

            WaitForTransactionCommit(id2);
        }

        [Fact, TestCategory("Transactions")]
        public async Task CascadingAbortTransactionTest()
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
                WaitForTransactionCommit(id2);
                Assert.True(false, "Transaction was not aborted");
            }
            catch (OrleansCascadingAbortException e)
            {
                Assert.True(e.TransactionId == id2);
                Assert.True(e.DependentTransactionId == id1);
            }
        }

        private void WaitForTransactionCommit(long transactionId)
        {
            while (true)
            {
                OrleansTransactionAbortedException e;
                var result = tm.GetTransactionStatus(transactionId, out e);
                if (result == TransactionStatus.Committed)
                {
                    return;
                }
                else if (result == TransactionStatus.Aborted)
                {
                    throw e;
                }
                else if (result == TransactionStatus.Unknown)
                {
                    throw new OrleansTransactionInDoubtException(transactionId);
                }
                Assert.True(result == TransactionStatus.InProgress);
                Thread.Sleep(100);
            }
        }
    }
}
