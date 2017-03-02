
namespace Orleans.Runtime
{
    internal class TransactionsStatisticsGroup
    {
        internal static CounterStatistic StartTransactionQueueLength;
        internal static CounterStatistic StartTransactionRequests;
        internal static CounterStatistic StartTransactionCompleted;

        internal static CounterStatistic CommitTransactionQueueLength;
        internal static CounterStatistic CommitTransactionRequests;
        internal static CounterStatistic CommitTransactionCompleted;

        internal static CounterStatistic TransactionsInDoubt;

        internal static CounterStatistic AbortedTransactionsTotal;

        internal static void Init()
        {
            StartTransactionQueueLength = CounterStatistic.FindOrCreate(StatisticNames.TRANSACTIONS_START_QUEUE_LENGTH, false);
            StartTransactionRequests = CounterStatistic.FindOrCreate(StatisticNames.TRANSACTIONS_START_REQUEST);
            StartTransactionCompleted = CounterStatistic.FindOrCreate(StatisticNames.TRANSACTIONS_START_COMPLETED);

            CommitTransactionQueueLength = CounterStatistic.FindOrCreate(StatisticNames.TRANSACTIONS_COMMIT_QUEUE_LENGTH, false);
            CommitTransactionRequests = CounterStatistic.FindOrCreate(StatisticNames.TRANSACTIONS_COMMIT_REQUEST);
            CommitTransactionCompleted = CounterStatistic.FindOrCreate(StatisticNames.TRANSACTIONS_COMMIT_COMPLETED);

            TransactionsInDoubt = CounterStatistic.FindOrCreate(StatisticNames.TRANSACTIONS_COMMIT_IN_DOUBT);

            AbortedTransactionsTotal = CounterStatistic.FindOrCreate(StatisticNames.TRANSACTIONS_ABORT_TOTAL);
        }

        internal static void OnTransactionStartRequest()
        {
            StartTransactionQueueLength.Increment();
            StartTransactionRequests.Increment();
        }

        internal static void OnTransactionStarted()
        {
            StartTransactionQueueLength.DecrementBy(1);
            StartTransactionCompleted.Increment();
        }

        internal static void OnTransactionStartFailed()
        {
            StartTransactionQueueLength.DecrementBy(1);
        }

        internal static void OnTransactionCommitRequest()
        {
            CommitTransactionQueueLength.Increment();
            CommitTransactionRequests.Increment();
        }

        internal static void OnTransactionCommitted()
        {
            CommitTransactionQueueLength.DecrementBy(1);
            CommitTransactionCompleted.Increment();
        }

        internal static void OnTransactionInDoubt()
        {
            CommitTransactionQueueLength.DecrementBy(1);
            TransactionsInDoubt.Increment();
        }

        internal static void OnTransactionAborted()
        {
            CommitTransactionQueueLength.DecrementBy(1);
            AbortedTransactionsTotal.Increment();
        }

    }
}
