using System;
using System.Threading;

namespace Orleans.Transactions
{
    class ActiveTransactionsTracker
    {
        private TransactionsConfiguration config;
        private TransactionLog log;

        private long smallestActiveTransactionId;
        private long highestActiveTransactionId;

        private object lockObj;

        private long maxAllocatedTransactionId;
        private Thread allocationThread;
        private AutoResetEvent allocationEvent;

        public ActiveTransactionsTracker(TransactionsConfiguration config, TransactionLog log)
        {
            this.config = config;
            this.log = log;
            lockObj = new object();
            allocationEvent = new AutoResetEvent(true);

            allocationThread = new Thread(AllocateTransactionId);
        }

        public void Start(long initialTransactionId)
        {
            smallestActiveTransactionId = initialTransactionId + 1;
            highestActiveTransactionId = initialTransactionId;
            maxAllocatedTransactionId = initialTransactionId;

            allocationEvent.Set();
            allocationThread.Start();
        }

        public long GetNewTransactionId()
        {
            var id = Interlocked.Increment(ref highestActiveTransactionId);

            if (maxAllocatedTransactionId - highestActiveTransactionId <= config.AvailableTransactionIdThreshold)
            {
                // Signal the allocation thread to allocate more Ids
                allocationEvent.Set();
            }

            while (id > maxAllocatedTransactionId)
            {
                // Wait until the allocation thread catches up before returning.
                // This should never happen if we are pre-allocating fast enough.
                allocationEvent.Set();
                lock (lockObj)
                {
                }
            }

            return id;
        }

        public long GetSmallestActiveTransactionId()
        {
            // NOTE: this result is not strictly correct if there are NO active transactions
            // but for all purposes in which this is used it is still valid.
            // TODO: consider renaming this or handling the no active transactions case.
            lock (lockObj)
            {
                return smallestActiveTransactionId;
            }
        }

        public long GetHighestActiveTransactionId()
        {
            // NOTE: this result is not strictly correct if there are NO active transactions
            // but for all purposes in which this is used it is still valid.
            // TODO: consider renaming this or handling the no active transactions case.
            lock (lockObj)
            {
                return Math.Min(highestActiveTransactionId, maxAllocatedTransactionId);
            }
        }


        public void PopSmallestActiveTransactionId()
        {
            lock (lockObj)
            {
                smallestActiveTransactionId++;
            }
        }

        private void AllocateTransactionId(object args)
        {
            while (true)
            {
                allocationEvent.WaitOne();

                lock (lockObj)
                {
                    if (maxAllocatedTransactionId - highestActiveTransactionId <= config.AvailableTransactionIdThreshold)
                    {
                        var batchSize = config.TransactionIdAllocationBatchSize;
                        log.UpdateStartRecord(maxAllocatedTransactionId + batchSize).Wait();

                        maxAllocatedTransactionId += batchSize;
                    }
                }
            }
        }
    }
}
