﻿using System;
using System.Threading;
using Orleans.Runtime;

namespace Orleans.Transactions
{
    internal class ActiveTransactionsTracker
    {
        private readonly TransactionsConfiguration config;
        private readonly TransactionLog log;
        private readonly Logger logger;
        private readonly object lockObj;

        private long smallestActiveTransactionId;
        private long highestActiveTransactionId;

        private long maxAllocatedTransactionId;
        private readonly Thread allocationThread;
        private readonly AutoResetEvent allocationEvent;

        public ActiveTransactionsTracker(TransactionsConfiguration config, TransactionLog log, Factory<string, Logger> logFactory)
        {
            this.config = config;
            this.log = log;
            this.logger = logFactory(nameof(ActiveTransactionsTracker));
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
            return Interlocked.Read(ref smallestActiveTransactionId);
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
            Interlocked.Increment(ref smallestActiveTransactionId);
        }

        private void AllocateTransactionId(object args)
        {
            while (true)
            {
                try
                {
                    allocationEvent.WaitOne();

                    lock (lockObj)
                    {
                        if (maxAllocatedTransactionId - highestActiveTransactionId <= config.AvailableTransactionIdThreshold)
                        {
                            var batchSize = config.TransactionIdAllocationBatchSize;
                            log.UpdateStartRecord(maxAllocatedTransactionId + batchSize).GetAwaiter().GetResult();

                            maxAllocatedTransactionId += batchSize;
                        }
                    }
                }
                catch (Exception exception)
                {
                    this.logger.Warn(
                        ErrorCode.Transactions_IdAllocationFailed,
                        "Ignoring exception in " + nameof(this.AllocateTransactionId),
                        exception);
                }
            }
        }
    }
}
