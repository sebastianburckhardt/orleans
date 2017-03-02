
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Orleans.Transactions
{
    internal class InClusterTransactionManager : TransactionManagerBase
    {
        private readonly InterlockedExchangeLock dependencyLock;
        private readonly InterlockedExchangeLock commitLock;
        private readonly InterlockedExchangeLock checkpointLock;
        private readonly Dictionary<ITransactionalGrain, long> grains;
        private readonly List<Transaction> transactions;

        public InClusterTransactionManager(TransactionsConfiguration config)
            : base(config)
        {
            this.dependencyLock = new InterlockedExchangeLock();
            this.commitLock = new InterlockedExchangeLock();
            this.checkpointLock = new InterlockedExchangeLock();
            this.grains = new Dictionary<ITransactionalGrain, long>();
            this.transactions = new List<Transaction>();
        }

        protected override void BeginDependencyCompletionLoop()
        {
            BeginDependencyCompletionLoopAsync().Ignore();
        }

        protected override void BeginGroupCommitLoop()
        {
            BeginGroupCommitLoopAsync().Ignore();
        }

        protected override void BeginCheckpointLoop()
        {
            BeginCheckpointLoopAsync().Ignore();
        }

        protected override void SignalDependencyEnqueued()
        {
            BeginDependencyCompletionLoop();
        }

        protected override void SignalGroupCommitEnqueued()
        {
            BeginGroupCommitLoop();
        }

        protected override void SignalCheckpointEnqueued()
        {
            BeginCheckpointLoop();
        }

        private async Task BeginDependencyCompletionLoopAsync()
        {
            bool gotLock = false;
            try
            {
                if (!(gotLock = dependencyLock.TryGetLock()))
                {
                    return;
                }

                while (this.CheckDependenciesCompleted())
                {
                    // force yield thread
                    await Task.Delay(TimeSpan.FromTicks(1));
                }
            }
            finally
            {
                if (gotLock)
                    dependencyLock.ReleaseLock();
            }
        }

        private async Task BeginGroupCommitLoopAsync()
        {
            bool gotLock = false;
            try
            {
                if (!(gotLock = commitLock.TryGetLock()))
                {
                    return;
                }

                while (this.GroupCommit())
                {
                    // force yield thread
                    await Task.Delay(TimeSpan.FromTicks(1));
                }
            }
            finally
            {
                if (gotLock)
                    commitLock.ReleaseLock();
            }
        }

        private async Task BeginCheckpointLoopAsync()
        {
            bool gotLock = false;
            try
            {
                if (!(gotLock = checkpointLock.TryGetLock()))
                {
                    return;
                }

                while (await this.Checkpoint(grains, transactions))
                {
                }
            }
            finally
            {
                if (gotLock)
                    checkpointLock.ReleaseLock();
            }
        }
    }
}
