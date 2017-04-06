
using System;
using System.Collections.Generic;
using System.Threading;
using Orleans.Runtime;

namespace Orleans.Transactions
{
    public class TransactionManager : TransactionManagerBase
    {
        private readonly Thread dependencyThread;
        private readonly AutoResetEvent dependencyEvent;

        private readonly Thread commitThread;
        private readonly AutoResetEvent commitEvent;

        private readonly Thread checkpointThread;
        private readonly AutoResetEvent checkpointEvent;

        public TransactionManager(TransactionsConfiguration config)
            : base(config)
        {
            dependencyEvent = new AutoResetEvent(false);
            commitEvent = new AutoResetEvent(false);
            checkpointEvent = new AutoResetEvent(false);

            dependencyThread = new Thread(DependencyCompletionLoop);
            commitThread = new Thread(GroupCommitLoop);
            checkpointThread = new Thread(CheckpointLoop);
        }

        protected override void BeginDependencyCompletionLoop()
        {
            dependencyThread.Start();
        }

        protected override void BeginGroupCommitLoop()
        {
            commitThread.Start();
        }

        protected override void BeginCheckpointLoop()
        {
            checkpointThread.Start();
        }

        protected override void SignalDependencyEnqueued()
        {
            dependencyEvent.Set();
        }

        protected override void SignalGroupCommitEnqueued()
        {
            commitEvent.Set();
        }

        protected override void SignalCheckpointEnqueued()
        {
            checkpointEvent.Set();
        }

        private void DependencyCompletionLoop()
        {
            while (true)
            {
                try
                {
                    dependencyEvent.WaitOne();
                    base.CheckDependenciesCompleted();
                }
                catch (Exception exception)
                {
                    this.Logger.Warn(ErrorCode.Transactions_TMError, "Ignoring exception in " + nameof(this.DependencyCompletionLoop), exception);
                }
            }
        }

        private void GroupCommitLoop()
        {
            while (true)
            {
                try
                {
                    commitEvent.WaitOne();
                    base.GroupCommit();
                }
                catch (Exception exception)
                {
                    this.Logger.Warn(ErrorCode.Transactions_TMError, "Ignoring exception in " + nameof(this.GroupCommitLoop), exception);
                }
            }
        }

        private void CheckpointLoop()
        {
            Dictionary<ITransactionalResource, long> resources = new Dictionary<ITransactionalResource, long>();
            List<Transaction> transactions = new List<Transaction>();
            while (true)
            {
                try
                {
                    // Maybe impose a max per batch to decrease latency?
                    checkpointEvent.WaitOne();
                    base.Checkpoint(resources, transactions).Wait();
                }
                catch(Exception exception)
                {
                    this.Logger.Warn(ErrorCode.Transactions_TMError, "Ignoring exception in " + nameof(this.CheckpointLoop), exception);
                }
            }
        }
    }
}
