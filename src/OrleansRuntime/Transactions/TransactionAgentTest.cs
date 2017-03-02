using Orleans.Runtime;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Transactions
{
    internal class TransactionAgentTest : SystemTarget, ITransactionAgent, ITransactionAgentSystemTarget /*, ISiloShutdownParticipant*/
    {

        public long ReadOnlyTransactionId { get; private set; }

        public TransactionAgentTest(GrainId grain, SiloAddress currentSilo) : base(grain, currentSilo) { }

        public Task Start()
        {
            ReadOnlyTransactionId = 0;
            return TaskDone.Done;
        }

        public Task<TransactionInfo> StartTransaction(bool readOnly, TimeSpan timeout)
        {
            long id = Interlocked.Increment(ref currentTransactionId);
            var info = new TransactionInfo(id, readOnly);
            return Task.FromResult<TransactionInfo>(info);
        }

        public async Task Commit(TransactionInfo transactionInfo)
        {
            bool canCommit = true;
            int index = 0;
            Task<bool>[] prepareTasks = new Task<bool>[transactionInfo.WriteSet.Count];
            foreach (var g in transactionInfo.WriteSet.Keys)
            {
                GrainVersion write;
                write.TransactionId = transactionInfo.TransactionId;
                write.WriteNumber = transactionInfo.WriteSet[g];
                prepareTasks[index++] = g.Prepare(transactionInfo.TransactionId, write, null);
            }

            await Task.WhenAll(prepareTasks);
            foreach (var t in prepareTasks)
            {
                if (!t.Result)
                {
                    canCommit = false;
                }
            }

            if (!canCommit)
            {
                abortedTransactions.TryAdd(transactionInfo.TransactionId, 0);
                throw new OrleansPrepareFailedException(transactionInfo.TransactionId);
            }
        }

        public void Abort(TransactionInfo transactionInfo)
        {
            abortedTransactions.TryAdd(transactionInfo.TransactionId, 0);
        }

        public bool IsAborted(long transactionId)
        {
            return abortedTransactions.ContainsKey(transactionId);
        }


        private long currentTransactionId = 0;
        private ConcurrentDictionary<long, long> abortedTransactions = new ConcurrentDictionary<long, long>();

    }
}
