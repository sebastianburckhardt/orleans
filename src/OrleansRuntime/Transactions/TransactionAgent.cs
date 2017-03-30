

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Orleans.Runtime;
using Orleans.Runtime.Configuration;
using Orleans.Concurrency;

namespace Orleans.Transactions
{
    [Reentrant]
    internal class TransactionAgent : SystemTarget, ITransactionAgent, ITransactionAgentSystemTarget /*, ISiloShutdownParticipant*/
    {
        private readonly ITransactionServiceFactory serviceFactory;

        private ITransactionStartService tmStartProxy;
        private ITransactionCommitService tmCommitProxy;

        //private long abortSequenceNumber;
        private long abortLowerBound;
        private readonly ConcurrentDictionary<long, long> abortedTransactions;

        private readonly ConcurrentQueue<Tuple<TransactionInfo, TaskCompletionSource<bool>>> transactionCommitQueue;
        private readonly ConcurrentQueue<Tuple<TimeSpan, TaskCompletionSource<long>>> transactionStartQueue;

        private readonly Logger logger;

        private IGrainTimer requestProcessor;
        private Task startTransactionsTask = TaskDone.Done;
        private Task commitTransactionsTask = TaskDone.Done;

        public long ReadOnlyTransactionId { get; private set; }

        public TransactionAgent(ILocalSiloDetails siloDetails, ITransactionServiceFactory serviceFactory)
            : base(Constants.TransactionAgentSystemTargetId, siloDetails.SiloAddress)
        {
            logger = LogManager.GetLogger("TransactionAgent");
            this.serviceFactory = serviceFactory;
            tmStartProxy = null;
            tmCommitProxy = null;
            ReadOnlyTransactionId = 0;
            //abortSequenceNumber = 0;
            abortLowerBound = 0;


            abortedTransactions = new ConcurrentDictionary<long, long>();
            transactionCommitQueue = new ConcurrentQueue<Tuple<TransactionInfo, TaskCompletionSource<bool>>>();
            transactionStartQueue = new ConcurrentQueue<Tuple<TimeSpan, TaskCompletionSource<long>>>();
        }

        #region ITransactionAgent

        public async Task<TransactionInfo> StartTransaction(bool readOnly, TimeSpan timeout)
        {
            if (readOnly)
            {
                return new TransactionInfo(ReadOnlyTransactionId, true);
            }

            TransactionsStatisticsGroup.OnTransactionStartRequest();
            var completion = new TaskCompletionSource<long>();
            transactionStartQueue.Enqueue(new Tuple<TimeSpan, TaskCompletionSource<long>>(timeout, completion));

            long id = await completion.Task;
            return new TransactionInfo(id, false);
        }

        public async Task Commit(TransactionInfo transactionInfo)
        {
            TransactionsStatisticsGroup.OnTransactionCommitRequest();

            if (transactionInfo.IsReadOnly)
            {
                return;
            }

            var completion = new TaskCompletionSource<bool>();
            bool canCommit = true;

            List<Task<bool>> prepareTasks = new List<Task<bool>>(transactionInfo.WriteSet.Count);
            foreach (var g in transactionInfo.WriteSet.Keys)
            {
                TransactionalUnitVersion write;
                write.TransactionId = transactionInfo.TransactionId;
                write.WriteNumber = transactionInfo.WriteSet[g];

                TransactionalUnitVersion? read = null;
                if (transactionInfo.ReadSet.ContainsKey(g))
                {
                    read = transactionInfo.ReadSet[g];
                    transactionInfo.ReadSet.Remove(g);
                }
                prepareTasks.Add(g.Prepare(transactionInfo.TransactionId, write, read));
            }

            foreach (var g in transactionInfo.ReadSet.Keys)
            {
                TransactionalUnitVersion read = transactionInfo.ReadSet[g];
                prepareTasks.Add(g.Prepare(transactionInfo.TransactionId, null, read));
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
                TransactionsStatisticsGroup.OnTransactionAborted();
                abortedTransactions.TryAdd(transactionInfo.TransactionId, 0);
                throw new OrleansPrepareFailedException(transactionInfo.TransactionId);
            }
            transactionCommitQueue.Enqueue(new Tuple<TransactionInfo, TaskCompletionSource<bool>>(transactionInfo, completion));
            await completion.Task;
        }

        public void Abort(TransactionInfo transactionInfo)
        {
            abortedTransactions.TryAdd(transactionInfo.TransactionId, 0);
        }

        public bool IsAborted(long transactionId)
        {
            if (transactionId < abortLowerBound)
            {
                return true;
            }

            return abortedTransactions.ContainsKey(transactionId);
        }

        #endregion

        private async Task ProcessRequests(object args)
        {
            // NOTE: This code is a bit complicated because we want to issue both start and commit requests,
            // but wait for each one separately in its own continuation. This can be significantly simplified
            // if we can register a separate timer for start and commit.

            List<TransactionInfo> committingTransactions = new List<TransactionInfo>();
            List<TaskCompletionSource<bool>> commitCompletions = new List<TaskCompletionSource<bool>>();
            List<TimeSpan> startingTransactions = new List<TimeSpan>();
            List<TaskCompletionSource<long>> startCompletions = new List<TaskCompletionSource<long>>();

            while (transactionCommitQueue.Count > 0 || transactionStartQueue.Count > 0)
            {
                await WaitForWork();
                
                int startCount = transactionStartQueue.Count;
                while (startCount > 0 && startTransactionsTask.IsCompleted)
                {
                    Tuple<TimeSpan, TaskCompletionSource<long>> elem;
                    transactionStartQueue.TryDequeue(out elem);
                    startingTransactions.Add(elem.Item1);
                    startCompletions.Add(elem.Item2);

                    startCount--;
                }

                int commitCount = transactionCommitQueue.Count;
                while (commitCount > 0 && commitTransactionsTask.IsCompleted)
                {
                    Tuple<TransactionInfo, TaskCompletionSource<bool>> elem;
                    transactionCommitQueue.TryDequeue(out elem);
                    committingTransactions.Add(elem.Item1);
                    commitCompletions.Add(elem.Item2);

                    commitCount--;
                }


                if (startingTransactions.Count > 0 && startTransactionsTask.IsCompleted)
                {
                    logger.Verbose(ErrorCode.Transactions_SendingTMRequest, "Calling TM to start {0} transactions", startingTransactions.Count);

                    var startProxy = tmStartProxy ?? (tmStartProxy = await this.serviceFactory.GetTransactionStartService());
                    startTransactionsTask = startProxy.StartTransactions(startingTransactions).ContinueWith(
                        async startRequest =>
                        {
                            try
                            {
                                var startResponse = await startRequest;
                                var startedIds = startResponse.TransactionId;
                                Debug.Assert(startedIds.Count == startCompletions.Count);

                                // reply to clients with results
                                for (int i = 0; i < startCompletions.Count; i++)
                                {
                                    TransactionsStatisticsGroup.OnTransactionStarted();
                                    startCompletions[i].SetResult(startedIds[i]);
                                }

                                // Refresh cached values using new values from TM.
                                ReadOnlyTransactionId = startResponse.ReadOnlyTransactionId;
                                abortLowerBound = startResponse.AbortLowerBound;
                                logger.Verbose(ErrorCode.Transactions_ReceivedTMResponse, "{0} transactions started. readOnlyTransactionId {1}, abortLowerBound {2}", startingTransactions.Count, ReadOnlyTransactionId, abortLowerBound);
                            }
                            catch (Exception e)
                            {
                                logger.Error(ErrorCode.Transactions_TMError, "", e);

                                foreach (var completion in startCompletions)
                                {
                                    TransactionsStatisticsGroup.OnTransactionStartFailed();
                                    completion.SetException(new OrleansStartTransactionFailedException(e));
                                }

                                tmStartProxy = null; // Force refreshing the reference.
                            }

                            startingTransactions.Clear();
                            startCompletions.Clear();
                        });
                }

                if (committingTransactions.Count > 0 && commitTransactionsTask.IsCompleted)
                {
                    logger.Verbose(ErrorCode.Transactions_SendingTMRequest, "Calling TM to commit {0} transactions", committingTransactions.Count);

                    var commitProxy = tmCommitProxy ?? (tmCommitProxy = await this.serviceFactory.GetTransactionCommitService());
                    commitTransactionsTask = commitProxy.CommitTransactions(committingTransactions).ContinueWith(
                        async commitRequest =>
                        {
                            try
                            {
                                var commitResponse = await commitRequest;
                                var commitResults = commitResponse.CommitResult;
                                Debug.Assert(commitResults.Count == commitCompletions.Count);

                                // reply to clients with results
                                for (int i = 0; i < commitCompletions.Count; i++)
                                {
                                    if (commitResults[i].Success)
                                    {
                                        TransactionsStatisticsGroup.OnTransactionCommitted();
                                        commitCompletions[i].SetResult(true);
                                    }
                                    else
                                    {
                                        TransactionsStatisticsGroup.OnTransactionAborted();
                                        commitCompletions[i].SetException(commitResults[i].AbortingException);
                                    }
                                }

                                // Refresh cached values using new values from TM.
                                ReadOnlyTransactionId = commitResponse.ReadOnlyTransactionId;
                                abortLowerBound = commitResponse.AbortLowerBound;
                                logger.Verbose(ErrorCode.Transactions_ReceivedTMResponse, "{0} transactions committed. readOnlyTransactionId {1}, abortLowerBound {2}", committingTransactions.Count, ReadOnlyTransactionId, abortLowerBound);
                            }
                            catch (Exception e)
                            {
                                logger.Error(ErrorCode.Transactions_TMError, "", e);

                                for (int i = 0; i < commitCompletions.Count; i++)
                                {
                                    TransactionsStatisticsGroup.OnTransactionInDoubt();
                                    commitCompletions[i].SetException(new OrleansTransactionInDoubtException(committingTransactions[i].TransactionId));
                                }

                                tmCommitProxy = null; // Force refreshing the reference.
                            }

                            committingTransactions.Clear();
                            commitCompletions.Clear();
                        });
                }
            }

        }

        private Task WaitForWork()
        {
            // Returns a task that can be waited on until the RequestProcessor has
            // actionable work. The purpose is to avoid looping indefinitely while waiting
            // for the outstanding start or commit requests to complete.
            List<Task> toWait = new List<Task>();

            if (transactionStartQueue.Count > 0)
            {
                toWait.Add(startTransactionsTask);
            }

            if (transactionCommitQueue.Count > 0)
            {
                toWait.Add(commitTransactionsTask);
            }

            if (toWait.Count == 0)
            {
                return TaskDone.Done;
            }

            return Task.WhenAny(toWait);
        }


        public Task Start()
        {
            requestProcessor = GrainTimer.FromTaskCallback(this.RuntimeClient.Scheduler, ProcessRequests, null, TimeSpan.FromMilliseconds(0), TimeSpan.FromMilliseconds(10), "TransactionAgent");
            requestProcessor.Start();
            return TaskDone.Done;
        }

    }
}
