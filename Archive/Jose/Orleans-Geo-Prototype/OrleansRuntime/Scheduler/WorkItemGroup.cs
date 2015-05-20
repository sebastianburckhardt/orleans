using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Orleans.Counters;


namespace Orleans.Scheduler
{
    internal enum WorkGroupStatus
    {
        Waiting = 0,
        Runnable = 1,
        Running = 2,
        Quiescing = 3,
        Shutdown = 4
    }

    [DebuggerDisplay("WorkItemGroup State={state} PendingContinuations={pendingContinuationCount} Context={_schedulingContext.Name}")]
    internal class WorkItemGroup : IWorkItem
    {
        private static readonly Logger appLogger = Logger.GetLogger("Scheduler.WorkItemGroup", Logger.LoggerType.Runtime);
        private readonly Logger log;

        private readonly OrleansTaskScheduler masterScheduler;
        internal ActivationTaskScheduler TaskRunner { get; private set; }
        private WorkGroupStatus state;
        private readonly Object lockable;
        private readonly Queue<Task> workItems;

        private long totalItemsEnQueued;    // equals total items queued, + 1
        private long totalItemsProcessed;
        private readonly QueueTrackingStatistic queueTracking;
        private TimeSpan totalQueuingDelay;
        private readonly long quantumExpirations;
        private readonly int workItemGroupStatisticsNumber;
        public DateTime TimeQueued { get; set; }

        public TimeSpan TimeSinceQueued
        {
            //get { return timeIntervalSinceQueued.Elapsed; }
            get { return Utils.Since(TimeQueued); } 
        }

        public ISchedulingContext SchedulingContext { get; set; }

        public bool IsSystem
        {
            get { return SchedulingUtils.IsSystemContext(this.SchedulingContext); }
        }

        public string Name { get { return SchedulingContext == null ? "unknown" : SchedulingContext.Name; } }

        internal int ExternalWorkItemCount
        {
            get { lock (lockable) { return workItemCount; } }
        }

        private int workItemCount
        {
            get { return workItems.Count; } 
        }

        internal float AverageQueueLenght
        {
            get 
            {
#if TRACK_DETAILED_STATS
                if (StatisticsCollector.CollectQueueStats)
                {
                    return queueTracking.AverageQueueLength;
                }
#endif
                return 0;
            }
        }

        internal float NumEnqueuedRequests
        {
            get
            {
#if TRACK_DETAILED_STATS
                if (StatisticsCollector.CollectQueueStats)
                {
                    return queueTracking.NumEnqueuedRequests;
                }
#endif
                return 0;
            }
        }

        internal float ArrivalRate
        {
            get
            {
#if TRACK_DETAILED_STATS
                if (StatisticsCollector.CollectQueueStats)
                {
                    return queueTracking.ArrivalRate;
                }
#endif
                return 0;
            }
        }

        private bool IsActive
        {
            get
            {
                return workItemCount != 0;
            }
        }

        // This is the maximum number of work items to be processed in an activation turn. 
        // If this is set to zero or a negative number, then the full work queue is drained (MaxTimePerTurn allowing).
        private static readonly int MaxWorkItemsPerTurn;
        // This is a soft time limit on the duration of activation macro-turn (a number of micro-turns). 
        // If a activation was running its micro-turns longer than this, we will give up the thread.
        // If this is set to zero or a negative number, then the full work queue is drained (MaxWorkItemsPerTurn allowing).
        public static TimeSpan ActivationSchedulingQuantum { get; set; }
        // This is the maximum number of waiting threads (blocked in WaitForResponse) allowed
        // per ActivationWorker. An attempt to wait when there are already too many threads waiting
        // will result in a TooManyWaitersException being thrown.
        //private static readonly int MaxWaitingThreads;
        // This is the maximum number of pending work items for a single activation before we write a warning log.
        private static LimitValue MaxPendingItemsLimit;

        static WorkItemGroup()
        {
            MaxWorkItemsPerTurn = 0;    // Unlimited
            //MaxWaitingThreads = 500;
        }

        internal WorkItemGroup(OrleansTaskScheduler sched, ISchedulingContext schedulingContext)
        {
            masterScheduler = sched;
            SchedulingContext = schedulingContext;
            state = WorkGroupStatus.Waiting;
            workItems = new Queue<Task>();
            lockable = new Object();
            totalItemsEnQueued = 0;
            totalItemsProcessed = 0;
            totalQueuingDelay = TimeSpan.Zero;
            //timeIntervalSinceQueued = TimeIntervalFactory.CreateTimeInterval(StatisticsCollector.MeasureFineGrainedTime);
            quantumExpirations = 0;
            TaskRunner = new ActivationTaskScheduler(this);
            MaxPendingItemsLimit = LimitManager.GetLimit(LimitNames.Limit_MaxPendingItems);
            log = IsSystem ? Logger.GetLogger("Scheduler." + Name + ".WorkItemGroup", Logger.LoggerType.Runtime) : appLogger;

            if (StatisticsCollector.CollectQueueStats)
            {
                queueTracking = new QueueTrackingStatistic("Scheduler." + SchedulingContext.Name,
                               StatisticsCollector.ReportPerWorkItemStats(SchedulingContext) ? CounterStorage.LogAndTable : CounterStorage.DontStore);
                queueTracking.OnStartExecution();
            }

            if (StatisticsCollector.CollectPerWorkItemStats)
            {
                workItemGroupStatisticsNumber = SchedulerStatisticsGroup.RegisterWorkItemGroup(SchedulingContext.Name, SchedulingContext,
                            () =>
                            {
                                StringBuilder sb = new StringBuilder();
                                lock (lockable)
                                {
                                    
                                    sb.Append("QueueLength = " + workItemCount);
                                sb.Append(String.Format(", State = {0}", state.ToString()));
                                if (state == WorkGroupStatus.Runnable)
                                {
                                        sb.Append(String.Format("; oldest item is {0} old", workItems.Count >= 0 ? workItems.Peek().ToString() : "null"));
                                }
                                }
                                return sb.ToString();
                            });
            }
        }

        /// <summary>
        /// Adds a task to this activation.
        /// If we're adding it to the run list and we used to be waiting, now we're runnable.
        /// </summary>
        /// <param name="task">The work item to add.</param>
        public void EnqueueTask(Task task)
        {
            lock (lockable)
            {
#if DEBUG
                if (log.IsVerbose2) log.Verbose2("EnqueueWorkItem {0} into {1} when TaskScheduler.Current={2}", task, SchedulingContext, TaskScheduler.Current);
#endif

                if (state == WorkGroupStatus.Shutdown)
                {
                    string msg = string.Format("Enqueuing task {0} to a stopped work item group {1}.\n Stack Trace: {2}", task, DumpStatus(), new StackTrace(true));
                    log.Error(ErrorCode.SchedulerNotEnqueuWorkWhenShutdown, msg);
                    TaskRunner.RunTaskOutsideContext(task);

                    // Throwing is necessary to avoid dropping work items
                    //throw new InvalidOperationException(msg);
                    //masterScheduler.RunTaskOnNullContext(task);
                }

                long thisSequenceNumber = totalItemsEnQueued++;
                int count = workItemCount;
#if TRACK_DETAILED_STATS
                if (StatisticsCollector.CollectQueueStats)
                {
                    queueTracking.OnEnQueueRequest(1, count);
                }
                if (StatisticsCollector.CollectGlobalShedulerStats)
                {
                    SchedulerStatisticsGroup.OnWorkItemEnqueue();
                }
#endif
                workItems.Enqueue(task);
                int maxPendingItemsLimit = MaxPendingItemsLimit.SoftLimitThreshold;
                if (maxPendingItemsLimit > 0 && count > maxPendingItemsLimit)
                {
                    log.Warn(ErrorCode.SchedulerTooManyPendingItems, String.Format("{0} pending work items for group {1}, exceeding the warning threshold of {2}",
                        count, Name, maxPendingItemsLimit));
                }
                if (state == WorkGroupStatus.Waiting)
                {
                    state = WorkGroupStatus.Runnable;
#if DEBUG
                    if (log.IsVerbose3)
                    {
                        log.Verbose3("Add to RunQueue {0}, #{1}, onto {2}", task, thisSequenceNumber, SchedulingContext);
                    }
#endif
                    masterScheduler.RunQueue.Add(this);
                }
            }
        }

        /// <summary>
        /// Shuts down this work item group so that it will not process any additional work items, even if they
        /// have already been queued.
        /// </summary>
        internal void Stop()
        {
            lock (lockable)
            {
                if (IsActive)
                {
                    ReportWorkGroupProblemWithBacktrace(
                        "WorkItemGroup is being stoped while still active.",
                        ErrorCode.SchedulerWorkGroupStopping); // Throws InvalidOperationException
                }

                if (state == WorkGroupStatus.Shutdown)
                {
                    log.Warn(ErrorCode.SchedulerWorkGroupShuttingDown, "WorkItemGroup is already shutting down {0}", this.ToString());
                    return;
                }

                state = WorkGroupStatus.Shutdown;

                if (StatisticsCollector.CollectPerWorkItemStats)
                {
                    SchedulerStatisticsGroup.UnRegisterWorkItemGroup(workItemGroupStatisticsNumber);
                }

                if (StatisticsCollector.CollectGlobalShedulerStats)
                {
                    SchedulerStatisticsGroup.OnWorkItemDrop(workItemCount);
                }
                if (StatisticsCollector.CollectQueueStats)
                {
                    queueTracking.OnStopExecution();
                }
                workItems.Clear();
            }
        }
        #region IWorkItem Members

        public WorkItemType ItemType
        {
            get { return WorkItemType.WorkItemGroup; }
        }

        // Execute one or more turns for this activation. 
        // This method is always called in a single-threaded environment -- that is, no more than one
        // thread will be in this method at once -- but other asynch threads may still be queueing tasks, etc.
        public void Execute()
        {
            lock (lockable)
            {
                if (state == WorkGroupStatus.Shutdown)
                {
                    if (!IsActive)
                    {
                        return;  // Don't mind if no work has been queued to this work group yet.
                    }

                    ReportWorkGroupProblemWithBacktrace(
                        "Cannot execute work item group if shutdown.",
                        ErrorCode.SchedulerNotExecuteWhenShutdown); // Throws InvalidOperationException
                }
                state = WorkGroupStatus.Running;
            }

            WorkerPoolThread thread = WorkerPoolThread.CurrentWorkerThread;

            try
            {
                // Process multiple items -- drain the applicationMessageQueue (up to max items) for this physical activation
                int count = 0;
                Stopwatch timer = new Stopwatch();
                timer.Start();
                // while ((count <= MaxWorkItemsPerTurn) || (MaxWorkItemsPerTurn <= 0))
                do 
                {
                    lock (lockable)
                    {
                        if (state == WorkGroupStatus.Shutdown)
                        {
                            log.Info(ErrorCode.SchedulerSkipWorkStopping, "Thread {0} is exiting work loop due to Shutdown state {1}. Have {2} work items in the queue.", 
                                thread.ToString(), this.ToString(), workItemCount);
                            break;
                        }

                        // Check the cancellation token (means that the silo is stopping)
                        if (thread.CancelToken.IsCancellationRequested)
                        {
                                log.Warn(ErrorCode.SchedulerSkipWorkCancelled, "Thread {0} is exiting work loop due to cancellation token. WorkItemGroup: {1}, Have {2} work items in the queue.",
                                    thread.ToString(), this.ToString(), workItemCount);
                            break;
                        }
                    }

                    // Get the first Work Item on the list
                    Task task = null;
                    lock (lockable)
                    {
                        if (workItems.Count > 0)
                        {
                            task = workItems.Dequeue();
                        }
                        // If the list is empty, then we're done
                        else
                        {
                            break;
                        }
                    }

                    ////// Capture the queue wait time for this task
                    ////TimeSpan waitTime = workItem.TimeSinceQueued;
                    ////if (waitTime > masterScheduler.DelayWarningThreshold && !Debugger.IsAttached)
                    ////{
                    ////    SchedulerStatisticsGroup.NumLongQueueWaitTimes.Increment();
                    ////    log.Warn(ErrorCode.SchedulerWorkItemGroupQueueWaitTime, "Queue wait time of {0} on thread {1} for Group {2}", waitTime, thread.ToString(), workItem);
                    ////}
                    ////totalQueuingDelay += waitTime;

#if TRACK_DETAILED_STATS
                    if (StatisticsCollector.CollectGlobalShedulerStats)
                    {
                        SchedulerStatisticsGroup.OnWorkItemDequeue();
                    }
#endif
                    // Set the current activation SchedulingContext
                    //RuntimeContext.SetExecutionContext(SchedulingContext, TaskRunner);

#if DEBUG
                    if (log.IsVerbose2) log.Verbose2("About to execute task {0} in SchedulingContext={1}", task, SchedulingContext);
#endif
                    TimeSpan taskStart = timer.Elapsed;

                    try
                    {
                        thread.CurrentTask = task;
#if TRACK_DETAILED_STATS
                        if (StatisticsCollector.CollectTurnsStats)
                        {
                            SchedulerStatisticsGroup.OnTurnExecutionStartsByWorkGroup(workItemGroupStatisticsNumber, thread.WorkerThreadStatisticsNumber, SchedulingContext);
                        }
#endif
                        TaskRunner.RunTask(task);
                    }
                    catch (Exception ex)
                    {
                        log.Error(ErrorCode.SchedulerExceptionFromExecute, String.Format("Worker thread caught an exception thrown from Execute by task {0}", task), ex);
                        throw;
                    }
                    finally
                    {
#if TRACK_DETAILED_STATS
                        if (StatisticsCollector.CollectTurnsStats)
                        {
                            SchedulerStatisticsGroup.OnTurnExecutionEnd(Utils.Since(thread.CurrentStateStarted));
                        }
                        if (StatisticsCollector.CollectThreadTimeTrackingStats)
                        {
                            thread.threadTracking.IncrementNumberOfProcessed();
                        }
#endif
                        totalItemsProcessed++;
                        TimeSpan taskLength = timer.Elapsed - taskStart;
                        if (taskLength > OrleansTaskScheduler.TurnWarningLengthThreshold)
                        {
                            SchedulerStatisticsGroup.NumLongRunningTurns.Increment();
                            log.Warn(ErrorCode.SchedulerTurnTooLong3, "Task {0} in WorkGroup {1} took elapsed time {2:g} for execution, which is longer than {3}. Running on thread {4}",
                                    OrleansTaskExtentions.ToString(task), SchedulingContext.ToString(), taskLength, OrleansTaskScheduler.TurnWarningLengthThreshold, thread.ToString());
                        }
                        thread.CurrentTask = null;
                    }
                    count++;
                } while (((MaxWorkItemsPerTurn <= 0) || (count <= MaxWorkItemsPerTurn)) &&
                    ((ActivationSchedulingQuantum <= TimeSpan.Zero) || (timer.Elapsed < ActivationSchedulingQuantum)));
                timer.Stop();
            }
            catch (Exception ex)
            {
                log.Error(ErrorCode.Runtime_Error_100032, String.Format("Worker thread {0} caught an exception thrown from IWorkItem.Execute", thread), ex);
            }
            finally
            {
                // Now we're not Running anymore. 
                // If we left work items on our run list, we're Runnable, and need to go back on the silo run queue; 
                // If our run list is empty, then we're waiting.
                lock (lockable)
                {
                    if (state != WorkGroupStatus.Shutdown)
                    {
                        if (workItemCount > 0)
                        {
                            state = WorkGroupStatus.Runnable;
                            masterScheduler.RunQueue.Add(this);
                        }
                        else
                        {
                            state = WorkGroupStatus.Waiting;
                        }
                    }
                }
            }
        }

        #endregion

        public override string ToString()
        {
            return String.Format("{0}WorkItemGroup:Name={1},State={2}",
                IsSystem ? "System*" : "",
                Name,
                state
            );
        }

        public string DumpStatus()
        {
            lock (lockable)
            {
                StringBuilder sb = new StringBuilder();
                sb.Append(this.ToString());
                sb.AppendFormat(". Currently QueuedWorkItems={0}; Total EnQueued={1}; Total processed={2}; Quantum expirations={3}; ",
                    workItemCount, totalItemsEnQueued, totalItemsProcessed, quantumExpirations);
                if (AverageQueueLenght != 0)
                {
                    sb.AppendFormat("average queue length at enqueue: {0}; ", AverageQueueLenght);
                    if (!totalQueuingDelay.Equals(TimeSpan.Zero))
                    {
                        sb.AppendFormat("average queue delay: {0}ms; ", totalQueuingDelay.Divide(totalItemsProcessed).TotalMilliseconds);
                    }
                }
                sb.AppendFormat("TaskRunner={0}; ", TaskRunner);
                sb.AppendFormat("SchedulingContext={0}.", SchedulingContext);
                return sb.ToString();
            }
        }

        private void ReportWorkGroupProblemWithBacktrace(string what, ErrorCode errorCode)
        {
            var st = new StackTrace();
            string msg = string.Format("{0} {1}", what, DumpStatus());
            log.Error(errorCode, msg + " \nCalled from " + st);
            throw new InvalidOperationException(msg);
        }
    }
}
