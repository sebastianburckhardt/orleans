//#define EXTRA_STATS
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

#if EXTRA_STATS
using Orleans.Counters;
#endif

namespace Orleans.Scheduler
{
    /// <summary>
    /// A single-concurrency, in-order task scheduler for per-activation work scheduling.
    /// </summary>
    [DebuggerDisplay("ActivationTaskScheduler-{_myId} RunQueue={_workerGroup.WorkItemCount}")]
    internal class ActivationTaskScheduler : TaskScheduler, ITaskScheduler
    {
        private static readonly Logger logger = Logger.GetLogger("Scheduler.ActivationTaskScheduler", Logger.LoggerType.Runtime);

        private static long _idCounter;
        private readonly long _myId;
        private readonly WorkItemGroup _workerGroup;
#if EXTRA_STATS
        private readonly CounterStatistic turnsExecutedStatistic;
#endif

        internal ActivationTaskScheduler(WorkItemGroup workGroup)
        {
            _myId = Interlocked.Increment(ref _idCounter);
            _workerGroup = workGroup;
#if EXTRA_STATS
            turnsExecutedStatistic = CounterStatistic.FindOrCreate(name + ".TasksExecuted");
#endif
            if (logger.IsVerbose) logger.Verbose("Created {0} with SchedulingContext={1}", this, _workerGroup.SchedulingContext);
        }

        #region TaskScheduler methods

        /// <summary>Gets an enumerable of the tasks currently scheduled on this scheduler.</summary>
        /// <returns>An enumerable of the tasks currently scheduled.</returns>
        protected override IEnumerable<Task> GetScheduledTasks()
        {
            return new Task[0];
        }

        public void RunTask(Task task)
        {
            RuntimeContext.SetExecutionContext(_workerGroup.SchedulingContext, this);
            bool done = base.TryExecuteTask(task);
            if (!done)
            {
                logger.Warn(ErrorCode.SchedulerTaskExecuteIncomplete4, "RunTask: Incomplete base.TryExecuteTask for Task Id={0} with Status={1}",
                    task.Id, task.Status);
            }
            // GK: REVIEW: 
            //  Consider adding ResetExecutionContext() or even better:
            //  Consider getting rid of ResetExecutionContext completely and just making sure we always call SetExecutionContext before TryExecuteTask.
        }

        internal void RunTaskOutsideContext(Task task)
        {
            bool done = base.TryExecuteTask(task);
            if (!done)
            {
                logger.Warn(ErrorCode.SchedulerTaskExecuteIncomplete4, "RunTask: Incomplete base.TryExecuteTask for Task Id={0} with Status={1}",
                    task.Id, task.Status);
            }
        }

        /// <summary>Queues a task to the scheduler.</summary>
        /// <param name="task">The task to be queued.</param>
        protected override void QueueTask(Task task)
        {
#if DEBUG
            if (logger.IsVerbose2) logger.Verbose2(_myId + " QueueTask Task Id={0}", task.Id);
#endif
            //TaskWorkItem workItem = new TaskWorkItem(this, task, _workerGroup.SchedulingContext);
            //_workerGroup.EnqueueWorkItem(workItem);
            _workerGroup.EnqueueTask(task);
        }

        /// <summary>
        /// Determines whether the provided <see cref="T:System.Threading.Tasks.Task"/> can be executed synchronously in this call, and if it can, executes it.
        /// </summary>
        /// <returns>
        /// A Boolean value indicating whether the task was executed inline.
        /// </returns>
        /// <param name="task">The <see cref="T:System.Threading.Tasks.Task"/> to be executed.</param>
        /// <param name="taskWasPreviouslyQueued">A Boolean denoting whether or not task has previously been queued. If this parameter is True, then the task may have been previously queued (scheduled); if False, then the task is known not to have been queued, and this call is being made in order to execute the task inline without queuing it.</param>
        protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued)
        {
            // GK: REVIEW
            bool canExecuteInline = WorkerPoolThread.CurrentWorkerThread != null;

            RuntimeContext ctx = RuntimeContext.Current;
            bool canExecuteInline2 = canExecuteInline && ctx != null && object.Equals(ctx.ActivationContext, _workerGroup.SchedulingContext);
            canExecuteInline = canExecuteInline2;

#if DEBUG
            if (logger.IsVerbose2)
            {
                logger.Verbose2(_myId + " --> TryExecuteTaskInline Task Id={0} Status={1} PreviouslyQueued={2} CanExecute={3} Queued={4}",
                    task.Id, task.Status, taskWasPreviouslyQueued, canExecuteInline, _workerGroup.ExternalWorkItemCount);
            }
#endif
            if (!canExecuteInline) return false;

            // If the task was previously queued, remove it from the queue
            if (taskWasPreviouslyQueued)
            {
                canExecuteInline = TryDequeue(task);
            }

            if (!canExecuteInline)
            {
#if DEBUG
                if (logger.IsVerbose2)
                {
                    logger.Verbose2(_myId + " <-X TryExecuteTaskInline Task Id={0} Status={1} Execute=No",
                        task.Id, task.Status);
                }
#endif
                return false;
            }

#if EXTRA_STATS
            turnsExecutedStatistic.Increment();
#endif
#if DEBUG
            if (logger.IsVerbose3)
            {
                logger.Verbose3(_myId + " TryExecuteTaskInline Task Id={0} Thread={1} Execute=Yes",
                    task.Id, Thread.CurrentThread.ManagedThreadId);
            }
#endif
            // Try to run the task.
            bool done;
            done = base.TryExecuteTask(task);
            if (!done)
            {
                logger.Warn(ErrorCode.SchedulerTaskExecuteIncomplete3, "TryExecuteTaskInline: Incomplete base.TryExecuteTask for Task Id={0} with Status={1}",
                    task.Id, task.Status);
            }
#if DEBUG
            if (logger.IsVerbose2)
            {
                logger.Verbose2(_myId + " <-- TryExecuteTaskInline Task Id={0} Thread={1} Execute=Done Ok={2}",
                    task.Id, Thread.CurrentThread.ManagedThreadId, done);
            }
#endif
            return done;
        }

        #endregion TaskScheduler methods

        public override string ToString()
        {
            return string.Format("{0}-{1}:Queued={2}", GetType().Name, _myId, _workerGroup.ExternalWorkItemCount);
        }

        // Task.TaskStatus:
        // 
        // Created                      = The task has been initialized but has not yet been scheduled.
        // WaitingForActivation         = The task is waiting to be activated and scheduled internally by the .NET Framework infrastructure.
        // WaitingToRun                 = The task has been scheduled for execution but has not yet begun executing.
        // Running                      = The task is running but has not yet completed.
        // WaitingForChildrenToComplete = The task has finished executing and is implicitly waiting for attached child tasks to complete.
        // RanToCompletion              = The task completed execution successfully.
        // Canceled                     = The task acknowledged cancellation by throwing an OperationCanceledException with its own CancellationToken while the token was in signaled state, or the task's CancellationToken was already signaled before the task started executing.
        // Faulted                      = The task completed due to an unhandled exception. 
    }
}
