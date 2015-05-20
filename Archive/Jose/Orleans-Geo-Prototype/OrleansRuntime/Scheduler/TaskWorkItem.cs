using System;
using System.Threading.Tasks;

namespace Orleans.Scheduler
{
    internal class TaskWorkItem : WorkItemBase
    {
        private readonly Task Task;
        private readonly ITaskScheduler scheduler;
        private static readonly Logger logger = Logger.GetLogger("Scheduler.TaskWorkItem", Logger.LoggerType.Runtime);

        public override string Name { get { return String.Format("TaskRunner for task {0}", Task.Id); } }

        /// <summary>
        /// Create a new TaskWorkItem for running the specified Task on the specified scheduler.
        /// </summary>
        /// <param name="sched">Scheduler to execute this Task action. A value of null means use the Orleans system scheduler.</param>
        /// <param name="t">Task to be performed</param>
        /// <param name="taskName">Short descriptive name for this Task work item</param>
        internal TaskWorkItem(ITaskScheduler sched, Task t, ISchedulingContext context)
        {
            this.scheduler = sched;
            this.Task = t;
            this.SchedulingContext = context;
#if DEBUG
            if (logger.IsVerbose2) logger.Verbose2("Created TaskWorkItem {0} for Id={1} State={2} with Status={3} Scheduler={4}",
                Name, Task.Id, (Task.AsyncState == null) ? "null" : Task.AsyncState.ToString(), Task.Status, scheduler);
#endif
        }

        #region IWorkItem Members

        public override WorkItemType ItemType
        {
            get { return WorkItemType.Task; }
        }

        public override void Execute()
        {
#if DEBUG
            if (logger.IsVerbose2) logger.Verbose2("Executing TaskWorkItem for Task Id={0},Name={1},Status={2} on Scheduler={3}", Task.Id, Name, Task.Status, this.scheduler);
#endif

            this.scheduler.RunTask(Task);

#if DEBUG
            if (logger.IsVerbose2)
            {
                logger.Verbose2("Completed Task Id={0},Name={1} with Status={2} {3}",
                    Task.Id, Name, Task.Status, Task.Status == TaskStatus.Faulted ? "FAULTED: " + Task.Exception : "");
            }
#endif
        }

        internal static bool IsTaskRunning(Task t)
        {
            return !(
                t.Status == TaskStatus.Created
                || t.Status == TaskStatus.WaitingForActivation
                //|| t.Status == TaskStatus.WaitingToRun
            );
        }

        internal static bool IsTaskFinished(Task t)
        {
            return (
                t.Status == TaskStatus.RanToCompletion
                || t.Status == TaskStatus.Faulted
                || t.Status == TaskStatus.Canceled
            );
        }

        #endregion

        public override string ToString()
        {
            return base.ToString();
        }

        public override bool Equals(object other)
        {
            var otherItem = other as TaskWorkItem;
            // Note: value of the name field is ignored
            return otherItem != null && this.Task == otherItem.Task && this.scheduler == otherItem.scheduler;
        }

        public override int GetHashCode()
        {
            return Task.GetHashCode() ^ scheduler.GetHashCode();
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
