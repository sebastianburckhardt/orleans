#define STATISTICS
#define USE_TASK_WRAPPER
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Orleans.Counters;

namespace Orleans.Scheduler
{
    [DebuggerDisplay("OrleansTaskScheduler RunQueue={RunQueue.Length}")]
    internal class OrleansTaskScheduler : AbstractOrleansTaskScheduler, ITaskScheduler, ISiloShutdownParticipant, IHealthCheckParticipant
    {
        public static OrleansTaskScheduler Instance { get; private set; }

        internal WorkQueue RunQueue { get; private set; }
        internal WorkerPool Pool { get; private set; }
        internal TimeSpan DelayWarningThreshold { get; private set; }
        private TimeSpan ResponseTimeout;
        private readonly ConcurrentDictionary<ISchedulingContext, WorkItemGroup> wgDirectory; // work group directory
        private Action tryFinishShutdown;
        private bool applicationTurnsStopped;

        public int RunQueueLength { get { return RunQueue.Length; } }

        internal static TimeSpan TurnWarningLengthThreshold { get; set; }

        readonly Logger logger = Logger.GetLogger("Scheduler.OrleansTaskScheduler", Logger.LoggerType.Runtime);

        public OrleansTaskScheduler(int maxActiveThreads)
            : this(maxActiveThreads, TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(100), 
            Constants.DEFAULT_RESPONSE_TIMEOUT, NodeConfiguration.INJECT_MORE_WORKER_THREADS)
        {
        }

        public OrleansTaskScheduler(GlobalConfiguration globalConfig, NodeConfiguration config)
            : this(config.MaxActiveThreads, config.DelayWarningThreshold, config.ActivationSchedulingQuantum,
                    config.TurnWarningLengthThreshold, globalConfig.ResponseTimeout, config.InjectMoreWorkerThreads)
        {
        }

        private OrleansTaskScheduler(int maxActiveThreads, TimeSpan delayWarningThreshold, TimeSpan activationSchedulingQuantum,
            TimeSpan turnWarningLengthThreshold, TimeSpan responseTimeout, bool injectMoreWorkerThreads)
        {
            Instance = this;
            DelayWarningThreshold = delayWarningThreshold;
            WorkItemGroup.ActivationSchedulingQuantum = activationSchedulingQuantum;
            TurnWarningLengthThreshold = turnWarningLengthThreshold;
            ResponseTimeout = responseTimeout;
            applicationTurnsStopped = false;
            wgDirectory = new ConcurrentDictionary<ISchedulingContext, WorkItemGroup>();
            RunQueue = new WorkQueue();
            logger.Info("Starting OrleansTaskScheduler with {0} Max Active application Threads and 1 system thread.", maxActiveThreads);
            Pool = new WorkerPool(this, maxActiveThreads, injectMoreWorkerThreads);
            IntValueStatistic.FindOrCreate(StatNames.STAT_SCHEDULER_WORKITEMGROUP_COUNT, () => WorkItemGroupCount);
            IntValueStatistic.FindOrCreate(new StatName(StatNames.STAT_QUEUES_QUEUE_SIZE_INSTANTANEOUS_PER_QUEUE, "Scheduler.LevelOne"), () => RunQueueLength);
            if (StatisticsCollector.CollectQueueStats)
            {
                //FloatValueStatistic.FindOrCreate(new StatName(StatNames.STAT_QUEUES_QUEUE_SIZE_INSTANTANEOUS_PER_QUEUE, "Scheduler.LevelTwo.Average"), () => RunQueueLength_LevelTwo);

                FloatValueStatistic.FindOrCreate(new StatName(StatNames.STAT_QUEUES_QUEUE_SIZE_AVERAGE_PER_QUEUE, "Scheduler.LevelTwo.Average"), () => Average_RunQueueLength_LevelTwo);
                FloatValueStatistic.FindOrCreate(new StatName(StatNames.STAT_QUEUES_ENQUEUED_PER_QUEUE, "Scheduler.LevelTwo.Average"), () => Average_Enqueued_LevelTwo);
                FloatValueStatistic.FindOrCreate(new StatName(StatNames.STAT_QUEUES_AVERAGE_ARRIVAL_RATE_PER_QUEUE, "Scheduler.LevelTwo.Average"), () => Average_ArrivalRate_LevelTwo);

                FloatValueStatistic.FindOrCreate(new StatName(StatNames.STAT_QUEUES_QUEUE_SIZE_AVERAGE_PER_QUEUE, "Scheduler.LevelTwo.Sum"), () => Sum_RunQueueLength_LevelTwo);
                FloatValueStatistic.FindOrCreate(new StatName(StatNames.STAT_QUEUES_ENQUEUED_PER_QUEUE, "Scheduler.LevelTwo.Sum"), () => Sum_Enqueued_LevelTwo);
                FloatValueStatistic.FindOrCreate(new StatName(StatNames.STAT_QUEUES_AVERAGE_ARRIVAL_RATE_PER_QUEUE, "Scheduler.LevelTwo.Sum"), () => Sum_ArrivalRate_LevelTwo);
            }
        }

        public int WorkItemGroupCount { get { return wgDirectory.Count; } }

        private float Average_RunQueueLength_LevelTwo
        {
            get
            {
                if (wgDirectory.IsEmpty) return 0;
                return (float)wgDirectory.Values.Sum(workgroup => workgroup.AverageQueueLenght) / (float)wgDirectory.Values.Count;
            }
        }

        private float Average_Enqueued_LevelTwo
        {
            get
            {
                if (wgDirectory.IsEmpty) return 0;
                return (float)wgDirectory.Values.Sum(workgroup => workgroup.NumEnqueuedRequests) / (float)wgDirectory.Values.Count;
            }
        }

        private float Average_ArrivalRate_LevelTwo
        {
            get
            {
                if (wgDirectory.IsEmpty) return 0;
                return (float)wgDirectory.Values.Sum(workgroup => workgroup.ArrivalRate) / (float)wgDirectory.Values.Count;
            }
        }

        private float Sum_RunQueueLength_LevelTwo
        {
            get
            {
                return (float)wgDirectory.Values.Sum(workgroup => workgroup.AverageQueueLenght);
            }
        }

        private float Sum_Enqueued_LevelTwo
        {
            get
            {
                return (float)wgDirectory.Values.Sum(workgroup => workgroup.NumEnqueuedRequests);
            }
        }

        private float Sum_ArrivalRate_LevelTwo
        {
            get
            {
                return (float)wgDirectory.Values.Sum(workgroup => workgroup.ArrivalRate);
            }
        }

        public void Start()
        {
            Pool.Start();
        }

        public void StopApplicationTurns()
        {
#if DEBUG
            if (logger.IsVerbose) logger.Verbose("StopApplicationTurns");
#endif
            RunQueue.RunDownApplication();
            applicationTurnsStopped = true;
            foreach (var group in wgDirectory.Values)
            {
                if (!group.IsSystem)
                {
                    group.Stop();
                }
            }
        }

        public void Stop()
        {
            RunQueue.RunDown();
            Pool.Stop();
            PrintStatistics();
        }

        protected override IEnumerable<Task> GetScheduledTasks()
        {
            // TODO: return something more useful
            return new Task[0];
        }

        protected override void QueueTask(Task task)
        {
            object contextObj = task.AsyncState;
#if DEBUG
            if (logger.IsVerbose2) logger.Verbose2("QueueTask: Id={0} with Status={1} AsyncState={2} when TaskScheduler.Current={3}", task.Id, task.Status, task.AsyncState, Current);
#endif
            ISchedulingContext context = contextObj as ISchedulingContext;
            WorkItemGroup workGroup = GetWorkItemGroup(context);
            if (applicationTurnsStopped && (workGroup != null) && !workGroup.IsSystem)
            {
                // Drop the task on the floor if it's an application work item and application turns are stopped
                string msg = string.Format("Dropping Task {0} because applicaiton turns are stopped", task);
                logger.Warn(ErrorCode.SchedulerAppTurnsStopped, msg);
                return;
            }

            if (workGroup == null)
            {
                TaskWorkItem todo = new TaskWorkItem(this, task, context);
                RunQueue.Add(todo);
            }
            else
            {
                string error = String.Format("QueueTask was called on OrleansTaskScheduler for task {0} on Context {1}."
                    + " Should only call OrleansTaskScheduler.QueueTask with tasks on the null context.",
                    task.Id, context);
                logger.Error(ErrorCode.SchedulerQueueTaskWrongCall, error);
                throw new InvalidOperationException(error);
                //workGroup.EnqueueWorkItem(todo);
            }
        }

        // Enqueue a work item to a given context
        public void QueueWorkItem(IWorkItem workItem, ISchedulingContext context)
        {
#if DEBUG
            if (logger.IsVerbose2) logger.Verbose2("QueueWorkItem " + context);
#endif
            if (workItem is TaskWorkItem)
            {
                string error = String.Format("QueueWorkItem was called on OrleansTaskScheduler for TaskWorkItem {0} on Context {1}."
                    + " Should only call OrleansTaskScheduler.QueueWorkItem on WorkItems that are NOT TaskWorkItem. Tasks should be queued to the scheduler via QueueTask call.",
                    workItem.ToString(), context);
                logger.Error(ErrorCode.SchedulerQueueWorkItemWrongCall, error);
                throw new InvalidOperationException(error);
            }

            WorkItemGroup workGroup = GetWorkItemGroup(context);
            if (applicationTurnsStopped && (workGroup != null) && !workGroup.IsSystem)
            {
                // Drop the task on the floor if it's an application work item and application turns are stopped
                string msg = string.Format("Dropping work item {0} because applicaiton turns are stopped", workItem);
                logger.Warn(ErrorCode.SchedulerAppTurnsStopped, msg);
                return;
            }

            workItem.SchedulingContext = context;

            // GK: we must wrap any work item in Task and enqueue it as a task to the right scheduler via Task.Start.
            // This will make sure the TaskScheduler.Current is set correctly on any task that is created implicitly in the execution of this workItem.
            if (workGroup == null)
            {
                Task t = TaskSchedulerUtils.WrapWorkItemAsTask(workItem, context, this);
                t.Start(this);
                //RunQueue.Add(workItem);
            }
            else
            {
                // Create Task wrapper for this work item
                Task t = TaskSchedulerUtils.WrapWorkItemAsTask(workItem, context, workGroup.TaskRunner);
                t.Start(workGroup.TaskRunner);
            }
        }

        // Only required if you have work groups flagged by a context that is not a WorkGroupingContext
        public WorkItemGroup RegisterWorkContext(ISchedulingContext context)
        {
            if (context != null)
            {
                WorkItemGroup wg = new WorkItemGroup(this, context);
                wgDirectory.TryAdd(context, wg);
                return wg;
            }
            return null;
        }

        // Only required if you have work groups flagged by a context that is not a WorkGroupingContext
        public void UnregisterWorkContext(ISchedulingContext context)
        {
            if (context != null)
            {
                WorkItemGroup workGroup;
                if (wgDirectory.TryRemove(context, out workGroup))
                {
                    workGroup.Stop();
                }
            }
        }

        // public for testing only -- should be private, otherwise
        public WorkItemGroup GetWorkItemGroup(ISchedulingContext context)
        {
            WorkItemGroup workGroup = null;
            if (context != null)
            {
                wgDirectory.TryGetValue(context, out workGroup);
            }
            return workGroup;
        }

        public override TaskScheduler GetTaskScheduler(ISchedulingContext context)
        {
            if (context == null)
            {
                return this;
            }
            WorkItemGroup workGroup = null;
            if (wgDirectory.TryGetValue(context, out workGroup))
            {
                return workGroup.TaskRunner;
            }
            return this;
        }

        public override int MaximumConcurrencyLevel { get { return Pool.MaxActiveThreads; } }

        protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued)
        {
            //bool canExecuteInline = WorkerPoolThread.CurrentContext != null;

            RuntimeContext ctx = RuntimeContext.Current;
            bool canExecuteInline = ctx == null || ctx.ActivationContext==null;

#if DEBUG
            if (logger.IsVerbose2) 
            {
                logger.Verbose2("TryExecuteTaskInline Id={0} with Status={1} PreviouslyQueued={2} CanExecute={3}",
                    task.Id, task.Status, taskWasPreviouslyQueued, canExecuteInline);
            }
#endif
            if (canExecuteInline)
            {
                if (taskWasPreviouslyQueued)
                {
                    canExecuteInline = TryDequeue(task);
                }

                if (canExecuteInline)
                {
                    // We are on a worker pool thread, so can execute this task
                    bool done = base.TryExecuteTask(task);
                    if (!done)
                    {
                        logger.Warn(ErrorCode.SchedulerTaskExecuteIncomplete1, "TryExecuteTaskInline: Incomplete base.TryExecuteTask for Task Id={0} with Status={1}",
                            task.Id, task.Status);
                    }
                    return done;
                }
            }
            // Otherwise, we can't execute tasks in-line on non-worker pool threads
            return false;
        }

        //internal void RunTaskOnNullContext(Task task)
        //{
        //    ISchedulingContext captured = AsyncCompletion.Context;
        //    Utils.SafeExecute(() => RuntimeContext.SetExecutionContext(null, this), logger, "OrleansTaskScheduler.RunTaskOnNullContext.SetExecutionContext(null)");
        //    Utils.SafeExecute(() => base.TryExecuteTask(task), logger, "OrleansTaskScheduler.RunTaskOnNullContext.TryExecuteTask()");
        //    Utils.SafeExecute(() => RuntimeContext.SetExecutionContext(captured, this), logger, "OrleansTaskScheduler.RunTaskOnNullContext.SetExecutionContext(captured)");
        //}
        /// <summary>
        /// Run the specified task synchronously on the current thread
        /// </summary>
        /// <param name="task"><c>Task</c> to be executed</param>
        public void RunTask(Task task)
        {
#if DEBUG
            if (logger.IsVerbose2) logger.Verbose2("RunTask: Id={0} with Status={1} AsyncState={2} when TaskScheduler.Current={3}", task.Id, task.Status, task.AsyncState, Current);
#endif
            ISchedulingContext context = AsyncCompletion.Context;
            WorkItemGroup workGroup = GetWorkItemGroup(context);

            if (workGroup == null)
            {
                RuntimeContext.SetExecutionContext(null, this);
                bool done = base.TryExecuteTask(task);
                if (!done)
                {
                    logger.Warn(ErrorCode.SchedulerTaskExecuteIncomplete2, "RunTask: Incomplete base.TryExecuteTask for Task Id={0} with Status={1}",
                        task.Id, task.Status);
                }
            }
            else
            {
                string error = String.Format("RunTask was called on OrleansTaskScheduler for task {0} on Context {1}. Should only call OrleansTaskScheduler.RunTask on tasks queued on a null context.", 
                    task.Id, context);
                logger.Error(ErrorCode.SchedulerTaskRunningOnWrongScheduler1, error);
                throw new InvalidOperationException(error);
            }

#if DEBUG
            if (logger.IsVerbose2) logger.Verbose2("RunTask: Completed Id={0} with Status={1} task.AsyncState={2} when TaskScheduler.Current={3}", task.Id, task.Status, task.AsyncState, Current);
#endif
        }

        // Returns true if healthy, false if not
        public bool CheckHealth(DateTime lastCheckTime)
        {
            return Pool.DoHealthCheck();
        }

        /// <summary>
        /// Action to be invoked when there is no more work for this scheduler
        /// </summary>
        internal Action OnIdle { get; set; }

        /// <summary>
        /// Invoked by WorkerPool when all threads go idle
        /// </summary>
        internal void OnAllWorkerThreadsIdle()
        {
            if (OnIdle != null && RunQueueLength == 0)
            {
#if DEBUG
                if (logger.IsVerbose2) logger.Verbose2("OnIdle");
#endif
                OnIdle();
            }
        }

        private void PrintStatistics()
        {
            if (logger.IsInfo)
            {
                string stats = Utils.IEnumerableToString(wgDirectory.Values.OrderBy(wg => wg.Name), wg => string.Format("--{0}", wg.DumpStatus()), "\r\n");
                if (stats.Length > 0)
                {
                    logger.LogWithoutBulkingAndTruncating(OrleansLogger.Severity.Info, ErrorCode.SchedulerStatistics, "OrleansTaskScheduler.PrintStatistics(): RunQueue={0}, WorkItems={1}, Directory=\r\n{2}",
                                RunQueue.Length, WorkItemGroupCount, stats);
                }
            }
        }

        internal void DumpSchedulerStatus(bool alwaysOutput = true)
        {
            if (logger.IsVerbose || alwaysOutput)
            {
                PrintStatistics();

                StringBuilder sb = new StringBuilder();
                sb.AppendLine("Dump of current OrleansTaskScheduler status:");
                sb.AppendFormat("CPUs={0} RunQueue={1}, WorkItems={2} {3}",
                    Environment.ProcessorCount,
                    RunQueue.Length,
                    wgDirectory.Count,
                    applicationTurnsStopped ? "STOPPING" : "").AppendLine();

                sb.AppendLine("RunQueue:");
                RunQueue.DumpStatus(sb);

                Pool.DumpStatus(sb);

                foreach (var workgroup in wgDirectory.Values)
                {
                    sb.AppendLine(workgroup.DumpStatus());
                }

                logger.LogWithoutBulkingAndTruncating(OrleansLogger.Severity.Info, ErrorCode.SchedulerStatus, sb.ToString());
            }
        }

        // For testing only
        internal static OrleansTaskScheduler InitializeSchedulerForTesting(ISchedulingContext context)
        {
            StatisticsCollector.StatisticsCollectionLevel = StatisticsLevel.Info;
            SchedulerStatisticsGroup.Init();
            var scheduler = new OrleansTaskScheduler(4);
            LimitManager.Initialize(new DummyLimitsConfiguration());
            OrleansTask.Initialize(scheduler);
            scheduler.Start();
            WorkItemGroup ignore = scheduler.RegisterWorkContext(context);
            return scheduler;
        }

        #region Implementation of ISiloShutdownParticipant

        public void BeginShutdown(Action finishShutdownAction)
        {
            if (this.tryFinishShutdown != null)
                return;

            this.tryFinishShutdown = finishShutdownAction;
            if (OnIdle == null)
            {
                OnIdle = tryFinishShutdown;
            }
            else
            {
                var old = OnIdle; // capture
                Action both = () => { old(); tryFinishShutdown(); };
                OnIdle = both;
            }

            if (CanFinishShutdown())
                tryFinishShutdown();
        }

        public bool CanFinishShutdown()
        {
            // todo: review - also check for pending continuations from ContinueWith
            bool canShutdown = RunQueueLength == 0 && Pool.BusyWorkerCount <= 1; // todo: verify this is right - count this thread?
            if (logger.IsVerbose) logger.Verbose("CanFinishShutdown {0} {1} => {2}", RunQueueLength, Pool.BusyWorkerCount, canShutdown);
            return canShutdown;
        }

        public void FinishShutdown()
        {
            logger.Info(ErrorCode.SchedulerFinishShutdown, "OrleansTaskScheduler.FinishShutdown");
            Stop();
        }

        public SiloShutdownPhase Phase { get { return SiloShutdownPhase.Scheduling; } }

        #endregion
    }
}
