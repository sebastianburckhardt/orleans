using System;


namespace Orleans.Counters
{
    internal class SchedulerStatisticsGroup
    {
        private static CounterStatistic[] TurnsExecuted_PerWorkerThread_ApplicationTurns;
        private static CounterStatistic[] TurnsExecuted_PerWorkerThread_SystemTurns;
        private static CounterStatistic[] TurnsExecuted_PerWorkerThread_Null;
        private static CounterStatistic TurnsExecuted_ByAllWorkerThreads_TotalApplicationTurns;
        private static CounterStatistic TurnsExecuted_ByAllWorkerThreads_TotalSystemTurns;
        private static CounterStatistic TurnsExecuted_ByAllWorkerThreads_TotalNullTurns;

        private static CounterStatistic[] TurnsExecuted_PerWorkItemGroup;
        private static StringValueStatistic[] WorkItemGroupStatuses;
        private static CounterStatistic TurnsExecuted_ByAllWorkItemGroups_TotalApplicationTurns;
        private static CounterStatistic TurnsExecuted_ByAllWorkItemGroups_TotalSystem;
        private static CounterStatistic TotalPendingWorkItems;
        private static CounterStatistic TurnsExecuted_StartTotal;
        private static CounterStatistic TurnsExecuted_EndTotal;

        private static CounterStatistic TurnsEnQueuedTotal;
        private static CounterStatistic TurnsDeQueuedTotal;
        private static CounterStatistic TurnsDroppedTotal;
        private static CounterStatistic ClosureWorkItemsCreated;
        private static CounterStatistic ClosureWorkItemsExecuted;
        internal static CounterStatistic NumLongRunningTurns;
        internal static CounterStatistic NumLongQueueWaitTimes;

        private static HistogramValueStatistic turnLengthHistogram;
        private static readonly int turnLengthHistogramSize = 31;

        private static int workerThreadCounter;
        private static int workItemGroupCounter;
        private static object lockable;
        private static Logger logger;

        internal static void Init()
        {
            WorkItemGroupStatuses = new StringValueStatistic[1];
            workerThreadCounter = 0;
            workItemGroupCounter = 0;
            lockable = new object();

            if (StatisticsCollector.CollectGlobalShedulerStats)
            {
                TotalPendingWorkItems = CounterStatistic.FindOrCreate(StatNames.STAT_SCHEDULER_PENDINGWORKITEMS, false);
                TurnsEnQueuedTotal = CounterStatistic.FindOrCreate(StatNames.STAT_SCHEDULER_ITEMS_ENQUEUED_TOTAL);
                TurnsDeQueuedTotal = CounterStatistic.FindOrCreate(StatNames.STAT_SCHEDULER_ITEMS_DEQUEUED_TOTAL);
                TurnsDroppedTotal = CounterStatistic.FindOrCreate(StatNames.STAT_SCHEDULER_ITEMS_DROPPED_TOTAL);
                ClosureWorkItemsCreated = CounterStatistic.FindOrCreate(StatNames.STAT_SCHEDULER_CLOSURE_WORK_ITEMS_CREATED);
                ClosureWorkItemsExecuted = CounterStatistic.FindOrCreate(StatNames.STAT_SCHEDULER_CLOSURE_WORK_ITEMS_EXECUTED);
            }
            if (StatisticsCollector.CollectTurnsStats)
            {
                TurnsExecuted_ByAllWorkerThreads_TotalApplicationTurns = CounterStatistic.FindOrCreate(StatNames.STAT_SCHEDULER_TURNSEXECUTED_APPLICATION_BYALLWORKERTHREADS);
                TurnsExecuted_ByAllWorkerThreads_TotalSystemTurns = CounterStatistic.FindOrCreate(StatNames.STAT_SCHEDULER_TURNSEXECUTED_SYSTEM_BYALLWORKERTHREADS);
                TurnsExecuted_ByAllWorkerThreads_TotalNullTurns = CounterStatistic.FindOrCreate(StatNames.STAT_SCHEDULER_TURNSEXECUTED_NULL_BYALLWORKERTHREADS);

                TurnsExecuted_ByAllWorkItemGroups_TotalApplicationTurns = CounterStatistic.FindOrCreate(StatNames.STAT_SCHEDULER_TURNSEXECUTED_APPLICATION_BYALLWORKITEMGROUPS);
                TurnsExecuted_ByAllWorkItemGroups_TotalSystem = CounterStatistic.FindOrCreate(StatNames.STAT_SCHEDULER_TURNSEXECUTED_SYSTEM_BYALLWORKITEMGROUPS);
                turnLengthHistogram = ExponentialHistogramValueStatistic.Create_ExponentialHistogram_ForTiming(StatNames.STAT_SCHEDULER_TURN_LENGTH_HISTOGRAM, turnLengthHistogramSize);
                TurnsExecuted_StartTotal = CounterStatistic.FindOrCreate(StatNames.STAT_SCHEDULER_TURNSEXECUTED_TOTAL_START);
                TurnsExecuted_EndTotal = CounterStatistic.FindOrCreate(StatNames.STAT_SCHEDULER_TURNSEXECUTED_TOTAL_END);

                TurnsExecuted_PerWorkerThread_ApplicationTurns = new CounterStatistic[1];
                TurnsExecuted_PerWorkerThread_SystemTurns = new CounterStatistic[1];
                TurnsExecuted_PerWorkerThread_Null = new CounterStatistic[1];
                TurnsExecuted_PerWorkItemGroup = new CounterStatistic[1];
            }

            NumLongRunningTurns = CounterStatistic.FindOrCreate(StatNames.STAT_SCHEDULER_NUM_LONG_RUNNING_TURNS);
            NumLongQueueWaitTimes = CounterStatistic.FindOrCreate(StatNames.STAT_SCHEDULER_NUM_LONG_QUEUE_WAIT_TIMES);
            logger = Logger.GetLogger("SchedulerStatisticsGroup", Logger.LoggerType.Runtime);
        }

        internal static int RegisterWorkingThread(string threadName)
        {
            lock (lockable)
            {
                int i = workerThreadCounter;
                workerThreadCounter++;
                if (i == TurnsExecuted_PerWorkerThread_ApplicationTurns.Length)
                {
                    // need to resize the array
                    Array.Resize(ref TurnsExecuted_PerWorkerThread_ApplicationTurns, 2 * TurnsExecuted_PerWorkerThread_ApplicationTurns.Length);
                    Array.Resize(ref TurnsExecuted_PerWorkerThread_SystemTurns, 2 * TurnsExecuted_PerWorkerThread_SystemTurns.Length);
                    Array.Resize(ref TurnsExecuted_PerWorkerThread_Null, 2 * TurnsExecuted_PerWorkerThread_Null.Length);
                }
                TurnsExecuted_PerWorkerThread_ApplicationTurns[i] = CounterStatistic.FindOrCreate(new StatName(StatNames.STAT_SCHEDULER_TURNSEXECUTED_APPLICATION_PERTHREAD, threadName));
                TurnsExecuted_PerWorkerThread_SystemTurns[i] = CounterStatistic.FindOrCreate(new StatName(StatNames.STAT_SCHEDULER_TURNSEXECUTED_SYSTEM_PERTHREAD, threadName));
                TurnsExecuted_PerWorkerThread_Null[i] = CounterStatistic.FindOrCreate(new StatName(StatNames.STAT_SCHEDULER_TURNSEXECUTED_NULL_PERTHREAD, threadName));
                return i;
            }
        }

        internal static int RegisterWorkItemGroup(string workItemGroupName, ISchedulingContext context, Func<string> statusGetter)
        {
            lock (lockable)
            {
                int i = workItemGroupCounter;
                workItemGroupCounter++;
                if (i == TurnsExecuted_PerWorkItemGroup.Length)
                {
                    // need to resize the array
                    Array.Resize(ref TurnsExecuted_PerWorkItemGroup, 2 * TurnsExecuted_PerWorkItemGroup.Length);
                    Array.Resize(ref WorkItemGroupStatuses, 2 * WorkItemGroupStatuses.Length);
                }
                CounterStorage storage =  StatisticsCollector.ReportPerWorkItemStats(context) ? CounterStorage.LogAndTable : CounterStorage.DontStore;
                //CounterStorage storage = CounterStorage.LogAndTable;
                TurnsExecuted_PerWorkItemGroup[i] = CounterStatistic.FindOrCreate(new StatName(StatNames.STAT_SCHEDULER_ACTIVATION_TURNSEXECUTED_PERACTIVATION, workItemGroupName), storage);
                WorkItemGroupStatuses[i] = StringValueStatistic.FindOrCreate(new StatName(StatNames.STAT_SCHEDULER_ACTIVATION_STATUS_PERACTIVATION, workItemGroupName), statusGetter, storage);
                return i;
            }
        }

        internal static void UnRegisterWorkItemGroup(int workItemGroup)
        {
            Utils.SafeExecute(() => CounterStatistic.Delete(TurnsExecuted_PerWorkItemGroup[workItemGroup].Name),
                logger,
                () => String.Format("SchedulerStatisticsGroup.UnRegisterWorkItemGroup({0})", TurnsExecuted_PerWorkItemGroup[workItemGroup].Name));

            Utils.SafeExecute(() => StringValueStatistic.Delete(WorkItemGroupStatuses[workItemGroup].Name),
                logger,
                () => String.Format("SchedulerStatisticsGroup.UnRegisterWorkItemGroup({0})", WorkItemGroupStatuses[workItemGroup].Name));  
        }

        //----------- Global scheduler stats ---------------------//
        internal static void OnWorkItemEnqueue()
        {
            TotalPendingWorkItems.Increment();
            TurnsEnQueuedTotal.Increment();
        }

        internal static void OnWorkItemDequeue()
        {
            TotalPendingWorkItems.DecrementBy(1);
            TurnsDeQueuedTotal.Increment(); 
        }

        internal static void OnWorkItemDrop(int n)
        {
            TotalPendingWorkItems.DecrementBy(n);
            TurnsDroppedTotal.IncrementBy(n);
        }

        internal static void OnClosureWorkItemsCreated()
        {
            ClosureWorkItemsCreated.Increment();
        }

        internal static void OnClosureWorkItemsExecuted()
        {
            ClosureWorkItemsExecuted.Increment();
        }

        //------

        internal static void OnThreadStartsTurnExecution(int workerThread, ISchedulingContext context)
        {
            TurnsExecuted_StartTotal.Increment();
            if (context == null)
            {
                TurnsExecuted_PerWorkerThread_Null[workerThread].Increment();
                TurnsExecuted_ByAllWorkerThreads_TotalNullTurns.Increment();
            }
            else if (context.ContextType == SchedulingContextType.SystemTarget)
            {
                TurnsExecuted_PerWorkerThread_SystemTurns[workerThread].Increment();
                TurnsExecuted_ByAllWorkerThreads_TotalSystemTurns.Increment();
            }
            else if (context.ContextType == SchedulingContextType.Activation)
            {
                TurnsExecuted_PerWorkerThread_ApplicationTurns[workerThread].Increment();
                TurnsExecuted_ByAllWorkerThreads_TotalApplicationTurns.Increment();
            }
        }

        internal static void OnTurnExecutionStartsByWorkGroup(int workItemGroup, int workerThread, ISchedulingContext context)
        {
            TurnsExecuted_StartTotal.Increment();
            //if (StatisticsCollector.CollectPerWorkItemStats(context))
            {
                TurnsExecuted_PerWorkItemGroup[workItemGroup].Increment();
            }

            if (context == null)
            {
                throw new ArgumentException(String.Format("Cannot execute null context work item on work item group {0}.", workItemGroup));
            }
            else if (context.ContextType == SchedulingContextType.SystemTarget)
            {
                TurnsExecuted_ByAllWorkItemGroups_TotalSystem.Increment();
                TurnsExecuted_PerWorkerThread_SystemTurns[workerThread].Increment();
                TurnsExecuted_ByAllWorkerThreads_TotalSystemTurns.Increment();
            }
            else if (context.ContextType == SchedulingContextType.Activation)
            {
                TurnsExecuted_ByAllWorkItemGroups_TotalApplicationTurns.Increment();
                TurnsExecuted_PerWorkerThread_ApplicationTurns[workerThread].Increment();
                TurnsExecuted_ByAllWorkerThreads_TotalApplicationTurns.Increment();
            }
        }

        internal static void OnTurnExecutionEnd(TimeSpan timeSpan)
        {
            turnLengthHistogram.AddData(timeSpan);
            TurnsExecuted_EndTotal.Increment();
        }
    }
}

