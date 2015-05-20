#define PRIORITIZE_SYSTEM_TASKS

using System;
using System.Collections.Concurrent;
using System.Text;
using System.Threading;
using Orleans.Counters;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Orleans.Scheduler
{
    internal class WorkQueue
    {
        private BlockingCollection<IWorkItem> mainQueue;
        private BlockingCollection<IWorkItem> systemQueue;
        private BlockingCollection<IWorkItem>[] queueArray;
        private QueueTrackingStatistic mainQueueTracking;
        private QueueTrackingStatistic systemQueueTracking;
        private QueueTrackingStatistic tasksQueueTracking;

        public int Length { get { return mainQueue.Count + systemQueue.Count; } }

        internal WorkQueue()
        {
            mainQueue = new BlockingCollection<IWorkItem>(new ConcurrentBag<IWorkItem>());
            systemQueue = new BlockingCollection<IWorkItem>(new ConcurrentBag<IWorkItem>());
            queueArray = new BlockingCollection<IWorkItem>[] { systemQueue, mainQueue };

            if (StatisticsCollector.CollectQueueStats)
            {
                mainQueueTracking = new QueueTrackingStatistic("Scheduler.LevelOne.MainQueue");
                systemQueueTracking = new QueueTrackingStatistic("Scheduler.LevelOne.SystemQueue");
                tasksQueueTracking = new QueueTrackingStatistic("Scheduler.LevelOne.TasksQueue");
                mainQueueTracking.OnStartExecution();
                systemQueueTracking.OnStartExecution();
                tasksQueueTracking.OnStartExecution();
            }
        }

        public void Add(IWorkItem workItem)
        {
            //task.OnQueued();
            workItem.TimeQueued = DateTime.UtcNow;

            try
            {
#if PRIORITIZE_SYSTEM_TASKS
                if (workItem.IsSystem)
                {
    #if TRACK_DETAILED_STATS
                    if (StatisticsCollector.CollectQueueStats)
                    {
                        systemQueueTracking.OnEnQueueRequest(1, systemQueue.Count);
                    }
    #endif
                    systemQueue.Add(workItem);
                }
                else
                {
    #if TRACK_DETAILED_STATS
                    if (StatisticsCollector.CollectQueueStats)
                    {
                        mainQueueTracking.OnEnQueueRequest(1, mainQueue.Count);
                    }
    #endif
                    mainQueue.Add(workItem);                    
                }
#else
    #if TRACK_DETAILED_STATS
                    if (StatisticsCollector.CollectQueueStats)
                    {
                        mainQueueTracking.OnEnQueueRequest(1, mainQueue.Count);
                    }
    #endif
                mainQueue.Add(task);
#endif
#if TRACK_DETAILED_STATS
                if (StatisticsCollector.CollectGlobalShedulerStats)
                {
                    SchedulerStatisticsGroup.OnWorkItemEnqueue();
                }
#endif
            }
            catch (InvalidOperationException)
            {
                // Queue has been stopped; ignore the exception
            }
        }

        public IWorkItem Get(CancellationToken ct, TimeSpan timeout)
        {
            try
            {
                IWorkItem todo;
#if PRIORITIZE_SYSTEM_TASKS
                // TryTakeFromAny is a static method with no state held from one call to another, so each request is independent, 
                // and it doesn’t attempt to randomize where it next takes from, and does not provide any level of fairness across collections.
                // It has a “fast path” that just iterates over the collections from 0 to N to see if any of the collections already have data, 
                // and if it finds one, it takes from that collection without considering the others, so it will bias towards the earlier collections.  
                // If none of the collections has data, then it will fall through to the “slow path” of waiting on a collection of wait handles, 
                // one for each collection, at which point it’s subject to the fairness provided by the OS with regards to waiting on events. 
                if (BlockingCollection<IWorkItem>.TryTakeFromAny(queueArray, out todo, timeout) >= 0)
#else
                if (mainQueue.TryTake(out todo, timeout))
#endif
                {
#if TRACK_DETAILED_STATS
                    if (StatisticsCollector.CollectGlobalShedulerStats)
                    {
                        SchedulerStatisticsGroup.OnWorkItemDequeue();
                    }
#endif
                    return todo;
                }
                else
                {
                    return null;
                }
            }
            catch (InvalidOperationException)
            {
                return null;
            }
        }

        public IWorkItem GetSystem(CancellationToken ct, TimeSpan timeout)
        {
            try
            {
                IWorkItem todo;
#if PRIORITIZE_SYSTEM_TASKS
                if (systemQueue.TryTake(out todo, timeout))
#else
                if (mainQueue.TryTake(out todo, timeout))
#endif
                {
#if TRACK_DETAILED_STATS
                    if (StatisticsCollector.CollectGlobalShedulerStats)
                    {
                        SchedulerStatisticsGroup.OnWorkItemDequeue();
                    }
#endif
                    return todo;
                }
                else
                {
                    return null;
                }
            }
            catch (InvalidOperationException)
            {
                return null;
            }
        }


        public void DumpStatus(StringBuilder sb)
        {
            if (systemQueue.Count > 0)
            {
                sb.AppendLine("System Queue:");
                foreach (var workItem in systemQueue)
                {
                    sb.AppendFormat("  {0}", workItem).AppendLine();
                }
            }
            if (mainQueue.Count > 0)
            {
                sb.AppendLine("Main Queue:");
                foreach (var workItem in mainQueue)
                {
                    sb.AppendFormat("  {0}", workItem).AppendLine();
                }
            }
        }

        public void RunDownApplication()
        {
#if PRIORITIZE_SYSTEM_TASKS
            mainQueue.CompleteAdding();
#endif
        }

        public void RunDown()
        {
            mainQueue.CompleteAdding();
            systemQueue.CompleteAdding();
            if (StatisticsCollector.CollectQueueStats)
            {
                mainQueueTracking.OnStopExecution();
                systemQueueTracking.OnStopExecution();
                tasksQueueTracking.OnStopExecution();
            }
        }

        public void Dispose()
        {
            queueArray = null;

            if (mainQueue != null)
            {
                mainQueue.Dispose();
                mainQueue = null;
            }

            if (systemQueue != null)
            {
                systemQueue.Dispose();
                systemQueue = null;
            }

            GC.SuppressFinalize(this);
        }
    }
}
