using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;


namespace Orleans.Scheduler
{
    internal class TaskSchedulerUtils
    {
        internal static Task WrapWorkItemAsTask(IWorkItem todo, ISchedulingContext context, TaskScheduler sched)
        {
            Task task = new Task(state => RunWorkItemTask(todo, sched), context);
            return task;
        }

        private static void RunWorkItemTask(IWorkItem todo, TaskScheduler sched)
        {
            try
            {
                RuntimeContext.SetExecutionContext(todo.SchedulingContext, sched);
                todo.Execute();
            }
            finally
            {
                RuntimeContext.ResetExecutionContext();
            }
        }
    }
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
