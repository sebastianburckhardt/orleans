using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace Orleans.Scheduler
{
    internal interface IWorkItem
    {
        string Name { get; }
        WorkItemType ItemType { get; }
        ISchedulingContext SchedulingContext { get; set; }
        TimeSpan TimeSinceQueued { get; }
        DateTime TimeQueued { get; set;  }
        bool IsSystem { get; }
        void Execute();
        //void OnQueued();
    }

    internal interface ITaskScheduler
    {
        void RunTask(Task task);
    }
}
