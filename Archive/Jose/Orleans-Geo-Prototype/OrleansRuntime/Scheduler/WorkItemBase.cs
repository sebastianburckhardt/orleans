using System;
using System.Collections.Generic;
using Orleans.Counters;

namespace Orleans.Scheduler
{
    internal abstract class WorkItemBase : IWorkItem
    {

        internal protected WorkItemBase()
        {
            //this.timeIntervalSinceQueued = TimeIntervalFactory.CreateTimeInterval(StatisticsCollector.MeasureFineGrainedTime);
        }

        public ISchedulingContext SchedulingContext { get; set; }
        public TimeSpan TimeSinceQueued 
        {
            //get { return timeIntervalSinceQueued.Elapsed; } 
            get { return Utils.Since(TimeQueued); } 
        }

        public abstract string Name { get; }

        public abstract WorkItemType ItemType { get; }

        //private readonly ITimeInterval timeIntervalSinceQueued;
        public DateTime TimeQueued { get; set; }

        public abstract void Execute();

        public bool IsSystem
        {
            get { return SchedulingUtils.IsSystemContext(this.SchedulingContext); }
        }

        //public void OnQueued()
        //{
        //    //timeIntervalSinceQueued.Restart();
        //    timeQueued = DateTime.UtcNow;
        //}
        
        public override string ToString()
        {
            return String.Format("[{0} WorkItem Name={1}, Ctx={2}]", 
                this.ItemType, 
                Name ?? "",
                (SchedulingContext == null) ? "null" : SchedulingContext.ToString()
            );
        }
    }
}

