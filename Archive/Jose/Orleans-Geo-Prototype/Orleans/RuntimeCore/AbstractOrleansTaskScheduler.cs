using System;
using System.Threading.Tasks;

namespace Orleans
{
    internal abstract class AbstractOrleansTaskScheduler : TaskScheduler
    {
        public abstract TaskScheduler GetTaskScheduler(ISchedulingContext context);
    }
}