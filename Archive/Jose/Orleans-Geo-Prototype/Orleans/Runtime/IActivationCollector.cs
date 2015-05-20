using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime
{
    internal interface IActivationCollector
    {
        /// <summary>
        /// Schedules an activation to be collected at a specified point in the future.
        /// </summary>
        /// <param name="item">The activation to be collected.</param>
        /// <param name="timeout">The amount of time that should pass before the activation can be collected.</param>
        void ScheduleCollection(ActivationData item, TimeSpan timeout);

        /// <summary>
        /// Attempt to reschedule collection.
        /// </summary>
        /// <param name="item">The activation to be rescheduled.</param>
        /// <param name="timeout">The new timeout.</param>
        /// <returns></returns>
        bool TryRescheduleCollection(ActivationData item, TimeSpan timeout);
    }
}
