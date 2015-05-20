using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Orleans;

namespace Orleans
{
    /// <summary>
    /// Reason for aborting a task
    /// </summary>
    [Serializable]
    internal class ReasonDetail
    {
        /// <summary>
        /// Whether it can be retried
        /// </summary>
        public bool IsRetriable { get; private set; }

        public string Description { get; private set; }

        public static ReasonDetail ShuttingDown = new ReasonDetail {IsRetriable = true, Description = "Shutting down"};

        public static readonly ReasonDetail ReleaseFailed = new ReasonDetail { IsRetriable = true, Description = "ReleaseFailed" };

        public static readonly ReasonDetail Dependent = new ReasonDetail { IsRetriable = true, Description = "Dependent" };
        
        public static readonly ReasonDetail SyncFailed = new ReasonDetail { IsRetriable = true, Description = "SyncFailed" };

        public static readonly ReasonDetail AtomicityFailure = new ReasonDetail { IsRetriable = true, Description = "AtomicityFailure" };

        public static readonly ReasonDetail Busy = new ReasonDetail { IsRetriable = true, Description = "Busy" };

        public static readonly ReasonDetail NotLocal = new ReasonDetail { IsRetriable = true, Description = "NotLocal" };

        public static readonly ReasonDetail Creating = new ReasonDetail { IsRetriable = true, Description = "Creating" };

        public static readonly ReasonDetail Strategy = new ReasonDetail { IsRetriable = true, Description = "Strategy" };

        // incorrect state of task system itself
        public static readonly ReasonDetail RuntimeError = new ReasonDetail { IsRetriable = true, Description = "RuntimeError" };
        
        // application-level failure, e.g. uncaught exception
        public static ReasonDetail ApplicationFailure(bool retriable, string description)
        {
            return new ReasonDetail { IsRetriable = retriable, Description = "Application Failure: " + description };
        }

        public static readonly ReasonDetail SiloFailure = new ReasonDetail { IsRetriable = true, Description = "SiloFailure" };

        public static readonly ReasonDetail Aborted = new ReasonDetail { IsRetriable = true, Description = "Aborted" };

        public static readonly ReasonDetail Ordering = new ReasonDetail {IsRetriable = true, Description = "Ordering"};

        public static ReasonDetail NotifyingGovernor = new ReasonDetail {IsRetriable = true, Description = "NotifyingGovernor"};
        
        public static ReasonDetail DuplicateRequest = new ReasonDetail { IsRetriable = false, Description = "DuplicateTask" };

        public override string ToString()
        {
            return String.Format("ReasonDetail {0}retriable: {1}", IsRetriable ? "" : "not ", Description);
        }

        public bool ReasonEquals(ReasonDetail other)
        {
            return Description == other.Description;
        }
    }
}
