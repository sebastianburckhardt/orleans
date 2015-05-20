using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Orleans
{
    /// <summary>
    /// Implemented by runtime objects that participate in shutdown process
    /// </summary>
    internal interface ISiloShutdownParticipant
    {
        /// <summary>
        /// Notification that shutdown process is beginning. Should do minimal work on this thread.
        /// </summary>
        void BeginShutdown(Action tryFinishShutdown);

        /// <summary>
        /// Poll if this participant is ready to finish shutdown.
        /// </summary>
        /// <returns>True if ready to finish</returns>
        bool CanFinishShutdown();

        /// <summary>
        /// Invoked after all participants have said they can finish shutdown,
        /// to actually finish the shutdown process.
        /// Must force a shutdown even if it is not ready.
        /// </summary>
        void FinishShutdown();

        SiloShutdownPhase Phase { get; }
    }

    internal enum SiloShutdownPhase
    {
        Early,
        Middle,
        Late,
        Messaging,
        Scheduling,
    }
}
