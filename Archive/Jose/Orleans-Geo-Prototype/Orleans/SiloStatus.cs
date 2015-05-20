using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;



namespace Orleans
{
    /// <summary>
    /// This enumeration is for internal use only.
    /// </summary>
    public enum SiloStatus
    {
        /// <summary>
        /// For internal use only.
        /// </summary>
        None = 0,
        /// <summary>
        /// For internal use only.
        /// This silo was just created, but not started yet.
        /// </summary>
        Created = 1,
        /// <summary>
        /// For internal use only.
        /// This silo has just started, but not ready yet. It is attempting to join the cluster.
        /// </summary>
        Joining = 2,         
        /// <summary>
        /// For internal use only.
        /// This silo is alive and functional.
        /// </summary>
        Active = 3,
        /// <summary>
        /// For internal use only.
        /// This silo is shutting itself down.
        /// </summary>
        ShuttingDown = 4,    
        /// <summary>
        /// For internal use only.
        /// This silo is stopping itself down.
        /// </summary>
        Stopping = 5,
        /// <summary>
        /// For internal use only.
        /// This silo is de-activated/considered to be dead.
        /// </summary>
        Dead = 6
    }
}
