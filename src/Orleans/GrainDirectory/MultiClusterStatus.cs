using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.GrainDirectory
{
    /// <summary>
    /// Status of a directory entry with respect to multi-cluster registration
    /// </summary>
    internal enum MultiClusterStatus
    {
        OWNED,                      // Registration is owned by this cluster.
        DOUBTFUL,                   // Failed to contact one or more clusters while registering, so may be a duplicate.

        CACHED,                     // Cached reference to a registration owned by a remote cluster. 

        REQUESTED_OWNERSHIP,        // The cluster is in the process of checking remote clusters for existing registrations.
        RACE_LOSER,                 // The cluster lost a race condition.
    }

}
