using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime.LogViews
{
    /// <summary>
    /// A log view provider that stores all the events in an event store.
    /// Supports multiple clusters connecting to the same primary storage (doing optimistic concurrency control via e-tags)
    ///<para>
    /// The log itself is transient, i.e. not actually saved to storage - only the latest view (snapshot) and some 
    /// metadata (the log position, and write flags) are stored in the primary. 
    /// </para>
    /// </summary>
    class EventStoreProvider
    {
    }
}
