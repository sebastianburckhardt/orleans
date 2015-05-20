using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Orleans.Scheduler
{
    [Flags]
    internal enum WorkItemType
    {
        None = 0x0000,
        Shutdown = 0x0001,        // Urgent shutdown of the activation
        Timer = 0x0002,           // Timer expiration
        Request = 0x0004,         // New request received
        Response = 0x0008,        // Response to a request received
        Resume = 0x0010,          // Resumption of a waiting (i.e., blocked) turn
        Closure = 0x0020,         // Deferred closure
        Deactivation = 0x0040,    // Controlled shutdown of the activation
        Clone = 0x0080,           // Clone the activation (for load)
        Task = 0x0100,            // Execute a TPL task within an activation
        WorkItemGroup = 0x0200,          // Special -- activation worker as work item
        Invoke = 0x0400,            // Invoke a method on an activation
        Callback = 0x0800,          // Callback with context when a response is received
        WithinRequest = 0x0d3b,   // Work items that can come in during the processing of a single request
        TaskWrapper = 0x1000,            // Execute a TPL task within an activation
        All = 0xffff
    }
}
