using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.Storage.Pileus
{
    /// <summary>
    /// Specifies the consistency that is requested and provided for read operations on replicated data.
    /// </summary>
    public enum Consistency 
    { 
        /// <summary>
        /// Returns the data that was most recently written by any client.
        /// </summary>
        Strong, 
        
        /// <summary>
        /// Returns the latest data that was written by this client 
        /// or optionally some later data written by someone else.
        /// If the client has performed no writes, then this is the same as eventual consistency.
        /// </summary>
        ReadMyWrites, 
        
        /// <summary>
        /// Returns data that is at least as up-to-date as data that was previously read by this client, 
        /// but not necessarily the most recently written data.
        /// </summary>
        MonotonicReads, 

        /// <summary>
        /// Combines ReadMyWrites and MonotonicReads consistency.
        /// </summary>
        Session,
        
        /// <summary>
        /// Causal consistency
        /// </summary>
        Causal,

        /// <summary>
        /// Returns data that is stale by at most N seconds,
        /// i.e. returns any data that was written more than N seconds ago or more recent data.
        /// The value of N must be specified elsewhere, such as in an SLA.
        /// With N set to zero, this is the same as strong consistency.
        /// With N set to some overly large value, this is the same as eventual consistency.
        /// </summary>
        Bounded,
 
        /// <summary>
        /// Combines Bounded consistency with the ReadMyWrites guarantee.
        /// </summary>
        BoundedReadMyWrites,

        /// <summary>
        /// Combines Bounded consistency with the MonotonicReads guarantee.
        /// </summary>
        BoundedMonotonicReads,
        
        /// <summary>
        /// Combines Bounded consistency with ReadMyWrites and MonotonicReads.
        /// </summary>
        BoundedSession,
        
        /// <summary>
        /// Returns any data that was previous written by any client.
        /// Repeatedly performing a read with this guarantee will eventually return the latest data,
        /// but there is no gauranteed bound on how long this will take.
        /// This is the weakest form of consistency
        /// </summary>
        Eventual 
    }
}
