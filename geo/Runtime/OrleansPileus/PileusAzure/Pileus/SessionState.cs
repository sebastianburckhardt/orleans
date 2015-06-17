using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices; 

namespace Microsoft.WindowsAzure.Storage.Pileus
{

    /// <summary>
    /// Maintains per-session state that is used to determine the set of acceptable replicas for a given desired consistency.
    /// Note that the server here means site.
    /// </summary>
    [Serializable()]
    public class SessionState
    {

        [NonSerialized()]
        public IDictionary<string, DataObjectState> objectsRead;

        [NonSerialized()]
        public IDictionary<string, DataObjectState> objectsWritten;

        public DateTimeOffset maxReadTimestamp; // Max timestamp across gets in a session, used for causal consistency

        /// <summary>
        /// Constructs a new (empty) session state.
        /// </summary>
        public SessionState()
        {
            this.objectsRead = new Dictionary<string, DataObjectState>();
            this.objectsWritten = new Dictionary<string, DataObjectState>();
            this.maxReadTimestamp = new DateTime(0); // Initialize it to zero ticks
            NumberOfWrites = 0;
            NumberOfReads = 0;
            CurrentMinute = -1;
        }

        /// <summary>
        /// Adds a new record indicating that an object was read.
        /// </summary>
        /// <param name="name">The name of the object</param>
        /// <param name="timestamp">The object's version timestamp</param>
        /// <param name="server">The server from which to object was read</param>
        public void RecordObjectRead(string name, DateTimeOffset timestamp, ServerState server, String SlaId)
        {
            try
            {
                IncrementRead();
                if (objectsRead.ContainsKey(name) && objectsRead[name].Timestamp >= timestamp)
                {
                    // previously read and recorded a newer version of this object
                    // so ignore this one
                    return;
                }
                DataObjectState record = new DataObjectState(name, timestamp, server.Name, SlaId);
                objectsRead[name] = record;
                if (server.HighTime < timestamp)
                {
                    // update lastest known version from server
                    server.HighTime = timestamp;
                }

                if (maxReadTimestamp < timestamp)
                {
                    // Update the max session timestamp 
                    maxReadTimestamp = timestamp;
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Adds a new record indicating that an object was written.
        /// </summary>
        /// <param name="name">The name of the object</param>
        /// <param name="timestamp">The object's new version timestamp</param>
        /// <param name="server">The server to which the write was performed</param>
        public void RecordObjectWritten(string name, DateTimeOffset timestamp, ServerState server)
        {
            IncrementWrite();
            DataObjectState record = new DataObjectState(name, timestamp, server.Name);
            objectsWritten[name] = record;
            if (server.HighTime < timestamp)
            {
                // update lastest known version for server
                server.HighTime = timestamp;
            }
        }

        // Records the number of reads and writes per minute
        public int NumberOfWrites { get; private set; }
        public int NumberOfReads { get; private set; }
        private int CurrentMinute { get; set; }

        public int GetNumberOfWritesPerMonth()
        {
            return NumberOfWrites * 60 * 24 * 30;
        }

        public int GetNumberOfReadsPerMonth()
        {
            return NumberOfReads * 60 * 24 * 30;
        }

        [MethodImplAttribute(MethodImplOptions.Synchronized)]
        private void IncrementWrite()
        {
            if (CurrentMinute == DateTime.Now.Minute)
                NumberOfWrites++;
            else
            {
                NumberOfWrites = 1;
                CurrentMinute = DateTime.Now.Minute;
            }
        }

        [MethodImplAttribute(MethodImplOptions.Synchronized)]
        private void IncrementRead()
        {
            if (CurrentMinute == DateTime.Now.Minute)
                NumberOfReads++;
            else
            {
                NumberOfReads = 1;
                CurrentMinute = DateTime.Now.Minute;
            }
        }

    }
}
