using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.Storage.Pileus
{
    /// <summary>
    /// Records information about a version of an object.
    /// </summary>
    public class DataObjectState
    {
        private string name;        // object's name
        private DateTimeOffset timestamp; // last known version stamp 
        private string server;      // server known to hold this version
        private DateTimeOffset updated;   // when this state was recorded
        private string slaId;                    // SLA Id that added this entry

        // Note: we don't store the object's contents, but we could cache data here if desired.

        /// <summary>
        /// Constucts a new record of some object version that was read or written.
        /// </summary>
        /// <param name="name">Name of the object</param>
        /// <param name="ts">The version's timestamp</param>
        /// <param name="server">The server from which the obejct was read or to which the object was written</param>
        public DataObjectState(string name, DateTimeOffset ts, string server, string id)
        {
            this.name = name;
            this.timestamp = ts;
            this.server = server;
            this.updated = DateTimeOffset.Now;
            this.slaId = id;
        }
      
        /// <summary>
        /// Constucts a new record of some object version that was read or written.
        /// </summary>
        /// <param name="name">Name of the object</param>
        /// <param name="ts">The version's timestamp</param>
        /// <param name="server">The server from which the obejct was read or to which the object was written</param>
        public DataObjectState(string name, DateTimeOffset ts, string server)
        {
            this.name = name;
            this.timestamp = ts;
            this.server = server;
            this.updated = DateTimeOffset.Now;
            this.slaId = "";
        }
        // Note that there are no setter methods since session state records are treated as read-only.

        /// <summary>
        /// Gets the name of the object.
        /// </summary>
        public string Name
        {
            get { return name; }
        }

        /// <summary>
        /// Gets the version timestamp.
        /// </summary>
        public DateTimeOffset Timestamp
        {
            get { return timestamp; }
        }

        /// <summary>
        /// Gets the server that performed the read or write operation for which this version was returned.
        /// </summary>
        public string Server
        {
            get { return server; }
        }

        /// <summary>
        /// Gets the time when this record was created.
        /// </summary>
        public DateTimeOffset Updated
        {
            get { return updated; }
        }

        public string SlaId
        {
            get { return slaId; }
        }
    }
}
