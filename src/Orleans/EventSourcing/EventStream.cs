using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.EventSourcing
{
    public class EventStream : IEventStream
    {
        public EventStream(string streamName, int version, IReadOnlyCollection<object> events)
        {
            this.StreamName = streamName;
            this.Version = version;
            this.Events = events;
        }

        public string StreamName { get; set;  }
        public int Version { get; set;  }
        public IReadOnlyCollection<object> Events { get; set; }
    }
}
