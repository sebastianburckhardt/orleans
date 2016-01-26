using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Providers.EventStores
{
    /// <summary>
    /// Use this class to attach tags to events when storing in event stores.
    /// The tags are used for duplicate filtering when append operations encounter inconclusive exceptions.
    /// </summary>
    [Serializable]
    public class TaggedEvent
    {
        public object Event;
        public Guid Guid;

        public TaggedEvent() { }
        public TaggedEvent(object e, Guid g) { this.Event = e; this.Guid = g; }
    }
}
