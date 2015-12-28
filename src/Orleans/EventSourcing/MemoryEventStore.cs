using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.EventSourcing
{
    class MemoryEventStore : IEventStore
    {
        private readonly ConcurrentDictionary<string, List<object>> streams = new ConcurrentDictionary<string, List<object>>();

        public Task AppendToStream(string streamId, IEnumerable<object> events)
        {
            var stream = streams.GetOrAdd(streamId, x => new List<object>());
            stream.AddRange(events);

            return TaskDone.Done;
        }

        public Task DeleteStream(string streamId)
        {
            List<object> dummy;

            streams.TryRemove(streamId, out dummy);

            return TaskDone.Done;
        }

        public Task<IEnumerable<object>> LoadStream(string streamId)
        {
            var stream = streams.GetOrAdd(streamId, x => new List<object>());

            return Task.FromResult(stream as IEnumerable<object>);
        }

        public Task<IEnumerable<object>> LoadStreamFromVersion(string streamId, int version)
        {
            var stream = streams.GetOrAdd(streamId, x => new List<object>());

            return Task.FromResult(stream.Skip(version));
        }
    }
}
