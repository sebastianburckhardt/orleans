using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans.LogViews;
using Orleans.Providers;

namespace Orleans.Providers.EventStores
{
    public class MemoryEventStore : EventStoreProviderBase, ILogViewProvider
    {
        private readonly ConcurrentDictionary<string, List<object>> streams = new ConcurrentDictionary<string, List<object>>();

        public override Task Init(IProviderConfiguration config)
        {
            return TaskDone.Done;
        }

        public override Task AppendToStream(string streamName, int? expectedVersion, IEnumerable<object> events)
        {
            var stream = streams.GetOrAdd(streamName, x => new List<object>());

            if (expectedVersion.HasValue && expectedVersion.Value != stream.Count)
                throw new OptimisticConcurrencyException(streamName, expectedVersion.Value);

            stream.AddRange(events);

            return TaskDone.Done;
        }

        public override Task DeleteStream(string streamName, int? expectedVersion)
        {
            if(streams.ContainsKey(streamName))
            {
                var stream = streams[streamName];

                if (expectedVersion.HasValue && expectedVersion.Value != stream.Count)
                    throw new OptimisticConcurrencyException(streamName, expectedVersion.Value);

                streams.TryRemove(streamName, out stream);
            }

            return TaskDone.Done;
        }

        public override Task<IEventStream> LoadStream(string streamName)
        {
            var stream = streams.GetOrAdd(streamName, x => new List<object>());

            return Task.FromResult(new EventStream(streamName, stream.Count, stream) as IEventStream);
        }

        public override Task<IEventStream> LoadStreamFromVersion(string streamName, int version)
        {
            var stream = streams.GetOrAdd(streamName, x => new List<object>());

            return Task.FromResult(new EventStream(streamName, stream.Count, stream.Skip(version).ToList()) as IEventStream);
        }
    }
}