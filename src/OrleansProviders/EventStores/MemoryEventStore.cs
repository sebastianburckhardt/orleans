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
    /// <summary>
    /// An in-memory event-store-based log view provider for testing purposes.
    /// </summary>
    public class MemoryEventStore : EventStoreProviderBase, ILogViewProvider
    {
        private readonly ConcurrentDictionary<string, List<object>> streams = new ConcurrentDictionary<string, List<object>>();

        public override Task Init(IProviderConfiguration config)
        {
            return TaskDone.Done;
        }

        private const int simulated_IO_delay = 1;

        public override async Task AppendToStream(string streamName, int? expectedVersion, IEnumerable<object> events)
        {
            var stream = streams.GetOrAdd(streamName, x => new List<object>());

            await Task.Delay(simulated_IO_delay); // simulate I/O

            lock (stream)
            {
                if (expectedVersion.HasValue && expectedVersion.Value != stream.Count)
                    throw new OptimisticConcurrencyException(streamName, expectedVersion.Value);

                stream.AddRange(events);
            }

            await Task.Delay(simulated_IO_delay); // simulate I/O
        }

        public override Task DeleteStream(string streamName, int? expectedVersion)
        {

            List<object> stream;

            if (streams.TryGetValue(streamName, out stream))
                lock (stream)
                {
                    if (expectedVersion.HasValue && expectedVersion.Value != stream.Count)
                        throw new OptimisticConcurrencyException(streamName, expectedVersion.Value);

                    List<object> ignored;
                    streams.TryRemove(streamName, out ignored);
                }

            return TaskDone.Done;
        }

        public override async Task<IEventStream> LoadStream(string streamName)
        {
            var stream = streams.GetOrAdd(streamName, x => new List<object>());

            await Task.Delay(simulated_IO_delay); // simulate I/O

            IEventStream result;

            lock (stream)
              result = new EventStream(streamName, stream.Count, stream.ToList()) as IEventStream;

            await Task.Delay(simulated_IO_delay); // simulate I/O

            return result;
        }

        public override async Task<IEventStream> LoadStreamFromVersion(string streamName, int version)
        {
            var stream = streams.GetOrAdd(streamName, x => new List<object>());

            await Task.Delay(simulated_IO_delay); // simulate I/O

            IEventStream result;

            lock (stream)
                result = new EventStream(streamName, stream.Count, stream.Skip(version).ToList()) as IEventStream;

            await Task.Delay(simulated_IO_delay); // simulate I/O

            return result;
        }
    }
}