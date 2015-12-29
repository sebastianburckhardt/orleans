using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Orleans.Providers;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.EventSourcing
{
    public class JournaledStorageProvider : IJournaledStorageProvider
    {
        private static int counter;
        private readonly int id;
        private IEventStore eventStore;

        /// <summary> Name of this storage provider instance. </summary>
        /// <see cref="IProvider.Name"/>
        public string Name { get; private set; }

        /// <summary> Logger used by this storage provider instance. </summary>
        /// <see cref="IJournaledStorageProvider.Log"/>
        public Logger Log { get; private set; }

        public JournaledStorageProvider()
        {
            id = Interlocked.Increment(ref counter);
        }

        public Task ClearState(string streamName, int? expectedVersion)
        {
            return this.eventStore.DeleteStream(streamName, expectedVersion);
        }

        public Task Close()
        {
            return TaskDone.Done;
        }

        public Task Init(string name, IProviderRuntime providerRuntime, IProviderConfiguration config)
        {
            Name = name;
            Log = providerRuntime.GetLogger("Storage.JournaledStorageProvider." + id);

            // Instantiate event store based on provider configuration
            this.eventStore = Activator.CreateInstance(Type.GetType(config.GetProperty("EventStore", ""))) as IEventStore;
            this.eventStore.Init(config);

            return TaskDone.Done;
        }

        public async Task ReadState(string streamName, GrainState grainState)
        {
            IEventStream eventsToApply;

            if (this.eventStore is ISupportSnapshots)
            {
                var snapshotProvider = this.eventStore as ISupportSnapshots;
                var snapshot = await snapshotProvider.LoadLatest(streamName);

                // Deserialize the state from snapshot
                // grainState = snapshot.Payload;

                eventsToApply = await this.eventStore.LoadStreamFromVersion(streamName, snapshot.AsOfVersion + 1);
            }
            else
                eventsToApply = await this.eventStore.LoadStream(streamName);

            foreach(var @event in eventsToApply.Events)
                ApplyEvent(grainState, @event);
        }

        public Task WriteState(string streamName, int? expectedVersion, IEnumerable<object> newEvents)
        {
            return this.eventStore.AppendToStream(streamName, expectedVersion, newEvents);
        }

        private static void ApplyEvent(dynamic state, dynamic @event)
        {
            state.Apply(@event);
        }
    }
}
