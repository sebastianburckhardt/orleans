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
        /// <see cref="IStorageProvider.Log"/>
        public Logger Log { get; private set; }

        public JournaledStorageProvider()
        {
            id = Interlocked.Increment(ref counter);
        }

        public Task ClearStateAsync(string grainType, GrainReference grainReference)
        {
            var streamId = GetStreamId(grainType, grainReference);

            return this.eventStore.DeleteStream(streamId);
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

            return TaskDone.Done;
        }

        public async Task ReadStateAsync(string grainType, GrainReference grainReference, GrainState grainState)
        {
            var streamId = GetStreamId(grainType, grainReference);

            IEnumerable<object> eventsToApply;

            if (this.eventStore is ISupportSnapshots)
            {
                var snapshotProvider = this.eventStore as ISupportSnapshots;
                var snapshot = await snapshotProvider.LoadLatest(streamId);

                // Deserialize the state from snapshot
                // grainState = snapshot.Payload;

                eventsToApply = await this.eventStore.LoadStreamFromVersion(streamId, snapshot.AsOfVersion + 1);
            }
            else
                eventsToApply = await this.eventStore.LoadStream(streamId);

            foreach(var @event in eventsToApply)
                ApplyEvent(grainState, @event);
        }

        public Task WriteStateAsync(string grainType, GrainReference grainReference, IEnumerable<object> newEvents)
        {
            var streamId = GetStreamId(grainType, grainReference);

            return this.eventStore.AppendToStream(streamId, newEvents);
        }

        private static string GetStreamId(string grainType, GrainReference grainReference)
        {
            return string.Format("{0}-{1}", grainType, grainReference.ToKeyString());
        }

        private static void ApplyEvent(dynamic state, dynamic @event)
        {
            state.Apply(@event);
        }
    }
}
