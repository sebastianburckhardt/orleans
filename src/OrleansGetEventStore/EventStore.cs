using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Exceptions;
using EventStore.ClientAPI.SystemData;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Orleans;
using Orleans.EventSourcing;
using Orleans.Providers;

namespace OrleansGetEventStore
{
    public class EventStore : IEventStore
    {
        private static readonly JsonSerializerSettings SerializerSettings = new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.None };

        private IEventStoreConnection Connection;

        public Task Init(IProviderConfiguration config)
        {
            var settings = ConnectionSettings.Create()
                .KeepReconnecting().KeepRetrying()
                .SetDefaultUserCredentials(new UserCredentials(config.GetProperty("Username", "admin"), config.GetProperty("Password", "changeit")));

            var connectionStringParts = config.GetProperty("Host", "localhost:1113").Split(':');
            var hostName = connectionStringParts[0];
            var hostPort = int.Parse(connectionStringParts[1]);
            var hostAddress = Dns.GetHostAddresses(hostName).First(a => a.AddressFamily == AddressFamily.InterNetwork);

            this.Connection = EventStoreConnection.Create(settings, new IPEndPoint(hostAddress, hostPort));
            return this.Connection.ConnectAsync();
        }

        public async Task AppendToStream(string streamName, int? expectedVersion, IEnumerable<object> events)
        {
            var writePageSize = 500;
            var commitHeaders = new Dictionary<string, object>();

            var eventsToSave = events.Select(e => ToEventData(Guid.NewGuid(), e, commitHeaders)).ToArray();

            if (eventsToSave.Length == 0)
                return;

            var version = expectedVersion.HasValue 
                ? expectedVersion.Value == 0 
                    ? ExpectedVersion.NoStream 
                    : expectedVersion.Value - 1
                : ExpectedVersion.Any;

            if (eventsToSave.Length < writePageSize)
            {
                try
                {
                    await this.Connection.AppendToStreamAsync(streamName, version, eventsToSave);
                }
                catch (WrongExpectedVersionException)
                {
                    throw new OptimisticConcurrencyException(streamName, version);
                }
            }
            else
            {
                try
                {
                    var transaction = await this.Connection.StartTransactionAsync(streamName, version);

                    var position = 0;
                    while (position < eventsToSave.Length)
                    {
                        var pageEvents = eventsToSave.Skip(position).Take(writePageSize);
                        await transaction.WriteAsync(pageEvents);
                        position += writePageSize;
                    }

                    await transaction.CommitAsync();
                }
                catch (WrongExpectedVersionException)
                {
                    throw new OptimisticConcurrencyException(streamName, version);
                }
            }
        }

        public Task DeleteStream(string streamName, int? expectedVersion)
        {
            try
            {
                return this.Connection.DeleteStreamAsync(streamName, expectedVersion.GetValueOrDefault(ExpectedVersion.Any));
            }
            catch (WrongExpectedVersionException)
            {
                throw new OptimisticConcurrencyException(streamName, expectedVersion.GetValueOrDefault(ExpectedVersion.Any));
            }
        }

        public Task<IEventStream> LoadStream(string streamName)
        {
            return LoadStreamFromVersion(streamName, 0);
        }

        public async Task<IEventStream> LoadStreamFromVersion(string streamName, int version)
        {
            var sliceStart = version;
            var readPageSize = 500;
            StreamEventsSlice currentSlice;

            var events = new List<object>();

            do
            {
                currentSlice = await this.Connection.ReadStreamEventsForwardAsync(streamName, sliceStart, readPageSize, true);

                if (currentSlice.Status == SliceReadStatus.StreamNotFound)
                    return new EventStream(streamName, -1, new List<object>());

                if (currentSlice.Status == SliceReadStatus.StreamDeleted)
                    return new EventStream(streamName, -1, new List<object>());

                sliceStart = currentSlice.NextEventNumber;

                foreach (var evnt in currentSlice.Events)
                    events.Add(DeserializeEvent(evnt.Event));

            } while (version >= currentSlice.NextEventNumber && !currentSlice.IsEndOfStream);

            return new EventStream(streamName, events.Count, events);
        }

        public static object DeserializeEvent(RecordedEvent @event)
        {
            // Backwards compatibility
            var metadata = DeserializeMetadata(@event.Metadata);

            var eventTypeProperty = metadata.Property("EventClrTypeName");

            if (eventTypeProperty != null)
                return DeserializeEvent((string)eventTypeProperty.Value, @event.Data);
            // Backwards compatibility

            return DeserializeEvent(@event.EventType, @event.Data);
        }
        public static JObject DeserializeMetadata(byte[] metadata)
        {
            return JObject.Parse(Encoding.UTF8.GetString(metadata));
        }

        public static object DeserializeEvent(string eventClrTypeName, byte[] data)
        {
            return JsonConvert.DeserializeObject(Encoding.UTF8.GetString(data), Type.GetType(eventClrTypeName));
        }

        private static EventData ToEventData(Guid eventId, object evnt, IDictionary<string, object> headers)
        {
            var data = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(evnt, SerializerSettings));

            var eventHeaders = new Dictionary<string, object>(headers);

            var metadata = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(eventHeaders, SerializerSettings));
            var eventType = evnt.GetType();
            var typeName = string.Concat(eventType.FullName, ", ", eventType.Assembly.GetName().Name);

            return new EventData(eventId, typeName, true, data, metadata);
        }
    }
}