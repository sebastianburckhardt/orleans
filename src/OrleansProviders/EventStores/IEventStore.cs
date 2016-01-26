using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.Providers;

namespace Orleans.Providers.EventStores
{
    public interface IEventStream
    {
        string StreamName { get; }
        int Version { get; }
        IReadOnlyCollection<object> Events { get; }
    }

    public interface IEventStore
    {
        Task Init(IProviderConfiguration config);

        Task<IEventStream> LoadStream(string streamName);

        Task<IEventStream> LoadStreamFromVersion(string streamName, int version);

        Task AppendToStream(string streamName, int? expectedVersion, IEnumerable<object> events);

        Task DeleteStream(string streamName, int? expectedVersion);
    }



    public interface ISnapshot
    {
        int AsOfVersion { get; }
        object Payload { get; }
    }

    public interface ISupportSnapshots
    {
        Task<ISnapshot> LoadLatest(string streamName);

        Task Create(string streamName, int version, object payload);
    }
}
