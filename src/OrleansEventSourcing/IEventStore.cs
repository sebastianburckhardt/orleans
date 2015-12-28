using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.EventSourcing
{
    public interface IEventStore
    {
        Task<IEnumerable<object>> LoadStream(string streamId);

        Task<IEnumerable<object>> LoadStreamFromVersion(string streamId, int version);

        Task AppendToStream(string streamId, IEnumerable<object> events);

        Task DeleteStream(string streamId);
    }

    public interface ISnapshot
    {
        int AsOfVersion { get; }
        object Payload { get; }
    }

    public interface ISupportSnapshots
    {
        Task<ISnapshot> LoadLatest(string streamId);

        Task Create(string streamId, int version, object payload);
    }
}
