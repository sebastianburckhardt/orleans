using Orleans;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GeoOrleans.Runtime.Strawman.ReplicatedGrains.Examples.Interfaces
{
    public interface ICommentsGrain : IGrain
    {
        // queries
        Task<Entry[]> Get(DateTime olderthan, int limit);

        // updates
        Task Post(Entry entry);
        Task Delete(Entry entry);
        Task DeleteRange(DateTime from, DateTime to);
    }

    [Serializable]
    public struct Entry : IEquatable<Entry>
    {
        public string User { get; set; }
        public DateTime Timestamp { get; set; }
        public string Text { get; set; }

        public bool Equals(Entry other)
        {
            return this.User == other.User && this.Timestamp == other.Timestamp && this.Text == other.Text;
        }
    }

}
