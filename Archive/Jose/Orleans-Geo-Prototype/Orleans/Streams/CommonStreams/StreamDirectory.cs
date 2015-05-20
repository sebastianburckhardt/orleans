#if !DISABLE_STREAMS 

using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using Orleans.Streams;

namespace Orleans.Streams
{
    // Stores all streams in this silo.
    internal class StreamDirectory
    {
        private class Pair : IEquatable<Pair>
        {
            private readonly StreamId _streamId;
            private readonly object   _executingEntityIdentity;

            internal Pair(StreamId streamId, object executingEntityIdentity)
            {
                _streamId = streamId;
                _executingEntityIdentity = executingEntityIdentity;
            }

            #region IEquatable<Pair> Members

            public bool Equals(Pair other)
            {
                if (other == null) return false;
                return _streamId.Equals(other._streamId) && Object.Equals(_executingEntityIdentity, other._executingEntityIdentity);
            }

            #endregion

            public override bool Equals(object obj)
            {
                var o = obj as Pair;
                return o != null && this.Equals(o);
            }

            public override int GetHashCode()
            {
                return _streamId.GetHashCode() ^ (_executingEntityIdentity == null ? 0 : _executingEntityIdentity.GetHashCode());
            }
        }

        private readonly ConcurrentDictionary<Pair, object> _allStreams;

        internal StreamDirectory()
        {
            _allStreams = new ConcurrentDictionary<Pair, object>();
        }

        internal IAsyncStream<T> GetOrAddStream<T>(StreamId streamId, object executingEntityIdentity, Func<IAsyncStream<T>> streamCreator)
        {
            object stream = _allStreams.GetOrAdd(new Pair(streamId, executingEntityIdentity), (Pair pair) => { return streamCreator(); });
            return stream as IAsyncStream<T>;
        }
    }
}

#endif