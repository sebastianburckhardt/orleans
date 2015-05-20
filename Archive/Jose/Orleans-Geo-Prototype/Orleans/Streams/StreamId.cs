#if !DISABLE_STREAMS

using System;
using System.Runtime.Serialization;

namespace Orleans.Streams
{
    /// <summary>
    /// </summary>
    [Serializable]
    [Immutable]
    public class StreamId : IRingIdentifier<StreamId>, IEquatable<StreamId>, IComparable<StreamId>, ISerializable
    {
        [NonSerialized]
        private static readonly int InternCacheInitialSize = InternerConstants.Size_Large;
        [NonSerialized]
        private static readonly TimeSpan InternCacheCleanupInterval = InternerConstants.DefaultCacheCleanupFreq;

        private readonly Guid guid;
        
        [NonSerialized]
        private uint uniformHashCache;
        [NonSerialized]
        private static readonly Lazy<Interner<Guid, StreamId>> streamIdInternCache = new Lazy<Interner<Guid, StreamId>>(
            () => new Interner<Guid, StreamId>(InternCacheInitialSize, InternCacheCleanupInterval));

        // GK: Keep public, similar to GrainId.GetPrimaryKey. Some app scenarios might need that.
        public Guid AsGuid { get { return guid; } }

        //TODO: need to integrate with Orleans serializer to really use Interner.
        private StreamId(Guid id)
        {
            guid = id;
        }

        public static StreamId GetStreamId(Guid guid)
        {
            return FindOrCreateStreamId(guid);
        }

        public static StreamId NewRandomStreamId()
        {
            return FindOrCreateStreamId(Guid.NewGuid());
        }

        private static StreamId FindOrCreateStreamId(Guid id)
        {
            return streamIdInternCache.Value.FindOrCreate(id, () => new StreamId(id));
        }

        #region IComparable<StreamId> Members

        public int CompareTo(StreamId other)
        {
            return guid.CompareTo(other.guid);
        }

        #endregion

        #region IEquatable<StreamId> Members

        public virtual bool Equals(StreamId other)
        {
            return other != null && guid.Equals(other.guid);
        }

        #endregion

        public override bool Equals(object obj)
        {
            var o = obj as StreamId;
            return o != null && guid.Equals(o.guid);
        }

        public override int GetHashCode()
        {
            return guid.GetHashCode();
        }

        public uint GetUniformHashCode()
        {
            if (uniformHashCache == 0)
            {
                JenkinsHash jenkinsHash = JenkinsHash.Factory.GetHashGenerator();
                byte[] guidBytes = guid.ToByteArray();
                uniformHashCache = jenkinsHash.ComputeHash(guidBytes);
            }
            return uniformHashCache;
        }

        public override string ToString()
        {
            return guid.ToString();
        }

        internal string ToShortString()
        {
            return guid.ToShortString();
        }

        internal string ToStringWithHashCode()
        {
            return String.Format("{0}/x{1, 8:X8}", guid.ToShortString(), this.GetUniformHashCode());
        }

        internal string ToParsableString()
        {
            return guid.ToString();
        }

        internal static StreamId FromParsableString(string str)
        {
            // NOTE: This function must be the "inverse" of ToParsableString, and data must round-trip reliably.
            Guid g = Guid.Parse(str);
            return GetStreamId(g);
        }

        
        #region ISerializable Members

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            // Use the AddValue method to specify serialized values.
            info.AddValue("Guid", guid, typeof(Guid));
        }

        // The special constructor is used to deserialize values. 
        protected StreamId(SerializationInfo info, StreamingContext context)
        {
            // Reset the property value using the GetValue method.
            guid = (Guid)info.GetValue("Guid", typeof(Guid));
        }

        #endregion
    }
}

#endif
