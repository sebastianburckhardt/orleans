#if !DISABLE_STREAMS

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Orleans.Streams
{
    /// <summary>
    /// </summary>
    [Serializable]
    [Immutable]
    public class QueueId : IRingIdentifier<QueueId>, IEquatable<QueueId>, IComparable<QueueId>
    {
        private static readonly object lockable = new object();
        private static readonly int InternCacheInitialSize = InternerConstants.Size_Large;
        private static readonly TimeSpan InternCacheCleanupInterval = InternerConstants.DefaultCacheCleanupFreq;
        private static Interner<uint, QueueId> queueIdInternCache;

        private readonly uint queueId;
        private readonly uint uniformHashCache;

        //TODO: need to integrate with Orleans serializer to really use Interner.
        private QueueId(uint id, uint hash)
        {
            queueId = id;
            uniformHashCache = hash;
        }

        public static QueueId GetQueueId(uint id, uint hash)
        {
            return FindOrCreateQueueId(id, hash);
        }

        private static QueueId FindOrCreateQueueId(uint id, uint hash)
        {
            if (queueIdInternCache == null)
            {
                lock (lockable)
                {
                    if (queueIdInternCache == null)
                    {
                        queueIdInternCache = new Interner<uint, QueueId>(InternCacheInitialSize, InternCacheCleanupInterval);
                    }
                }
            }
            return queueIdInternCache.FindOrCreate(id, () => new QueueId(id, hash));
        }

        #region IComparable<QueueId> Members

        public int CompareTo(QueueId other)
        {
            return queueId.CompareTo(other.queueId);
        }

        #endregion

        #region IEquatable<QueueId> Members

        public virtual bool Equals(QueueId other)
        {
            return other != null && queueId.Equals(other.queueId);
        }

        #endregion

        public override bool Equals(object obj)
        {
            var o = obj as QueueId;
            return o != null && queueId.Equals(o.queueId);
        }

        public override int GetHashCode()
        {
            return queueId.GetHashCode();
        }

        public uint GetUniformHashCode()
        {
            return uniformHashCache;
        }

        //public uint GetUniformHashCode()
        //{
        //    if (uniformHashCache == 0)
        //    {
        //        JenkinsHash jenkinsHash = new JenkinsHash();
        //        uint n0 = queueId;
  
        //        uint a = jenkinsHash.ComputeHash(0, n0, 0);
        //        uniformHashCache = unchecked((uint)a);
        //    }
        //    return uniformHashCache;
        //}

        public override string ToString()
        {
            return String.Format("aq-stream-{0}", queueId.ToString());
        }

        public string ToStringWithHashCode()
        {
            return String.Format("{0}-0x{1, 8:X8}", this.ToString(), this.GetUniformHashCode());
        }
    }
}

#endif
