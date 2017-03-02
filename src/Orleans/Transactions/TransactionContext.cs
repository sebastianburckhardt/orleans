
using System;
using System.Collections.Generic;
using System.Runtime.Remoting.Messaging;

namespace Orleans.Transactions
{
    public static class TransactionContext
    {
        internal const string TransactionInfoHeader = "#TC_TI";
        internal const string Orleans_TransactionContext_Key = "#ORL_TC";

        internal static TransactionInfo GetTransactionInfo()
        {
            Dictionary<string, object> values = GetContextData();
            object result;
            if ((values != null) && values.TryGetValue(TransactionInfoHeader, out result))
            {
                return result as TransactionInfo;
            }
            return null;
        }

        internal static void SetTransactionInfo(TransactionInfo info)
        {
            Dictionary<string, object> values = GetContextData();

            if (values == null)
            {
                values = new Dictionary<string, object>();
            }
            else
            {
                // Have to copy the actual Dictionary value, mutate it and set it back.
                // http://blog.stephencleary.com/2013/04/implicit-async-context-asynclocal.html
                // This is since LLC is only copy-on-write copied only upon LogicalSetData.
                values = new Dictionary<string, object>(values);
            }
            values[TransactionInfoHeader] = info;
            SetContextData(values);
        }

        internal static void Clear()
        {
            // Remove the key to prevent passing of its value from this point on
            CallContext.FreeNamedDataSlot(Orleans_TransactionContext_Key);
        }

        private static void SetContextData(Dictionary<string, object> values)
        {
            CallContext.LogicalSetData(Orleans_TransactionContext_Key, values);
        }

        private static Dictionary<string, object> GetContextData()
        {
            return (Dictionary<string, object>)CallContext.LogicalGetData(Orleans_TransactionContext_Key);
        }
    }

    [Serializable]
    public class TransactionInfo
    {
        public TransactionInfo(long id, bool readOnly = false)
        {
            TransactionId = id;
            IsReadOnly = readOnly;
            IsAborted = false;
            PendingCalls = 0;
            ReadSet = new Dictionary<ITransactionalGrain, GrainVersion>();
            WriteSet = new Dictionary<ITransactionalGrain, int>();
            DependentTransactions = new HashSet<long>();
        }

        /// <summary>
        /// Constructor used when TransactionInfo is transferred to a request
        /// </summary>
        /// <param name="other"></param>
        public TransactionInfo(TransactionInfo other)
        {
            TransactionId = other.TransactionId;
            IsReadOnly = other.IsReadOnly;
            IsAborted = other.IsAborted;
            PendingCalls = 0;
            ReadSet = new Dictionary<ITransactionalGrain, GrainVersion>();
            WriteSet = new Dictionary<ITransactionalGrain, int>();
            DependentTransactions = new HashSet<long>();
            //ReadSet = new Dictionary<ITransactionalGrain, WriteVersion>(other.ReadSet);
            //WriteSet = new Dictionary<ITransactionalGrain, int>(other.WriteSet);
            //DependentTransactions = new HashSet<long>(other.DependentTransactions);
        }

        public long TransactionId { get; private set; }

        public bool IsReadOnly { get; private set; }

        public bool IsAborted { get; set; }

        public int PendingCalls { get; set; }

        public Dictionary<ITransactionalGrain, GrainVersion> ReadSet { get; private set; }
        public Dictionary<ITransactionalGrain, int> WriteSet { get; private set; }
        public HashSet<long> DependentTransactions { get; private set; }

        public void Union(TransactionInfo other)
        {
            if (TransactionId != other.TransactionId)
            {
                // TODO: freak out
            }

            if (other.IsAborted)
            {
                IsAborted = true;
            }

            // Take a union of the ReadSets.
            foreach (var grain in other.ReadSet.Keys)
            {
                if (ReadSet.ContainsKey(grain))
                {
                    if (ReadSet[grain] != other.ReadSet[grain])
                    {
                        // Conflict! Transaction must abort
                        IsAborted = true;
                    }
                }
                else
                {
                    ReadSet.Add(grain, other.ReadSet[grain]);
                }
            }

            // Take a union of the WriteSets.
            foreach (var grain in other.WriteSet.Keys)
            {
                if (!WriteSet.ContainsKey(grain))
                {
                    WriteSet[grain] = 0;
                }

                WriteSet[grain] += other.WriteSet[grain];
            }

            DependentTransactions.UnionWith(other.DependentTransactions);
        }
    }

    [Serializable]
    public struct GrainVersion : IEquatable<GrainVersion>
    {
        public long TransactionId;
        public int WriteNumber;

        #region operators
        public static bool operator ==(GrainVersion v1, GrainVersion v2)
        {
            return v1.TransactionId == v2.TransactionId && v1.WriteNumber == v2.WriteNumber;
        }

        public static bool operator !=(GrainVersion v1, GrainVersion v2)
        {
            return !(v1 == v2);
        }

        public static bool operator <(GrainVersion v1, GrainVersion v2)
        {
            if (v1.TransactionId != v2.TransactionId)
            {
                return v1.TransactionId < v2.TransactionId;
            }

            return v1.WriteNumber < v2.WriteNumber;
        }

        public static bool operator >(GrainVersion v1, GrainVersion v2)
        {
            return v2 < v1;
        }
        #endregion

        #region IEquatable<T> methods - generated by ReSharper
        public bool Equals(GrainVersion other)
        {
            return TransactionId == other.TransactionId && WriteNumber == other.WriteNumber;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            return obj is GrainVersion && Equals((GrainVersion) obj);
        }

        public override int GetHashCode()
        {
            unchecked { return (TransactionId.GetHashCode() * 397) ^ WriteNumber; } 
        }
        #endregion

    }
}
