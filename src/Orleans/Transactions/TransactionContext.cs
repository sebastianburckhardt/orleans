
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.Remoting.Messaging;
using Orleans.Concurrency;
using Orleans.Serialization;

namespace Orleans.Transactions
{
    public static class TransactionContext
    {
        internal const string TransactionInfoHeader = "#TC_TI";
        internal const string Orleans_TransactionContext_Key = "#ORL_TC";

        public static TransactionInfo GetTransactionInfo()
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

            values = values == null ? new Dictionary<string, object>() : new Dictionary<string, object>(values);
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
            ReadSet = new Dictionary<ITransactionalResource, TransactionalResourceVersion>();
            WriteSet = new Dictionary<ITransactionalResource, int>();
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
            ReadSet = new Dictionary<ITransactionalResource, TransactionalResourceVersion>();
            WriteSet = new Dictionary<ITransactionalResource, int>();
            DependentTransactions = new HashSet<long>();
        }

        public long TransactionId { get; }

        public bool IsReadOnly { get; }

        public bool IsAborted { get; set; }

        public int PendingCalls { get; set; }

        public Dictionary<ITransactionalResource, TransactionalResourceVersion> ReadSet { get; }
        public Dictionary<ITransactionalResource, int> WriteSet { get; }
        public HashSet<long> DependentTransactions { get; }

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
    [Immutable]
    [DebuggerDisplay("Id={TransactionId} WriteNumber={WriteNumber}")]
    public struct TransactionalResourceVersion : IEquatable<TransactionalResourceVersion>
    {
        public long TransactionId { get; private set; }
        public int WriteNumber { get; private set; }

        public static TransactionalResourceVersion Create(long transactionId, int writeNumber)
        {
            return new TransactionalResourceVersion
            {
                TransactionId = transactionId,
                WriteNumber = writeNumber
            };
        }

        #region operators
        public static bool operator ==(TransactionalResourceVersion v1, TransactionalResourceVersion v2)
        {
            return v1.TransactionId == v2.TransactionId && v1.WriteNumber == v2.WriteNumber;
        }

        public static bool operator !=(TransactionalResourceVersion v1, TransactionalResourceVersion v2)
        {
            return !(v1 == v2);
        }

        public static bool operator <(TransactionalResourceVersion v1, TransactionalResourceVersion v2)
        {
            if (v1.TransactionId != v2.TransactionId)
            {
                return v1.TransactionId < v2.TransactionId;
            }

            return v1.WriteNumber < v2.WriteNumber;
        }

        public static bool operator >(TransactionalResourceVersion v1, TransactionalResourceVersion v2)
        {
            return v2 < v1;
        }
        #endregion

        #region IEquatable<T> methods - generated by ReSharper
        public bool Equals(TransactionalResourceVersion other)
        {
            return TransactionId == other.TransactionId && WriteNumber == other.WriteNumber;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            return obj is TransactionalResourceVersion && Equals((TransactionalResourceVersion) obj);
        }

        public override int GetHashCode()
        {
            unchecked { return (TransactionId.GetHashCode() * 397) ^ WriteNumber; } 
        }
        #endregion

    }
}
