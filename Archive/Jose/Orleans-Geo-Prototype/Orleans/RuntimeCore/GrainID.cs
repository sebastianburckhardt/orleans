using System;
using System.Diagnostics.Contracts;
using System.Globalization;

namespace Orleans
{
    [Serializable]
    internal class GrainId : UniqueIdentifier, IEquatable<GrainId>
    {
        private static readonly object lockable = new object();
        private static readonly int InternCacheInitialSize = InternerConstants.Size_Large;
        private static readonly TimeSpan InternCacheCleanupInterval = InternerConstants.DefaultCacheCleanupFreq;

        private static Interner<UniqueKey, GrainId> grainIdInternCache;

        public UniqueKey.Category Category { get { return Key.IdCategory; } }

        public bool IsSystemTarget { get { return Key.IsSystemTargetKey; } }

        public bool IsGrain { get { return Category == UniqueKey.Category.Grain || Category == UniqueKey.Category.KeyExtGrain; } }

        public bool IsClient { get { return Category == UniqueKey.Category.ClientGrain || Category == UniqueKey.Category.ClientAddressableObject; } }

        internal bool IsClientGrain { get { return Category == UniqueKey.Category.ClientGrain; } }
        internal bool IsClientAddressableObject { get { return Category == UniqueKey.Category.ClientAddressableObject; } }

        private GrainId(UniqueKey key)
            : base(key)
        {
        }

        public static GrainId NewId()
        {
            return FindOrCreateGrainId(UniqueKey.NewKey(Guid.NewGuid(), UniqueKey.Category.Grain));
        }

        public static GrainId NewClientGrainId()
        {
            return FindOrCreateGrainId(UniqueKey.NewKey(Guid.NewGuid(), UniqueKey.Category.ClientGrain));
        }

        public static GrainId NewClientAddressableGrainId()
        {
            return FindOrCreateGrainId(UniqueKey.NewKey(Guid.NewGuid(), UniqueKey.Category.ClientAddressableObject));
        }

        public static GrainId GetGrainId(byte[] bytes, int keyExtLen = -1)
        {
            return FindOrCreateGrainId(UniqueKey.FromByteArray(bytes, keyExtLen));
        }

        internal static GrainId GetGrainId(Guid guid)
        {
            return FindOrCreateGrainId(UniqueKey.NewKey(guid, UniqueKey.Category.SystemGrain));
        }

        internal static GrainId GetSystemTargetGrainId(short systemGrainId)
        {
            return FindOrCreateGrainId(UniqueKey.NewSystemTargetKey(systemGrainId));
        }

        internal static GrainId GetGrainId(long typeCode, long primaryKey, string keyExt=null)
        {
            return FindOrCreateGrainId(UniqueKey.NewKey(primaryKey, 
                keyExt == null ? UniqueKey.Category.Grain : UniqueKey.Category.KeyExtGrain, 
                typeCode, keyExt));
        }

        internal static GrainId GetGrainId(long typeCode, Guid primaryKey, string keyExt=null)
        {
            return FindOrCreateGrainId(UniqueKey.NewKey(primaryKey, 
                keyExt == null ? UniqueKey.Category.Grain : UniqueKey.Category.KeyExtGrain, 
                typeCode, keyExt));
        }

        [Pure]
        internal short GetSystemId()
        {
            return Key.PrimaryKeyToSystemId();
        }

        internal long GetPrimaryKeyLong(out string keyExt)
        {
            return Key.PrimaryKeyToLong(out keyExt);
        }

        [Pure]
        internal long GetPrimaryKeyLong()
        {
            return Key.PrimaryKeyToLong();
        }

        internal Guid GetPrimaryKey(out string keyExt)
        {
            return Key.PrimaryKeyToGuid(out keyExt);
        }

        internal Guid GetPrimaryKey()
        {
            return Key.PrimaryKeyToGuid();
        }

        internal int GetTypeCode()
        {
            return Key.BaseTypeCode;
        }

        private static GrainId FindOrCreateGrainId(UniqueKey key)
        {
            // Note: This is done here to avoid a wierd cyclic dependency / static initialization ordering problem involving the GrainId, Constants & Interner classes
            if (grainIdInternCache == null)
            {
                lock (lockable)
                {
                    if (grainIdInternCache == null)
                    {
                        grainIdInternCache = new Interner<UniqueKey, GrainId>(InternCacheInitialSize, InternCacheCleanupInterval);
                    }
                }
            }
            return grainIdInternCache.FindOrCreate(key, () => new GrainId(key));
        }

        #region IEquatable<GrainId> Members

        public bool Equals(GrainId other)
        {
            //return base.Equals(other);
            return other != null && Key.Equals(other.Key);
        }

        #endregion

        public override bool Equals(UniqueIdentifier obj)
        {
            var o = obj as GrainId;
            return o != null && Key.Equals(o.Key);
        }

        public override bool Equals(object obj)
        {
            var o = obj as GrainId;
            return o != null && Key.Equals(o.Key);
        }

        // Keep compiler happy -- it does not like classes to have Equals(...) without GetHashCode() methods
        public override int GetHashCode()
        {
            return Key.GetHashCode();
        }

        /// <summary>
        /// Get a uniformly distributed hash code value for this grain, based on Jenkins Hash function.
        /// NOTE: Hash code value may be positive or NEGATIVE.
        /// </summary>
        /// <returns>Hash code for this GrainId</returns>
        public int GetUniformHashCode()
        {
            return Key.GetUniformHashCode();
        }

        public override string ToString()
        {
            if (Category == UniqueKey.Category.SystemTarget)
            {
                return Constants.SystemTargetName(this);
            }
            string name;
            if (Constants.TryGetSystemGrainName(this, out name))
            {
                return name;
            }

            // print hash code and not just 8 bytes to capture the whole range of the grain ids.
            //string idString = GetHashCode().ToString("X");

            var keyString = Key.ToString("X");
            // [mlr] this should grab the least-significant half of n1, suffixing it with the key extension.
            string idString;
            if (keyString.Length >= 48)
                idString = keyString.Substring(24, 8) + keyString.Substring(48);
            else
                idString = keyString.Substring(24, 8);

            switch (Category)
            {
                case UniqueKey.Category.Grain:
                case UniqueKey.Category.KeyExtGrain:
                    var typeString = GetTypeCode().ToString("X").Tail(8);
                    return String.Format("*grn/{0}/{1}", typeString, idString);
                case UniqueKey.Category.ClientGrain:
                    return "*cli/" + idString;
                case UniqueKey.Category.ClientAddressableObject:
                    return "*cliObj/" + idString;
                default:
                    return "???/" + idString;
            }
        }

        // same as ToString, just full primary key and type code
        internal string ToDetailedString()
        {
            if (Category == UniqueKey.Category.SystemTarget)
            {
                return Constants.SystemTargetName(this);
            }
            string name;
            if (Constants.TryGetSystemGrainName(this, out name))
            {
                return name;
            }

            var idString = Key.ToString("X");

            switch (Category)
            {
                case UniqueKey.Category.Grain:
                case UniqueKey.Category.KeyExtGrain:
                    var typeString = GetTypeCode().ToString("X");
                    return String.Format("*grn/{0}/{1}", typeString, idString);
                case UniqueKey.Category.ClientGrain:
                    return "*cli/" + idString;
                case UniqueKey.Category.ClientAddressableObject:
                    return "*cliObj/" + idString;
                default:
                    return "???/" + idString;
            }
        }

        internal string ToFullString()
        {
            string kx;
            string pks =
                Key.IsLongKey ?
                    GetPrimaryKeyLong(out kx).ToString(CultureInfo.InvariantCulture) :
                    GetPrimaryKey(out kx).ToString();
            string pksHex =
                Key.IsLongKey ?
                    GetPrimaryKeyLong(out kx).ToString("X") :
                    GetPrimaryKey(out kx).ToString("X");
            return
                String.Format(
                    "[GrainId: {0}, IdCategory: {1}, BaseTypeCode: {2} (x{3}), PrimaryKey: {4} (x{5}), UniformHashCode: {6} (0x{7, 8:X8}){8}]",
                    this.ToDetailedString(),                        // 0
                    this.Category,                          // 1
                    this.GetTypeCode(),                     // 2
                    this.GetTypeCode().ToString("X"),       // 3
                    pks,                                    // 4
                    pksHex,                                 // 5
                    this.GetUniformHashCode(),              // 6
                    this.GetUniformHashCode(),              // 7
                    this.Key.HasKeyExt ?  String.Format(", KeyExtension: {0}", kx) : "");   // 8
        }

        internal string ToStringWithHashCode()
        {
            return String.Format("{0}-0x{1, 8:X8}", this.ToString(), this.GetUniformHashCode()); 
        }

        /// <summary>
        /// Return this GrainId in a standard string form, suitable for later use with the <c>FromParsableString</c> method.
        /// </summary>
        /// <returns>GrainId in a standard string format.</returns>
        internal string ToParsableString()
        {
            // NOTE: This function must be the "inverse" of FromParsableString, and data must round-trip reliably.

            return Key.ToHexString();
        }

        /// <summary>
        /// Create a new GrainId object by parsing string in a standard form returned from <c>ToParsableString</c> method.
        /// </summary>
        /// <param name="addr">String containing the GrainId info to be parsed.</param>
        /// <returns>New GrainId object created from the input data.</returns>
        internal static GrainId FromParsableString(string str)
        {
            // NOTE: This function must be the "inverse" of ToParsableString, and data must round-trip reliably.

            UniqueKey key = UniqueKey.Parse(str);
            return FindOrCreateGrainId(key);
        }
    }
}
