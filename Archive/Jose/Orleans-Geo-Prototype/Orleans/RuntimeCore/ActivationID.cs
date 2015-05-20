using System;

namespace Orleans
{
    [Serializable]
    internal class ActivationId : UniqueIdentifier, IEquatable<ActivationId>
    {
        public bool IsSystem { get { return Key.IsSystemTargetKey; } }

        public static readonly ActivationId Zero;

        private static readonly Interner<UniqueKey, ActivationId> interner;

        static ActivationId()
        {
            interner = new Interner<UniqueKey, ActivationId>(InternerConstants.Size_Large, InternerConstants.DefaultCacheCleanupFreq);
            Zero = FindOrCreate(UniqueKey.Empty);
        }

        /// <summary>
        /// For internal use only
        /// Only used in Json serialization
        /// DO NOT USE TO CREATE A RANDOM ACTIVATION ID
        /// Use ActivationId.NewId to create new activation IDs.
        /// </summary>
        public ActivationId()
        {
            // [mlr][todo] is there some way to enforce that this is only used for JSON serialization?
        }

        private ActivationId(UniqueKey key)
            : base(key)
        {
        }

        public static ActivationId NewId()
        {
            return FindOrCreate(UniqueKey.NewKey());
        }

        public static ActivationId GetSystemActivation(GrainId grain, SiloAddress location)
        {
            var key = GetSystemKey(grain, location);
            return FindOrCreate(key);
        }

        public static ActivationId GetActivationId(byte[] bytes)
        {
            var key = UniqueKey.FromByteArray(bytes, 0);
            return FindOrCreate(key);
        }

        private static UniqueKey GetSystemKey(GrainId grain, SiloAddress location)
        {
            if (!grain.IsSystemTarget)
                throw new ArgumentException("System activation IDs can only be created for system grains");
            return UniqueKey.NewSystemTargetKey(grain.GetSystemId(), location.Endpoint);
        }

        private static ActivationId FindOrCreate(UniqueKey key)
        {
            return interner.FindOrCreate(key, () => new ActivationId(key));
        }

        public override bool Equals(UniqueIdentifier obj)
        {
            var o = obj as ActivationId;
            return o != null && Key.Equals(o.Key);
        }

        public override bool Equals(object obj)
        {
            var o = obj as ActivationId;
            return o != null && Key.Equals(o.Key);
        }

        #region IEquatable<ActivationId> Members

        public bool Equals(ActivationId other)
        {
            return other != null && Key.Equals(other.Key);
        }

        #endregion

        public override int GetHashCode()
        {
            return Key.GetHashCode();
        }

        public override string ToString()
        {
            string idString = Key.ToString("X").Substring(24, 8);
            return String.Format("@{0}{1}", IsSystem ? "S" : "", idString);
        }

        public string ToFullString()
        {
            string idString = Key.ToString("X");
            return String.Format("@{0}{1}", IsSystem ? "S" : "", idString);
        }
    }
}
