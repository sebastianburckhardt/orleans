using System;


namespace Orleans
{
    /// <summary>
    /// Marker interface for addressable endpoints, such as grains, observers, and other system-internal addressable endpoints
    /// </summary>
    public interface IAddressable
    {
    }

    /// <summary>
    /// Marker interface for grains
    /// </summary>
    public interface IGrain : IAddressable
    {
    }

    /// <summary>
    /// Marker interface for grain extensions, used by internal runtime extension endpoints
    /// </summary>
    public interface IGrainExtension
    {
    }

    /// <summary>
    /// Extension methods for grains.
    /// </summary>
    public static class GrainExtensions
    {
        /// <summary>
        /// Converts this grain to a <c>GrainReference</c>
        /// </summary>
        /// <param name="grain">The grain to convert.</param>
        /// <returns>A <c>GrainReference</c> for this grain.</returns>
        public static GrainReference AsReference(this IAddressable grain)
        {
            var reference = grain as GrainReference;
            // When called against an instance of a grain reference class, do nothing
            if (reference != null) return reference;

            var grainBase = grain as GrainBase;
            if (grainBase != null) return GrainReference.FromGrainId(grainBase.Identity);

            var systemTarget = grain as ISystemTargetBase;
            if (systemTarget != null)
                return GrainReference.FromGrainId(systemTarget.Grain, null, null, systemTarget.CurrentSilo);

            throw new OrleansException(String.Format("AsReference has been called on an unexpected type: {0}.", grain.GetType().FullName));
        }

        private static GrainId GetGrainId(IAddressable grain)
        {
            var reference = grain as GrainReference;
            if (reference != null) return reference.GrainId;

            var grainBase = grain as GrainBase;
            if (grainBase != null) return grainBase.Identity;
            
            throw new OrleansException(String.Format("GetGrainId has been called on an unexpected type: {0}.", grain.GetType().FullName));
        }

        /// <summary>
        /// Returns the long representation of a grain primary key.
        /// </summary>
        /// <param name="grain">The grain to find the primary key for.</param>
        /// <param name="keyExt">The output paramater to return the extended key part of the grain primary key, if extened primary key was provided for that grain.</param>
        /// <returns>A long representing the primary key for this grain.</returns>
        public static long GetPrimaryKeyLong(this IAddressable grain, out string keyExt)
        {
            return GetGrainId(grain).GetPrimaryKeyLong(out keyExt);
        }

        /// <summary>
        /// Returns the long representation of a grain primary key.
        /// </summary>
        /// <param name="grain">The grain to find the primary key for.</param>
        /// <returns>A long representing the primary key for this grain.</returns>
        public static long GetPrimaryKeyLong(this IAddressable grain)
        {
            return GetGrainId(grain).GetPrimaryKeyLong();
        }
        /// <summary>
        /// Returns the Guid representation of a grain primary key.
        /// </summary>
        /// <param name="grain">The grain to find the primary key for.</param>
        /// <param name="keyExt">The output paramater to return the extended key part of the grain primary key, if extened primary key was provided for that grain.</param>
        /// <returns>A Guid representing the primary key for this grain.</returns>
        public static Guid GetPrimaryKey(this IAddressable grain, out string keyExt)
        {
            return GetGrainId(grain).GetPrimaryKey(out keyExt);
        }

        /// <summary>
        /// Returns the Guid representation of a grain primary key.
        /// </summary>
        /// <param name="grain">The grain to find the primary key for.</param>
        /// <returns>A Guid representing the primary key for this grain.</returns>
        public static Guid GetPrimaryKey(this IAddressable grain)
        {
            return GetGrainId(grain).GetPrimaryKey();
        }

        // Add back copies of old extension method APIs to maintain backward compat 
        // with Halo grains until they can be recompiled.

        public static long GetPrimaryKeyLong(this IGrain grain, out string keyExt)
        {
            return GetGrainId(grain).GetPrimaryKeyLong(out keyExt);
        }
        public static long GetPrimaryKeyLong(this IGrain grain)
        {
            return GetGrainId(grain).GetPrimaryKeyLong();
        }
        public static Guid GetPrimaryKey(this IGrain grain, out string keyExt)
        {
            return GetGrainId(grain).GetPrimaryKey(out keyExt);
        }
        public static Guid GetPrimaryKey(this IGrain grain)
        {
            return GetGrainId(grain).GetPrimaryKey();
        }
    }
}
