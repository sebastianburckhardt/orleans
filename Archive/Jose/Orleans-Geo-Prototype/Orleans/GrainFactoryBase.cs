using System;
using System.Net;
using Orleans.Runtime;

namespace Orleans
{
    /// <summary>
    /// Abstract base class for all grain proxy factory classes.
    /// </summary>
    /// <remarks>
    /// These methods are used from Orleans generated code.
    /// </remarks>
    public static class GrainFactoryBase
    {
        /// <summary>
        /// Fabricate a grain reference for a grain with the specified Int64 primary key
        /// </summary>
        /// <param name="grainInterfaceType">Grain type</param>
        /// <param name="interfaceId">Type code value for this grain type</param>
        /// <param name="primaryKey">Primary key for the grain</param>
        /// <param name="grainClassNamePrefix">Prefix or full name of the grain class to disambiguate multiple implementations.</param>
        /// <returns><c>GrainReference</c> for connecting to the grain with the specified primary key</returns>
        /// <exception cref="System.ArgumentException">If called for a grain type that is not a valid grain type.</exception>
        public static IAddressable MakeGrainReferenceInternal(
            Type grainInterfaceType,
            int interfaceId,
            long primaryKey,
            string grainClassNamePrefix = null)
        {
            return
                _MakeGrainReference(
                    baseTypeCode => TypeCodeMapper.ComposeGrainId(baseTypeCode, primaryKey, grainInterfaceType),
                    grainInterfaceType,
                    interfaceId,
                    grainClassNamePrefix);
        }

        /// <summary>
        /// Fabricate a grain reference for a grain with the specified Guid primary key
        /// </summary>
        /// <param name="grainInterfaceType">Grain type</param>
        /// <param name="interfaceId">Type code value for this self-managed grain type</param>
        /// <param name="primaryKey">Primary key for the grain</param>
        /// <param name="grainClassNamePrefix">Prefix or full name of the grain class to disambiguate multiple implementations.</param>
        /// <returns><c>GrainReference</c> for connecting to the self-managed grain with the specified primary key</returns>
        /// <exception cref="System.ArgumentException">If called for a grain type that is not a valid grain type.</exception>
        public static IAddressable MakeGrainReferenceInternal(
            Type grainInterfaceType,
            int interfaceId,
            Guid primaryKey,
            string grainClassNamePrefix = null)
        {
            return
                _MakeGrainReference(
                    baseTypeCode => TypeCodeMapper.ComposeGrainId(baseTypeCode, primaryKey, grainInterfaceType),
                    grainInterfaceType,
                    interfaceId,
                    grainClassNamePrefix);
        }

        /// <summary>
        /// Fabricate a grain reference for an explicitly-placed grain with the specified Guid primary key
        /// </summary>
        /// <param name="grainInterfaceType">Grain type</param>
        /// <param name="interfaceId">Type code value for this grain type</param>
        /// <param name="primaryKey">Primary key for the grain</param>
        /// <param name="grainClassNamePrefix">Prefix or full name of the grain class to disambiguate multiple implementations.</param>
        /// <returns><c>GrainReference</c> for connecting to the grain with the specified primary key</returns>
        /// <exception cref="System.ArgumentException">If called for a grain type that is not a valid grain type.</exception>
        public static IAddressable MakeExplicitlyPlacedGrainReferenceInternal(
            Type grainInterfaceType,
            int interfaceId,
            Guid primaryKey,
            IPEndPoint placeOnSilo,
            string grainClassNamePrefix = null)
        {
            return
                _MakeGrainReference(
                    baseTypeCode => TypeCodeMapper.ComposeGrainId(baseTypeCode, primaryKey, grainInterfaceType),
                    grainInterfaceType,
                    interfaceId,
                    grainClassNamePrefix,
                    ExplicitPlacement.NewObject(placeOnSilo));
        }

        /// <summary>
        /// Fabricate a grain reference for an explicitly-placed grain with the specified Int64 primary key
        /// </summary>
        /// <param name="grainInterfaceType">Grain type</param>
        /// <param name="interfaceId">Type code value for this grain type</param>
        /// <param name="primaryKey">Primary key for the grain</param>
        /// <param name="grainClassNamePrefix">Prefix or full name of the grain class to disambiguate multiple implementations.</param>
        /// <returns><c>GrainReference</c> for connecting to the grain with the specified primary key</returns>
        /// <exception cref="System.ArgumentException">If called for a grain type that is not a valid grain type.</exception>
        public static IAddressable MakeExplicitlyPlacedGrainReferenceInternal(
            Type grainInterfaceType,
            int interfaceId,
            long primaryKey,
            IPEndPoint placeOnSilo,
            string grainClassNamePrefix = null)
        {
            return
                _MakeGrainReference(
                    baseTypeCode => TypeCodeMapper.ComposeGrainId(baseTypeCode, primaryKey, grainInterfaceType),
                    grainInterfaceType,
                    interfaceId,
                    grainClassNamePrefix,
                    ExplicitPlacement.NewObject(placeOnSilo));
        }

        /// <summary>
        /// Fabricate a grain reference for an extended-key grain with the specified Guid primary key
        /// </summary>
        /// <param name="grainInterfaceType">Grain type</param>
        /// <param name="interfaceId">Type code value for this grain type</param>
        /// <param name="primaryKey">Primary key for the grain</param>
        /// <param name="keyExt">Extended key for the grain</param>
        /// <param name="grainClassNamePrefix">Prefix or full name of the grain class to disambiguate multiple implementations.</param>
        /// <returns><c>GrainReference</c> for connecting to the grain with the specified primary key</returns>
        /// <exception cref="System.ArgumentException">If called for a grain type that is not a valid grain type.</exception>
        public static IAddressable MakeKeyExtendedGrainReferenceInternal(
            Type grainInterfaceType,
            int interfaceId,
            Guid primaryKey,
            string keyExt,
            string grainClassNamePrefix = null)
        {
            return
                _MakeGrainReference(
                    baseTypeCode => TypeCodeMapper.ComposeGrainId(baseTypeCode, primaryKey, grainInterfaceType, keyExt),
                    grainInterfaceType,
                    interfaceId,
                    grainClassNamePrefix);
        }

        /// <summary>
        /// Fabricate a grain reference for an extended-key grain with the specified Int64 primary key
        /// </summary>
        /// <param name="grainInterfaceType">Grain type</param>
        /// <param name="interfaceId">Type code value for this grain type</param>
        /// <param name="primaryKey">Primary key for the grain</param>
        /// <param name="keyExt">Extended key for the grain</param>
        /// <param name="grainClassNamePrefix">Prefix or full name of the grain class to disambiguate multiple implementations.</param>
        /// <returns><c>GrainReference</c> for connecting to the grain with the specified primary key</returns>
        /// <exception cref="System.ArgumentException">If called for a grain type that is not a valid grain type.</exception>
        public static IAddressable MakeKeyExtendedGrainReferenceInternal(
            Type grainInterfaceType,
            int interfaceId,
            long primaryKey,
            string keyExt,
            string grainClassNamePrefix = null)
        {
            return
                _MakeGrainReference(
                    baseTypeCode => TypeCodeMapper.ComposeGrainId(baseTypeCode, primaryKey, grainInterfaceType, keyExt),
                    grainInterfaceType,
                    interfaceId,
                    grainClassNamePrefix);
        }

        internal static IAddressable _MakeGrainReference(
            Func<int, GrainId> getGrainId,
            Type grainType,
            int interfaceId,
            string grainClassNamePrefix = null,
            PlacementStrategy placement = null)
        {
            CheckRuntimeEnvironmentSetup();
            if (!GrainClientGenerator.GrainInterfaceData.IsGrainType(grainType))
            {
                throw new ArgumentException("Cannot fabricate grain-reference for non-grain type: " + grainType.FullName);
            }
            int grainTypeCode = TypeCodeMapper.GetImplementationTypeCode(interfaceId, grainClassNamePrefix);
            GrainId grainId = getGrainId(grainTypeCode);
            return GrainReference.FromGrainId(grainId,
                grainType.IsGenericType ? grainType.UnderlyingSystemType.FullName : null, placement);
        }

        /// <summary>
        /// Check that a grain observer parameter is of the correct underlying concrent type -- either extending from <c>GrainRefereence</c> or <c>GrainBase</c>
        /// </summary>
        /// <param name="grainObserver">Grain observer parameter to be checked.</param>
        /// <exception cref="ArgumentNullException">If grainObserver is <c>null</c></exception>
        /// <exception cref="NotSupportedException">If grainObserver class is not an appropriate underlying concrete type.</exception>
        public static void CheckGrainObserverParamInternal(IGrainObserver grainObserver)
        {
            if (grainObserver == null)
            {
                throw new ArgumentNullException("grainObserver", "IGrainObserver parameters cannot be null");
            }
            if (grainObserver is GrainReference || grainObserver is GrainBase)
            {
                // OK
            }
            else
            {
                string errMsg = string.Format("IGrainObserver parameters must be GrainReference or GrainBase and cannot be type {0}. Did you forget to CreateObjectReference?", grainObserver.GetType());
                throw new NotSupportedException(errMsg);
            }
        }

        #region Utility functions

        /// <summary>
        /// Check the current runtime environment has been setup and initialized correctly.
        /// Throws InvalidOperationException if current runtime environment is not initialized.
        /// </summary>
        private static void CheckRuntimeEnvironmentSetup()
        {
            if (GrainClient.Current == null)
            {
                string msg = "Orleans runtime environment is not set up (GrainClient.Current==null). If you are running on the client, perhaps you are missing a call to OrleansClient.Initialize(...) ? " +
                    "If you are running on the silo, perhaps you are trying to send a message or create a grain reference not within Orleans thread or from within grain constructor?";
                throw new InvalidOperationException(msg);
            }
        }
        #endregion
    }
}
