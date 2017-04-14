
using System;
using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Core;
using Orleans.LogConsistency;
using Orleans.Storage;
using Orleans.Runtime.LogConsistency;
using Orleans.GrainDirectory;

namespace Orleans.Runtime
{
    /// <summary>
    /// Helper class used to create local instances of grains.
    /// </summary>
    internal class GrainCreator
    {
        private static readonly Func<GrainTypeData, ObjectFactory> createFactory = CreateFactory;

        private readonly Lazy<IGrainRuntime> grainRuntime;

        private readonly IServiceProvider services;

        private readonly ConcurrentDictionary<GrainTypeData, ObjectFactory> typeActivatorCache = new ConcurrentDictionary<GrainTypeData, ObjectFactory>(GrainTypeData.TypeComparer);

        private readonly Factory<Grain, IMultiClusterRegistrationStrategy, ProtocolServices> protocolServicesFactory;

        /// <summary>
        /// Initializes a new instance of the <see cref="GrainCreator"/> class.
        /// </summary>
        /// <param name="services">Service provider used to create new grains</param>
        /// <param name="getGrainRuntime">The delegate used to get the grain runtime.</param>
        /// <param name="protocolServicesFactory"></param>
        public GrainCreator(
            IServiceProvider services,
            Func<IGrainRuntime> getGrainRuntime,
            Factory<Grain, IMultiClusterRegistrationStrategy, ProtocolServices> protocolServicesFactory)
        {
            this.services = services;
            this.protocolServicesFactory = protocolServicesFactory;
            this.grainRuntime = new Lazy<IGrainRuntime>(getGrainRuntime);
        }

        /// <summary>
        /// Create a new instance of a grain
        /// </summary>
        /// <param name="grainType">The grain type.</param>
        /// <param name="identity">Identity for the new grain</param>
        /// <param name="arguments">Arguments available for grain construction</param>
        /// <returns>The newly created grain.</returns>
        public Grain CreateGrainInstance(GrainTypeData grainType, IGrainIdentity identity, object[] arguments)
        {
            var activator = this.typeActivatorCache.GetOrAdd(grainType, createFactory);
            var grain = (Grain)activator(this.services, arguments);

            // Inject runtime hooks into grain instance
            grain.Runtime = this.grainRuntime.Value;
            grain.Identity = identity;

            return grain;
        }

        /// <summary>
        /// Install the log-view adaptor into a log-consistent grain.
        /// </summary>
        /// <param name="grain">The grain.</param>
        /// <param name="grainType">The grain type.</param>
        /// <param name="stateType">The type of the grain state.</param>
        /// <param name="mcRegistrationStrategy">The multi-cluster registration strategy.</param>
        /// <param name="factory">The consistency adaptor factory</param>
        /// <param name="storageProvider">The storage provider, or null if none needed</param>
        /// <returns>The newly created grain.</returns>
        public void InstallLogViewAdaptor(Grain grain, Type grainType, 
            Type stateType, IMultiClusterRegistrationStrategy mcRegistrationStrategy,
            ILogViewAdaptorFactory factory, IStorageProvider storageProvider)
        {
            // encapsulate runtime services used by consistency adaptors
            var svc = this.protocolServicesFactory(grain, mcRegistrationStrategy);

            var state = Activator.CreateInstance(stateType);

            ((ILogConsistentGrain)grain).InstallAdaptor(factory, state, grainType.FullName, storageProvider, svc);
        }

        private static ObjectFactory CreateFactory(GrainTypeData grainTypeData)
        {
            return ActivatorUtilities.CreateFactory(grainTypeData.Type, grainTypeData.ConstructorInfo.FacetParameters);
        }
    }
}