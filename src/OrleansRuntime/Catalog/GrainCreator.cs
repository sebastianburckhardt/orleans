using System;
using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Core;
using Orleans.Storage;
using Orleans.LogViews;
using Orleans.Runtime.LogViews;
using Orleans.GrainDirectory;

namespace Orleans.Runtime
{
    /// <summary>
    /// Helper classe used to create local instances of grains.
    /// </summary>
    public class GrainCreator
    {
        private readonly Lazy<IGrainRuntime> grainRuntime;

        private readonly IServiceProvider services;

        private readonly Func<Type, ObjectFactory> createFactory;

        private readonly ConcurrentDictionary<Type, ObjectFactory> typeActivatorCache = new ConcurrentDictionary<Type, ObjectFactory>();

        /// <summary>
        /// Initializes a new instance of the <see cref="GrainCreator"/> class.
        /// </summary>
        /// <param name="services">Service provider used to create new grains</param>
        /// <param name="getGrainRuntime">
        /// The delegate used to get the grain runtime.
        /// </param>
        public GrainCreator(IServiceProvider services, Func<IGrainRuntime> getGrainRuntime)
        {
            this.services = services;
            this.grainRuntime = new Lazy<IGrainRuntime>(getGrainRuntime);
            if (services != null)
            {
                this.createFactory = type => ActivatorUtilities.CreateFactory(type, Type.EmptyTypes);
            }
            else
            {
                this.createFactory = type => (sp, args) => Activator.CreateInstance(type);
            }
        }

        /// <summary>
        /// Create a new instance of a grain
        /// </summary>
        /// <param name="grainType">The grain type.</param>
        /// <param name="identity">Identity for the new grain</param>
        /// <returns>The newly created grain.</returns>
        public Grain CreateGrainInstance(Type grainType, IGrainIdentity identity)
        {
            var activator = this.typeActivatorCache.GetOrAdd(grainType, this.createFactory);
            var grain = (Grain)activator(this.services, arguments: null);

            // Inject runtime hooks into grain instance
            grain.Runtime = this.grainRuntime.Value;
            grain.Identity = identity;

            return grain;
        }

        /// <summary>
        /// Create a new instance of a grain
        /// </summary>
        /// <param name="grainType">The grain type.</param>
        /// <param name="identity">Identity for the new grain</param>
        /// <param name="stateType">If the grain is a stateful grain, the type of the state it persists.</param>
        /// <param name="persistenceProvider">If the grain is a stateful grain, the provider used to persist the state.</param>
        /// <returns>The newly created grain.</returns>
        public Grain CreateGrainInstance(Type grainType, IGrainIdentity identity, Type stateType, IPersistenceProvider persistenceProvider)
        {
            // Create a new instance of the grain
            var grain = this.CreateGrainInstance(grainType, identity);

            var statefulGrain = grain as IStatefulGrain;
            var logViewGrain = grain as ILogViewGrain;

            if (statefulGrain != null)
            {
                var storage = new GrainStateStorageBridge(grainType.FullName, statefulGrain, (IStorageProvider) persistenceProvider);

                //Inject state and storage data into the grain
                statefulGrain.GrainState.State = Activator.CreateInstance(stateType);
                statefulGrain.SetStorage(storage);
                return grain;
            }
            else if (logViewGrain != null)
            {
                var logViewProvider = persistenceProvider as ILogViewProvider;

                // if given a plain storage provider instead of a log view provider, convert it to log view provider
                if (logViewProvider == null)
                    logViewProvider = new LogViewProviderManager.WrappedStorageProvider((IStorageProvider) persistenceProvider);

                // install protocol adaptor into grain
                var svc = new ProtocolServices(grain, logViewProvider, MultiClusterRegistrationStrategy.FromGrainType(grainType));
                logViewGrain.InstallAdaptor(logViewProvider, Activator.CreateInstance(stateType), grainType.FullName, svc);
            }
          
            return grain;
        }
    }
}