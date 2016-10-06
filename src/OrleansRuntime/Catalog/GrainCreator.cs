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
        private readonly IGrainRuntime _grainRuntime;
        private readonly IServiceProvider _services;
        private readonly Func<Type, ObjectFactory> _createFactory;
        private ConcurrentDictionary<Type, ObjectFactory> _typeActivatorCache = new ConcurrentDictionary<Type, ObjectFactory>();

        /// <summary>
        /// Instantiate a new instance of a <see cref="GrainCreator"/>
        /// </summary>
        /// <param name="grainRuntime">Runtime to use for all new grains</param>
        /// <param name="services">(Optional) Service provider used to create new grains</param>
        public GrainCreator(IGrainRuntime grainRuntime, IServiceProvider services)
        {
            _grainRuntime = grainRuntime;
            _services = services;
            if (_services != null)
            {
                _createFactory = (type) => ActivatorUtilities.CreateFactory(type, Type.EmptyTypes);
            }
            else
            {
                // TODO: we could optimize instance creation for the non-DI path also
                _createFactory = (type) => ((sp, args) => Activator.CreateInstance(type));
            }
    }

        /// <summary>
        /// Create a new instance of a grain
        /// </summary>
        /// <param name="grainType"></param>
        /// <param name="identity">Identity for the new grain</param>
        /// <returns></returns>
        public Grain CreateGrainInstance(Type grainType, IGrainIdentity identity)
        {
            var activator = _typeActivatorCache.GetOrAdd(grainType, _createFactory);
            var grain = (Grain)activator(_services, arguments: null);

            // Inject runtime hooks into grain instance
            grain.Runtime = _grainRuntime;
            grain.Identity = identity;

            return grain;
        }

        /// <summary>
        /// Create a new instance of a grain
        /// </summary>
        /// <param name="grainType"></param>
        /// <param name="identity">Identity for the new grain</param>
        /// <param name="stateType">If the grain is a stateful grain, the type of the state it persists.</param>
        /// <param name="persistenceProvider">If the grain is a stateful grain, the provider used to persist the state.</param>
        /// <returns></returns>
        public Grain CreateGrainInstance(Type grainType, IGrainIdentity identity, Type stateType, IPersistenceProvider persistenceProvider)
        {
            //Create a new instance of the grain
            var grain = CreateGrainInstance(grainType, identity);

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
