using System;
using Orleans.Runtime.Configuration;
using Orleans.MultiCluster;

namespace Orleans.GrainDirectory
{
    /// <summary>
    /// A superclass for all multi-cluster registration strategies.
    /// Strategy objects are used as keys to select the proper registrar.
    /// </summary>
    [Serializable]
    public abstract class MultiClusterRegistrationStrategy
    {
        private static MultiClusterRegistrationStrategy defaultStrategy;

        internal static void Initialize(GlobalConfiguration config = null)
        {
            InitializeStrategies();
            var strategy = config == null
                ? GlobalConfiguration.DEFAULT_MULTICLUSTER_REGISTRATION_STRATEGY
                : config.DefaultMultiClusterRegistrationStrategy;
            defaultStrategy = GetStrategy(strategy);
        }
        
        private static MultiClusterRegistrationStrategy GetStrategy(string strategy)
        {
            if (strategy.Equals(typeof (ClusterLocalRegistration).Name))
            {
                return ClusterLocalRegistration.Singleton;
            }
            return null;
        }

        private static void InitializeStrategies()
        {
            ClusterLocalRegistration.Initialize();
        }

        internal static MultiClusterRegistrationStrategy GetDefault()
        {
            return defaultStrategy;
        }

        internal static MultiClusterRegistrationStrategy FromAttributes(Type graintype)
        {
            var attrs = graintype.GetCustomAttributes(typeof(RegistrationAttribute), true);
            if (attrs.Length == 0)
                return defaultStrategy;
            return ((RegistrationAttribute)attrs[0]).RegistrationStrategy;
        }

        internal abstract bool IsSingleInstance();
    }
}
