using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans.Runtime.Configuration;

namespace Orleans.GrainDirectory
{
    [Serializable]
    internal abstract class ActivationStrategy
    {
        private static ActivationStrategy defaultStrategy;

        internal static void Initialize(GlobalConfiguration config = null)
        {
            InitializeStrategies();
            var strategy = config == null
                ? GlobalConfiguration.DEFAILT_ACTIVATION_STRATEGY
                : config.DefaultActivationStrategy;
            defaultStrategy = GetStrategy(strategy);
        }
        
        private static ActivationStrategy GetStrategy(string strategy)
        {
            if (strategy.Equals(typeof (StatelessWorkerActivationStrategy).Name))
            {
                return StatelessWorkerActivationStrategy.Singleton;
            }
            if (strategy.Equals(typeof (SingleInstanceActivationStrategy).Name))
            {
                return SingleInstanceActivationStrategy.Singleton;
            }
            return null;
        }

        private static void InitializeStrategies()
        {
            StatelessWorkerActivationStrategy.Initialize();
            SingleInstanceActivationStrategy.Initialize();
        }

        internal static ActivationStrategy GetDefault()
        {
            return defaultStrategy;
        }

        internal abstract bool IsSingleInstance();
    }
}
