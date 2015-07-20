using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans.GrainDirectory;

namespace Orleans.Runtime.GrainDirectory.MyTemp
{
    internal class GrainDirectoryManager
    {
        private readonly Dictionary<Type, IGrainRegistrar> directories = new Dictionary<Type, IGrainRegistrar>();
        private IGrainRegistrar defaultGrainDirectory;

        public static GrainDirectoryManager Instance { get; private set; }

        private GrainDirectoryManager()
        { }

        public static void InitializeGrainDirectoryManager(LocalGrainDirectory router)
        {
            Instance = new GrainDirectoryManager();
            Instance.Register<StatelessWorkerActivationStrategy>(new StatelessWorkerRegistrar(router));
            Instance.Register<SingleInstanceActivationStrategy>(new SingleInstanceRegistrar(router));
        }

        private void Register<TStrategy>(IGrainRegistrar directory)
            where TStrategy : ActivationStrategy
        {
            directories.Add(typeof(TStrategy), directory);
        }

        public IGrainRegistrar ResolveDirectory(ActivationStrategy strategy)
        {
            var strat = strategy ?? SingleInstanceActivationStrategy.Singleton;
            return directories[strat.GetType()];
        }
    }
}
