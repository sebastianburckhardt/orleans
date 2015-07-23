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
            Instance.Register<StatelessWorkerActivationStrategy>(new StatelessWorkerRegistrar(router.DirectoryPartition));
            Instance.Register<SingleInstanceActivationStrategy>(new SingleInstanceRegistrar(router.DirectoryPartition));
        }

        private void Register<TStrategy>(IGrainRegistrar directory)
            where TStrategy : ActivationStrategy
        {
            directories.Add(typeof(TStrategy), directory);
        }

        public IGrainRegistrar ResolveDirectory(GrainId gid)
        {
            string unusedGrainClass;
            PlacementStrategy unusedPlacement;
            ActivationStrategy strategy = null;

            var typeCode = gid.GetTypeCode();

            if (typeCode != 0) // special case for Membership grain or client grain.
                GrainTypeManager.Instance.GetTypeInfo(gid.GetTypeCode(), out unusedGrainClass, out unusedPlacement, out strategy);
            else if (gid.IsClient)
                strategy = StatelessWorkerActivationStrategy.Singleton;

            //use Single instance as default.
            strategy = strategy ?? SingleInstanceActivationStrategy.Singleton;

            return directories[strategy.GetType()];
        }
    }
}
