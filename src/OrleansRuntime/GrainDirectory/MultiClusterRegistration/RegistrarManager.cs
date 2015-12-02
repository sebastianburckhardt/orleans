using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans.GrainDirectory;

namespace Orleans.Runtime.GrainDirectory
{
    /// <summary>
    /// Maps multi-cluster registration strategies to the corresponding registrar
    /// </summary>
    internal class RegistrarManager
    {
        private readonly Dictionary<Type, IGrainRegistrar> registrars = new Dictionary<Type, IGrainRegistrar>();

        public static RegistrarManager Instance { get; private set; }


        private RegistrarManager()
        {
        }

        public static void InitializeGrainDirectoryManager(LocalGrainDirectory router)
        {
            Instance = new RegistrarManager();
            Instance.Register<ClusterLocalRegistration>(new ClusterLocalRegistrar(router.DirectoryPartition));
            Instance.Register<GlobalSingleInstanceRegistration>(new GlobalSingleInstanceRegistrar(router.DirectoryPartition, router.Logger));
        }

        private void Register<TStrategy>(IGrainRegistrar directory)
            where TStrategy : MultiClusterRegistrationStrategy
        {
            registrars.Add(typeof(TStrategy), directory);
        }

        public IGrainRegistrar GetRegistrarForGrain(GrainId gid)
        {
            string unusedGrainClass;
            PlacementStrategy unusedPlacement;
            MultiClusterRegistrationStrategy strategy = ClusterLocalRegistration.Singleton; // default

            var typeCode = gid.GetTypeCode();

            if (typeCode != 0) // special case for Membership grain or client grain.
                GrainTypeManager.Instance.GetTypeInfo(gid.GetTypeCode(), out unusedGrainClass, out unusedPlacement, out strategy);


            return registrars[strategy.GetType()];
        }
    }
}
