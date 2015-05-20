using System;


using System.Threading.Tasks;

namespace Orleans.Runtime.MembershipService
{
    internal class MembershipFactory
    {
        private Logger logger;
        internal IMembershipNamingService NamingServiceProvider { private get;  set; }

        internal MembershipFactory()
        {
            logger = Logger.GetLogger("MembershipFactory", Logger.LoggerType.Runtime);
        }

        internal Task CreateMembershipTableProvider(Catalog catalog, Silo silo)
        {
            var livenessType = silo.GlobalConfig.LivenessType;
            logger.Info(ErrorCode.MBRFactory1, "Creating membership table provider for type={0}", Enum.GetName(typeof(GlobalConfiguration.LivenessProviderType), livenessType));
            if (livenessType.Equals(GlobalConfiguration.LivenessProviderType.MembershipTableGrain))
            {
                return catalog.CreateSystemGrain(
                        Constants.SystemMembershipTableId,
                        typeof(GrainBasedMembershipTable).FullName
                );
            }
            return TaskDone.Done;
        }

        internal async Task<IMembershipOracle> CreateMembershipOracle(Silo silo)
        {
            var livenessType = silo.GlobalConfig.LivenessType;
            logger.Info("Creating membership oracle for type={0}", Enum.GetName(typeof(GlobalConfiguration.LivenessProviderType), livenessType));
            
            IMembershipTable membershipTable;
            if (livenessType.Equals(GlobalConfiguration.LivenessProviderType.MembershipTableGrain))
            {
                membershipTable = MembershipTableFactory.Cast(GrainReference.FromGrainId(Constants.SystemMembershipTableId));
            }
            //else if (livenessType.Equals(GlobalConfiguration.LivenessProviderType.File))
            //{
            //    membershipTable = new FileBasedMembershipTable(silo.GlobalConfig.LivenessFileDirectory);
            //}
            else if (livenessType.Equals(GlobalConfiguration.LivenessProviderType.AzureTable))
            {
                membershipTable = await AzureBasedMembershipTable.GetAzureBasedMembershipTable(silo.GlobalConfig.DeploymentId, silo.GlobalConfig.DataConnectionString, true);
            }
#if !DISABLE_WF_INTEGRATION
            else if (livenessType.Equals(GlobalConfiguration.LivenessProviderType.WindowsFabricNamingService))
            {
                if (this.NamingServiceProvider == null)
                {
                    string msg = String.Format("Trying to create WindowsFabricNamingService MembershipOracle, but NamingServiceProvider was not injected yet.");
                    logger.Error(ErrorCode.SiloMissingNamingServiceProvider, msg);
                    throw new ArgumentException(msg, "NamingServiceProvider");
                }
                return new NamingServiceMembershipOracle(silo, NamingServiceProvider);
            }
#endif
            else
            {
                throw new NotImplementedException();
            }
            return new MembershipOracle(silo, membershipTable);
        }
    }
}
