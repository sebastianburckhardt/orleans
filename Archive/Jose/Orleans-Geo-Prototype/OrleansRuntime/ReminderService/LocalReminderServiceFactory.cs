using System;
using System.Threading.Tasks;
using Orleans.Runtime.ReminderService;



namespace Orleans.Runtime
{
    internal class LocalReminderServiceFactory
    {
        private readonly Logger logger;

        internal LocalReminderServiceFactory()
        {
            logger = Logger.GetLogger("ReminderFactory", Logger.LoggerType.Runtime);
        }

        internal Task CreateReminderTableProvider(Catalog catalog, Silo silo)
        {
            var reminderServiceType = silo.GlobalConfig.ReminderServiceType;
            if (reminderServiceType.Equals(GlobalConfiguration.ReminderServiceProviderType.ReminderTableGrain))
            {
                logger.Info(ErrorCode.RS_Factory1, "Creating reminder table provider for type={0}", Enum.GetName(typeof(GlobalConfiguration.ReminderServiceProviderType), reminderServiceType));
                return catalog.CreateSystemGrain(
                        Constants.SystemReminderTableId,
                        typeof(GrainBasedReminderTable).FullName
                );
            }
            return TaskDone.Done;
        }

        internal async Task<IReminderService> CreateReminderService(Silo silo)
        {
            var reminderServiceType = silo.GlobalConfig.ReminderServiceType;
            logger.Info("Creating reminder system target for type={0}", Enum.GetName(typeof(GlobalConfiguration.ReminderServiceProviderType), reminderServiceType));

            await ReminderTable.Initialize(silo);
            return new LocalReminderService(silo.SiloAddress, Constants.ReminderServiceId, silo.ConsistentRingProvider, silo.LocalScheduler, ReminderTable.Singleton);
        }
    }
}
