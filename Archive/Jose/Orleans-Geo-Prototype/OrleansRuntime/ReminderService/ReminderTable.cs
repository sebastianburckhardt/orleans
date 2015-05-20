using System;
using System.Threading.Tasks;



namespace Orleans.Runtime.ReminderService
{
    internal static class ReminderTable
    {
        // [mlr][todo] once legacy code has been removed, i can make this completely private.
        internal static IReminderTable Singleton { get; private set; }

        public static async Task Initialize(Silo silo)
        {
            var config = silo.GlobalConfig;
            var serviceType = config.ReminderServiceType;
            switch (serviceType)
            {
                default:
                    throw new NotSupportedException(
                        String.Format(
                            "The reminder table does not currently support service provider {0}.",
                            serviceType));

                case GlobalConfiguration.ReminderServiceProviderType.AzureTable:
                    Singleton =
                        await AzureBasedReminderTable.GetAzureBasedReminderTable(
                            config.DeploymentId,
                            config.DataConnectionString);
                    return;

                case GlobalConfiguration.ReminderServiceProviderType.ReminderTableGrain:
                    Singleton =
                        ReminderTableFactory.Cast(
                            GrainReference.FromGrainId(Constants.SystemReminderTableId));
                    // [mlr][todo] is it really necessary to wait for the grain to be fully resolved?
                    return;
            }
        }

        public static Task Clear()
        {
            return Singleton.Clear();
        }
    }
}
