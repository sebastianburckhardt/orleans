using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.StorageClient;

namespace Orleans.AzureUtils
{
    /// <summary>
    /// Utility class for default retry / timeout settings for Azure storage.
    /// </summary>
    /// <remarks>
    /// These functions are mostly intended for internal usage by Orleans runtime, but due to certain assembly packaging constrants this class needs to have public visibility.
    /// </remarks>
    internal static class AzureTableDefaultPolicies
    {
        public static int MaxTableCreationRetries { get; private set; }
        public static int MaxTableOperationRetries { get; private set; }
        public static int MaxBusyRetries { get; internal set; }

        public static TimeSpan PauseBetweenTableCreationRetries { get; private set; }
        public static TimeSpan PauseBetweenTableOperationRetries { get; private set; }
        public static TimeSpan PauseBetweenBusyRetries { get; private set; }

        public static TimeSpan TableCreation_TIMEOUT { get; private set; }
        public static TimeSpan TableOperation_TIMEOUT { get; private set; }
        public static TimeSpan BusyRetries_TIMEOUT { get; private set; }

        public static RetryPolicy TableCreationRetryPolicy { get; private set; }
        public static RetryPolicy TableOperationRetryPolicy { get; private set; }

        public const int MAX_BULK_UPDATE_ROWS = 100;

        static AzureTableDefaultPolicies()
        {
            MaxTableCreationRetries = 60;
            PauseBetweenTableCreationRetries = TimeSpan.FromSeconds(1);
            TableCreationRetryPolicy = RetryPolicies.Retry(MaxTableCreationRetries, PauseBetweenTableCreationRetries); // 60 x 1s
            TableCreation_TIMEOUT = PauseBetweenTableCreationRetries.Multiply(MaxTableCreationRetries).Multiply(3);    // 3 min

            MaxTableOperationRetries = 5;
            PauseBetweenTableOperationRetries = TimeSpan.FromMilliseconds(100);
            TableOperationRetryPolicy = RetryPolicies.Retry(MaxTableOperationRetries, PauseBetweenTableOperationRetries); // 5 x 100ms
            TableOperation_TIMEOUT = PauseBetweenTableOperationRetries.Multiply(MaxTableOperationRetries).Multiply(6);    // 3 sec

            MaxBusyRetries = 120;
            PauseBetweenBusyRetries = TimeSpan.FromMilliseconds(500);
            BusyRetries_TIMEOUT = PauseBetweenBusyRetries.Multiply(MaxBusyRetries);  // 1 minute
        }
    }
}
