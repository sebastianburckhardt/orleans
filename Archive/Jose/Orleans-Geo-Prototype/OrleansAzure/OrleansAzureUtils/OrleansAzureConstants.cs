using System;
using System.Reflection;

namespace Orleans.Host.Azure.Utils
{
    internal sealed class OrleansAzureConstants
    {
        public static readonly TimeSpan STARTUP_TIME_PAUSE = TimeSpan.FromSeconds(5); // In seconds
        public const int MAX_RETRIES = 120;  // 120 x 5s = Total: 10 minutes

        //public static string DiagnosticsStorageAccountConfigurationSettingName = "Microsoft.WindowsAzure.Plugins.Diagnostics.ConnectionString";
        public static string DataConnectionConfigurationSettingName = "DataConnectionString";

        public static string SiloEndpointConfigurationKeyName = "OrleansSiloEndpoint";
        public static string ProxyEndpointConfigurationKeyName = "OrleansProxyEndpoint";
        public static string SiloLocalStoreConfigurationKeyName = "LocalStoreDirectory";
    }
}
