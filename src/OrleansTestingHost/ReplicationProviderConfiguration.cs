
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans.Runtime.Configuration;
using Orleans.TestingHost;
using Orleans.Runtime;

namespace Orleans.TestingHost
{
    public static class ReplicationProviderConfiguration
    {
        // change this as needed for debugging failing tests
        private const Severity LogViewProviderTraceLevel = Severity.Verbose2;


        public static void ConfigureLogViewProvidersForTesting(ClusterConfiguration config)
        {

            {
                var props = new Dictionary<string, string>();
                props.Add("DataConnectionString", StorageTestConstants.DataConnectionString);
                config.Globals.RegisterStorageProvider("Orleans.Storage.AzureTableStorage", "AzureStore", props);
            }

            {
                var props = new Dictionary<string, string>();
                props.Add("GlobalStorageProvider", "AzureStore");
                config.Globals.RegisterLogViewProvider("Orleans.Providers.LogViews.SharedStorageProvider", "SharedStorage", props);
            }

            {
                var props = new Dictionary<string, string>();
                props.Add("GlobalStorageProvider", "MemoryStore");
                config.Globals.RegisterLogViewProvider("Orleans.Providers.LogViews.SharedStorageProvider", "SharedMemory", props);
            }
            {
                var props = new Dictionary<string, string>();
                props.Add("PrimaryCluster", "A");
                config.Globals.RegisterLogViewProvider("Orleans.Providers.LogViews.CustomStorageProvider", "CustomStorage", props);
            }
            // logging  
            foreach (var o in config.Overrides)
                o.Value.TraceLevelOverrides.Add(new Tuple<string, Severity>("LogViews", Severity.Verbose2));

        }
    }
}
