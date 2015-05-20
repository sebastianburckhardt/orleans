#if !DISABLE_STREAMS
using System;
using System.Threading.Tasks;
using Orleans.Streams;

namespace Orleans.Providers.Streams.Persistent.AzureQueueAdapter
{
    public class AzureQueueAdapterFactory : IQueueAdapterFactory
    {
        public const string DATA_CONNECTION_STRING = "DataConnectionString";
        public const string DEPLOYMENT_ID = "DeploymentId";

        public Task<IQueueAdapter> Create(IProviderConfiguration config)
        {
            if (config == null)
            {
                throw new ArgumentNullException("config");
            }

            string dataConnectionString;
            if (!config.Properties.TryGetValue(DATA_CONNECTION_STRING, out dataConnectionString))
            {
                throw new ArgumentException(String.Format("{0} property not set", DATA_CONNECTION_STRING));
            }

            string deploymentId;
            if (!config.Properties.TryGetValue(DEPLOYMENT_ID, out deploymentId))
            {
                throw new ArgumentException(String.Format("{0} property not set", DEPLOYMENT_ID));
            }

            var adapter = new AzureQueueAdapter(dataConnectionString, deploymentId);
            // TODO: Could initialize all queues up front?
            return Task.FromResult<IQueueAdapter>(adapter);
        }
    }

}
#endif
