using System;
using System.Collections.Generic;
using System.Net;

using Orleans.AzureUtils;
using Orleans.WFUtils;
using System.Threading.Tasks;

namespace Orleans.Messaging
{
    internal class GatewayProviderFactory
    {
        internal static async Task<IGatewayListProvider> CreateGatewayListProvider(ClientConfiguration cfg)
        {
            IGatewayListProvider listProvider = null;
            ClientConfiguration.GatewayProviderType gatewayProviderToUse = cfg.GatewayProviderToUse;
            if (gatewayProviderToUse == ClientConfiguration.GatewayProviderType.AzureTable)
            {
                listProvider = await AzureGatewayListProvider.GetAzureGatewayListProvider(cfg);
            }
#if !DISABLE_WF_INTEGRATION
            else if (gatewayProviderToUse == ClientConfiguration.GatewayProviderType.WindowsFabric)
            {
                listProvider = new WFGatewayListProvider(cfg);
            }
#endif
            else if (gatewayProviderToUse == ClientConfiguration.GatewayProviderType.Config)
            {
                listProvider = new StaticGatewayListProvider(cfg);
            }
            return listProvider;
        }
    }


    internal class StaticGatewayListProvider : IGatewayListProvider
    {
        private List<IPEndPoint> knownGateways;

        public StaticGatewayListProvider(ClientConfiguration cfg)
        {
            knownGateways = cfg.Gateways;
        }

        #region Implementation of IGatewayListProvider

        public List<IPEndPoint> GetGateways()
        {
            return knownGateways;
        }

        public TimeSpan MaxStaleness 
        {
            get { return TimeSpan.MaxValue; }
        }

        public bool IsUpdatable
        {
            get { return false; }
        }

        #endregion
    }
}