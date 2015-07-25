using Orleans.Runtime.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime.GossipNetwork
{
    internal class GossipOracleFactory
    {
        private readonly TraceLogger logger;

        internal GossipOracleFactory()
        {
            logger = TraceLogger.GetLogger("ClusterGatewayMembershipFactory", TraceLogger.LoggerType.Runtime);
        }

        internal async Task<IGossipOracle> CreateClusterGatewayMembershipOracle(Silo silo)
        {
            var livenessType = silo.GlobalConfig.LivenessType;
            logger.Info("Creating membership oracle for type={0}", Enum.GetName(typeof(GlobalConfiguration.LivenessProviderType), livenessType));

            var channels = await GetGossipChannels(silo);

            return channels == null ? null : new GossipOracle(silo.SiloAddress, channels, silo.GlobalConfig);
        }

        internal async Task<List<IGossipChannel>> GetGossipChannels(Silo silo)
        {
            var config = silo.GlobalConfig;

            if (config.GlobalServiceId == null || config.ClusterId == null)
            {
                logger.Info("No appropriate GlobalService id or Cluster id found in configuration. No Cluster Gateway membership oracle will be created");
                return null;
            }

            var channellist = new List<IGossipChannel>();


            //TODO properly support  configuring multiple gossip channel
            // for now, just default to using single azure-table-based-channel
            //var defaultchannel = await AzureTableBasedGossipChannel.GetChannel(config);
            

            var defaultchannel = AssemblyLoader.LoadAndCreateInstance<IGossipChannel>(Constants.ORLEANS_AZURE_UTILS_DLL, logger);

            channellist.Add(defaultchannel);

            return channellist;
        }
    }
}
