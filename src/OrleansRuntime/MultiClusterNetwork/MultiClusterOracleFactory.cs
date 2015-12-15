using Orleans.Runtime.Configuration;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Orleans.Runtime.MultiClusterNetwork
{
    internal class MultiClusterOracleFactory
    {
        private readonly TraceLogger logger;

        internal MultiClusterOracleFactory()
        {
            logger = TraceLogger.GetLogger("MultiClusterOracleFactory", TraceLogger.LoggerType.Runtime);
        }

        internal async Task<IMultiClusterOracle> CreateGossipOracle(Silo silo)
        {
            if (! silo.GlobalConfig.HasMultiClusterNetwork)
            {
                logger.Info("Skip multicluster oracle creation (no multicluster network configured)");
                return null;
            }      
             
            logger.Info("Creating multicluster oracle...");

            var channels = await GetGossipChannels(silo);

            if (channels.Count == 0)
                logger.Warn(ErrorCode.MultiClusterNetwork_NoChannelsConfigured, "No gossip channels are configured.");

            var gossiporacle = new MultiClusterOracle(silo.SiloAddress, silo.ClusterId, channels, silo.GlobalConfig);

            logger.Info("Created multicluster oracle.");

            return gossiporacle;
        }

        internal async Task<List<IGossipChannel>> GetGossipChannels(Silo silo)
        {
            List<IGossipChannel> channellist = new List<IGossipChannel>();

            var channelconfigurations = silo.GlobalConfig.GossipChannels;
            if (channelconfigurations != null)
                foreach (var channelconf in channelconfigurations)
                {
                    switch (channelconf.ChannelType)
                    {
                        case GlobalConfiguration.GossipChannelType.AzureTable:
                            var tablechannel = AssemblyLoader.LoadAndCreateInstance<IGossipChannel>(Constants.ORLEANS_AZURE_UTILS_DLL, logger);
                            await tablechannel.Initialize(silo.GlobalConfig, channelconf.ConnectionString);
                            channellist.Add(tablechannel);

                            break;

                        default:
                            break;
                    }

                    logger.Info("Configured Gossip Channel: Type={0} ConnectionString={1}", channelconf.ChannelType, channelconf.ConnectionString);
                }

            return channellist;
        }
    }
}
