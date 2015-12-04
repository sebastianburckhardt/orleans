/*
Project Orleans Cloud Service SDK ver. 1.0
 
Copyright (c) Microsoft Corporation
 
All rights reserved.
 
MIT License

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and 
associated documentation files (the ""Software""), to deal in the Software without restriction,
including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense,
and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

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
