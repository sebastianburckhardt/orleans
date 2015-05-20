//#define USE_METRICS_READER
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using Orleans.Messaging;
using System.Threading.Tasks;


namespace Orleans.AzureUtils
{
    internal class AzureGatewayListProvider : IGatewayListProvider
    {
        private OrleansSiloInstanceManager siloInstanceManager;
#if USE_METRICS_READER
        private readonly SiloMetricsDataReader dataCollector;
#endif
        private readonly ClientConfiguration config;
        private readonly object lockable;

        private AzureGatewayListProvider(ClientConfiguration conf)
        {
            config = conf;
#if USE_METRICS_READER
            dataCollector = new SiloMetricsDataReader(config.DeploymentId, config.DataConnectionString);
#endif
            lockable = new object();
        }

        public static async Task<AzureGatewayListProvider> GetAzureGatewayListProvider(ClientConfiguration conf)
        {
            AzureGatewayListProvider provider = new AzureGatewayListProvider(conf);
            provider.siloInstanceManager = await OrleansSiloInstanceManager.GetManager(conf.DeploymentId, conf.DataConnectionString);
            return provider;
        }

        #region Implementation of IGatewayListProvider

#if !USE_METRICS_READER
        // no caching
        public List<IPEndPoint> GetGateways()
        {
            lock (lockable)
            {
                IEnumerable<IPEndPoint> gatewayEndpoints = siloInstanceManager.FindAllGatewayProxyEndpoints();
                if (gatewayEndpoints != null && gatewayEndpoints.Any())
                {
                    return gatewayEndpoints.ToList();
                }
                else
                {
                    return new List<IPEndPoint>();
                }
            }
        }
#else
        //private List<Tuple<IPEndPoint, double>> GetGateways()
        //{
        //    lock (lockable)
        //    {
        //        if ((lastList.Count == 0) || (DateTime.UtcNow.Subtract(lastAccessed) >= gatewayListRefreshPeriod))
        //        {
        //            IEnumerable<SiloMetricsData> metrics;
        //            IEnumerable<IPEndPoint> gatewayEndpoints;
        //            try
        //            {
        //                gatewayEndpoints = siloInstanceManager.FindAllGatewayProxyEndpoints();
        //                metrics = dataCollector.GetSiloMetrics();
        //            }
        //            catch (Exception)
        //            {
        //                // If we couldn't get the data, keep what we last had
        //                return lastList;
        //            }
        //            if (gatewayEndpoints != null && gatewayEndpoints.Count() > 0)
        //            {
        //                lastList = gatewayEndpoints.Select(gwIP =>
        //                {
        //                    SiloMetricsData weight = null;
        //                    if (metrics != null && metrics.Count() > 0)
        //                    {
        //                        // silo metrics may not include all silos yet, so take the weight only for those we do have 
        //                        // and Synthesize some blank metrics for those we don't.
        //                        weight = metrics.FirstOrDefault(met =>
        //                            {
        //                                var ip = new IPEndPoint(
        //                                    IPAddress.Parse(met.GatewayAddress),
        //                                    met.GatewayPort);
        //                                return ip.Equals(gwIP);
        //                            });
        //                    }
        //                    return new Tuple<IPEndPoint, double>(gwIP, (weight != null) ? weight.CPU : 0.0);
        //                }).ToList();
        //            }
        //            lastAccessed = DateTime.UtcNow;
        //        }
        //        return lastList;
        //    }
        //}
#endif

        public TimeSpan MaxStaleness 
        {
            get { return config.GatewayListRefreshPeriod; }
        }

        public bool IsUpdatable
        {
            get { return true; }
        }


        #endregion
    }
}
