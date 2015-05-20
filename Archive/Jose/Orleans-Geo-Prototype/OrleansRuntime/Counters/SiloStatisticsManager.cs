using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using System.Threading.Tasks;
using Orleans.Counters;
using Orleans.AzureUtils;

namespace Orleans.Runtime.Counters
{
    internal class SiloStatisticsManager
    {
        private LogStatistics logStatistics;
        private RuntimeStatisticsGroup runtimeStats;
        private PerfCountersStatistics perfCountersPublisher;
        internal SiloPerformanceMetrics MetricsTable;

        internal SiloStatisticsManager(GlobalConfiguration globalConfig, NodeConfiguration nodeConfig)
        {
            MessagingStatisticsGroup.Init(true);
            MessagingProcessingStatisticsGroup.Init();
            NetworkingStatisticsGroup.Init(true);
            ApplicationRequestsStatisticsGroup.Init(globalConfig.ResponseTimeout); 
            SchedulerStatisticsGroup.Init();
            StorageStatisticsGroup.Init();
            runtimeStats = new RuntimeStatisticsGroup();
            logStatistics = new LogStatistics(nodeConfig.StatisticsLogWriteInterval, true);
            MetricsTable = new SiloPerformanceMetrics(runtimeStats, nodeConfig);
            perfCountersPublisher = new PerfCountersStatistics(nodeConfig.StatisticsPerfCountersWriteInterval);
        }

        internal async Task SetSiloMetricsTableDataManager(GlobalConfiguration globalConfig, NodeConfiguration nodeConfig, string siloName, SiloAddress siloAddress)
        {
            bool writeAzureMetricsTable = !string.IsNullOrEmpty(globalConfig.DataConnectionString) && !string.IsNullOrEmpty(globalConfig.DeploymentId);
            if (writeAzureMetricsTable)
            {
                // Hook up to publish silo metrics to Azure storage table
                var gateway = nodeConfig.IsGatewayNode ? nodeConfig.ProxyGatewayEndpoint : null;
                var metricsDataPublisher = await SiloMetricsTableDataManager.GetManager(globalConfig.DeploymentId, globalConfig.DataConnectionString, siloName, siloAddress, gateway, nodeConfig.DNSHostName);
                MetricsTable.MetricsDataPublisher = metricsDataPublisher;
            }
        }

        internal async Task SetSiloStatsTableDataManager(GlobalConfiguration globalConfig, NodeConfiguration nodeConfig, string siloName, SiloAddress siloAddress)
        {
            bool writeAzureStatsTable = !string.IsNullOrEmpty(globalConfig.DataConnectionString) && !string.IsNullOrEmpty(globalConfig.DeploymentId)
                && nodeConfig.StatisticsWriteLogStatisticsToTable;
            if (writeAzureStatsTable)
            {
                var statsDataPublisher = await StatsTableDataManager.GetManager(true, globalConfig.DataConnectionString, globalConfig.DeploymentId, siloAddress.ToLongString(), siloName, nodeConfig.DNSHostName);
                logStatistics.StatsTablePublisher = statsDataPublisher;
            }
        }

        internal void Start(NodeConfiguration config)
        {
            perfCountersPublisher.Start();
            logStatistics.Start();
            runtimeStats.Start();
            // Start performance metrics publisher
            MetricsTable.MetricsTableWriteInterval = config.StatisticsMetricsTableWriteInterval;
        }

        internal void Stop()
        {
            if (runtimeStats != null)
                runtimeStats.Stop();
            runtimeStats = null;
            if (MetricsTable != null)
                MetricsTable.Dispose();
            MetricsTable = null;
            if (perfCountersPublisher != null)
                perfCountersPublisher.Stop();
            perfCountersPublisher = null;
            if (logStatistics != null)
            {
                logStatistics.Stop();
                logStatistics.DumpCounters();
            }
            logStatistics = null;
        }
    }
}
