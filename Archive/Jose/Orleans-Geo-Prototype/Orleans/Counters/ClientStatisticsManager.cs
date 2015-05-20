using System;
using System.Threading.Tasks;
using Orleans.AzureUtils;

namespace Orleans.Counters
{
    internal class ClientStatisticsManager
    {
        private ClientTableStatistics tableStatistics;
        private LogStatistics logStatistics;
        private RuntimeStatisticsGroup runtimeStats;

        internal ClientStatisticsManager(ClientConfiguration config)
        {
            runtimeStats = new RuntimeStatisticsGroup();
            logStatistics = new LogStatistics(config.StatisticsLogWriteInterval, false);
        }

        internal async Task Start(ClientConfiguration config, IMessageCenter transport, Guid clientId)
        {
            MessagingStatisticsGroup.Init(false);
            NetworkingStatisticsGroup.Init(false);
            ApplicationRequestsStatisticsGroup.Init(config.ResponseTimeout); 

            runtimeStats.Start();
            bool writeAzureMetricsTable = config.UseAzureStorage;
            if (writeAzureMetricsTable)
            {
                // Hook up to publish client metrics to Azure storage table
                ClientMetricsTableDataManager metricsDataPublisher = await ClientMetricsTableDataManager.GetManager(config, clientId.ToString(), transport.MyAddress.Endpoint.Address);
                tableStatistics = new ClientTableStatistics(transport, metricsDataPublisher, runtimeStats);
                tableStatistics.MetricsTableWriteInterval = config.StatisticsMetricsTableWriteInterval;
            }

            bool writeAzureStatsTable = config.UseAzureStorage && config.StatisticsWriteLogStatisticsToTable;
            if (writeAzureStatsTable)
            {
                var statsDataPublisher = await StatsTableDataManager.GetManager(false, config.DataConnectionString, config.DeploymentId, transport.MyAddress.Endpoint.ToString(), clientId.ToString(), config.DNSHostName);
                logStatistics.StatsTablePublisher = statsDataPublisher;
            }

            logStatistics.Start();
        }

        internal void Stop()
        {
            if (runtimeStats != null)
                runtimeStats.Stop();
            runtimeStats = null;
            if (logStatistics != null)
            {
                logStatistics.DumpCounters();
                logStatistics.Stop();
            }
            logStatistics = null;
            if (tableStatistics != null)
                tableStatistics.Dispose();
            tableStatistics = null;
        }
    }
}
