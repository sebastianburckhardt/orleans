using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans.Counters;
using Orleans.Runtime.Coordination;



namespace Orleans.Runtime
{
    internal class SiloControl : SystemTarget, ISiloControl
    {
        private readonly Silo silo;
        private static readonly Logger logger = Logger.GetLogger("SiloControl", Logger.LoggerType.Runtime);

        public SiloControl(Silo silo)
            : base(Constants.SiloControlId, silo.SiloAddress)
        {
            this.silo = silo;
        }

        #region Implementation of ISiloControl

        public Task Ping(string message)
        {
            logger.Info("Ping");
            return TaskDone.Done;
        }

        public Task SetSystemLogLevel(int traceLevel)
        {
            OrleansLogger.Severity newTraceLevel = (OrleansLogger.Severity)traceLevel;
            logger.Info("SetSystemLogLevel={0}", newTraceLevel);
            Logger.SetRuntimeLogLevel(newTraceLevel);
            silo.LocalConfig.DefaultTraceLevel = newTraceLevel;
            return TaskDone.Done;
        }

        public Task SetAppLogLevel(int traceLevel)
        {
            OrleansLogger.Severity newTraceLevel = (OrleansLogger.Severity)traceLevel;
            logger.Info("SetAppLogLevel={0}", newTraceLevel);
            Logger.SetAppLogLevel(newTraceLevel);
            return TaskDone.Done;
        }

        public Task SetLogLevel(string logName, int traceLevel)
        {
            OrleansLogger.Severity newTraceLevel = (OrleansLogger.Severity)traceLevel;
            logger.Info("SetLogLevel[{0}]={1}", logName, newTraceLevel);
            Logger l = Logger.FindLogger(logName);
            if (l != null)
            {
                l.SetSeverityLevel(newTraceLevel);
                return TaskDone.Done;
            }
            else
            {
                throw new ArgumentException(string.Format("Logger {0} not found", logName));
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2001:AvoidCallingProblematicMethods", MessageId = "System.GC.Collect")]
        public Task ForceGarbageCollection()
        {
            logger.Info("ForceGarbageCollection");
            GC.Collect();
            return TaskDone.Done;
        }

        public Task ForceActivationCollection(TimeSpan ageLimit)
        {
            logger.Info("ForceActivationCollection");

            return InsideGrainClient.Current.Catalog.CollectActivations(ageLimit);
        }

        public Task ForceRuntimeStatisticsCollection()
        {
            if (logger.IsVerbose) logger.Verbose("ForceRuntimeStatisticsCollection");
            return DeploymentLoadCollector.FetchStatistics().AsTask();
        }

        public Task<SiloRuntimeStatistics> GetRuntimeStatistics()
        {
            if (logger.IsVerbose) logger.Verbose("GetRuntimeStatistics");
            return Task.FromResult( new SiloRuntimeStatistics(silo.Metrics));
        }

        public Task<List<Tuple<GrainId, string, int>>> GetGrainStatistics()
        {
            logger.Info("GetGrainStatistics");
            return Task.FromResult( InsideGrainClient.Current.Catalog.GetGrainStatistics());
        }

        public Task<SimpleGrainStatistic[]> GetSimpleGrainStatistics()
        {
            logger.Info("GetSimpleGrainStatistics");
            return Task.FromResult( InsideGrainClient.Current.Catalog.GetSimpleGrainStatistics().Select(p =>
                new SimpleGrainStatistic { SiloAddress = silo.SiloAddress, GrainType = p.Key, ActivationCount = (int)p.Value }).ToArray());
        }

        public Task<DetailedGrainReport> GetDetailedGrainReport(GrainId grainId)
        {
            logger.Info("DetailedGrainReport for grain id {0}", grainId);
            return Task.FromResult( InsideGrainClient.Current.Catalog.GetDetailedGrainReport(grainId));
        }

        public Task<GlobalConfiguration> GetGlobalConfig()
        {
            logger.Info("GetGlobalConfig");
            return Task.FromResult( silo.GlobalConfig);
        }

        public Task<NodeConfiguration> GetLocalConfig()
        {
            logger.Info("GetLocalConfig");
            return Task.FromResult( silo.LocalConfig);
        }

        public Task UpdateConfiguration(string configuration)
        {
            logger.Info("UpdateConfiguration with {0}", configuration);
            silo.OrleansConfig.Update(configuration);
            logger.Info(ErrorCode.Runtime_Error_100318, "UpdateConfiguration - new config is now {0}", silo.OrleansConfig.ToString(silo.Name));
            return TaskDone.Done;
        }

        public Task<int> GetActivationCount()
        {
            return Task.FromResult(InsideGrainClient.Current.Catalog.ActivationCount);
        }

        #endregion
    }
}
