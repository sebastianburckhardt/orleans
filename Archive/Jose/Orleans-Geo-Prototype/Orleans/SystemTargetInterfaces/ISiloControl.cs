﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;

using Orleans.Counters;

namespace Orleans
{
    internal interface ISiloControl : ISystemTarget
    {
        Task Ping(string message);

        Task SetSystemLogLevel(int traceLevel);
        Task SetAppLogLevel(int traceLevel);
        Task SetLogLevel(string logName, int traceLevel);

        Task ForceGarbageCollection();
        Task ForceActivationCollection(TimeSpan ageLimit);
        Task ForceRuntimeStatisticsCollection();

        Task<SiloRuntimeStatistics> GetRuntimeStatistics();
        Task<List<Tuple<GrainId, string, int>>> GetGrainStatistics();
        Task<SimpleGrainStatistic[]> GetSimpleGrainStatistics();
        Task<DetailedGrainReport> GetDetailedGrainReport(GrainId grainId);

        Task<GlobalConfiguration> GetGlobalConfig();
        Task<NodeConfiguration> GetLocalConfig();

        Task UpdateConfiguration(string configuration);

        Task<int> GetActivationCount();
    }
}
