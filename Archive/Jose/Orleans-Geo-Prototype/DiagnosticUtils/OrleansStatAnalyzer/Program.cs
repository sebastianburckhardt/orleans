using System;
using System.Collections.Generic;
using System.Threading;

namespace OrleansStatAnalyzer
{
    class Program
    {
        public static AnalysisConfigBuilder ConfigBuilder { get; set; }

        static void Main(string[] args)
        {
            // Read the config

            string configFile = null;
            if (args.GetLength(0) == 0)
            {
                configFile = @"config\AnalysisConfiguration.xml";   
            }
            else
            {
                configFile = args[0];
            }

            // Build the in-memory model
            List<PerfCounterData> datasets = InitConfig(configFile);

            ThreadPool.SetMinThreads(100, 100);
            
            // Do the analysis
            foreach (var dataset in datasets)
            {
                dataset.Populate();
                dataset.Analyze();
            }

            // Do comparative analysis
            if (ConfigBuilder.CompAnalyzer != null)
            {
                ComparativeAnalyzer compAnalyzer = ConfigBuilder.CompAnalyzer;
                compAnalyzer.Compare(datasets);    
            }
        }

        static List<PerfCounterData> InitConfig(string configname)
        {
            ConfigBuilder = new AnalysisConfigBuilder();
            List<PerfCounterData> counterDatasets = ConfigBuilder.ProcessConfig(configname);
            return counterDatasets;
        }

    }
}
