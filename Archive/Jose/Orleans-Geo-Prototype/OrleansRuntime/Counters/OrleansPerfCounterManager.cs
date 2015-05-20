using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Linq;

using Orleans.Counters;

namespace Orleans.Runtime.Counters
{
    internal static class OrleansPerfCounterManager
    {
        internal const string CategoryName = "OrleansRuntime";
        internal const string CategoryDescription = "Orleans Runtime Counters";

        private static readonly Logger logger = Logger.GetLogger("OrleansPerfCounterManager", Logger.LoggerType.Runtime);

        private class PerfCounterConfigData
        {
            public StatName Name;
            //public string Description;
            public bool UseDeltaValue;
            internal IOrleansCounter<long> counterStat;
            internal PerformanceCounter perfCounter;
        }

        private static readonly List<PerfCounterConfigData> perfCounterData = new List<PerfCounterConfigData>();

        // ToDo: Move this list to some kind of config file
        private static readonly PerfCounterConfigData[] staticPerfCounters = new[] {
            new PerfCounterConfigData { Name=StatNames.STAT_CATALOG_ACTIVATION_COLLECTION_NUMBER_OF_COLLECTIONS, UseDeltaValue=true },
            new PerfCounterConfigData { Name=StatNames.STAT_CATALOG_ACTIVATION_COLLECTION_NUMBER_OF_COLLECTED_ACTIVATIONS_PROMPT },
            new PerfCounterConfigData { Name=StatNames.STAT_CATALOG_ACTIVATION_COLLECTION_NUMBER_OF_COLLECTED_ACTIVATIONS_DELAYED },
            new PerfCounterConfigData { Name=StatNames.STAT_DIRECTORY_CACHE_SIZE },
            new PerfCounterConfigData { Name=StatNames.STAT_DIRECTORY_LOOKUPS_FULL_ISSUED, UseDeltaValue=true },
            new PerfCounterConfigData { Name=StatNames.STAT_DIRECTORY_LOOKUPS_LOCAL_ISSUED, UseDeltaValue=true },
            new PerfCounterConfigData { Name=StatNames.STAT_DIRECTORY_LOOKUPS_LOCAL_SUCCESSES, UseDeltaValue=true },
            new PerfCounterConfigData { Name=StatNames.STAT_DIRECTORY_PARTITION_SIZE },
            new PerfCounterConfigData { Name=StatNames.STAT_GATEWAY_CONNECTED_CLIENTS },
            new PerfCounterConfigData { Name=StatNames.STAT_GATEWAY_LOAD_SHEDDING, UseDeltaValue=true },
            new PerfCounterConfigData { Name=StatNames.STAT_GATEWAY_RECEIVED, UseDeltaValue=true },
            new PerfCounterConfigData { Name=StatNames.STAT_MEMBERSHIP_ACTIVE_CLUSTER_SIZE },
            new PerfCounterConfigData { Name=StatNames.STAT_MESSAGING_SENT_BYTES_TOTAL, UseDeltaValue=true },
            new PerfCounterConfigData { Name=StatNames.STAT_MESSAGING_SENT_MESSAGES_TOTAL, UseDeltaValue=true },
            new PerfCounterConfigData { Name=StatNames.STAT_MESSAGING_SENT_LOCALMESSAGES, UseDeltaValue=true },

            new PerfCounterConfigData { Name=new StatName(StatNames.STAT_MESSAGING_DISPATCHER_PROCESSED_OK_PER_DIRECTION, Message.Directions.OneWay.ToString()) , UseDeltaValue=true },
            new PerfCounterConfigData { Name=new StatName(StatNames.STAT_MESSAGING_DISPATCHER_PROCESSED_OK_PER_DIRECTION, Message.Directions.Request.ToString()) , UseDeltaValue=true },
            new PerfCounterConfigData { Name=new StatName(StatNames.STAT_MESSAGING_DISPATCHER_PROCESSED_OK_PER_DIRECTION, Message.Directions.Response.ToString()) , UseDeltaValue=true },  
            new PerfCounterConfigData { Name=new StatName(StatNames.STAT_MESSAGING_DISPATCHER_PROCESSED_ERRORS_PER_DIRECTION, Message.Directions.OneWay.ToString()) , UseDeltaValue=true },
            new PerfCounterConfigData { Name=new StatName(StatNames.STAT_MESSAGING_DISPATCHER_PROCESSED_ERRORS_PER_DIRECTION, Message.Directions.Request.ToString()) , UseDeltaValue=true },
            new PerfCounterConfigData { Name=new StatName(StatNames.STAT_MESSAGING_DISPATCHER_PROCESSED_ERRORS_PER_DIRECTION, Message.Directions.Response.ToString()) , UseDeltaValue=true },
            new PerfCounterConfigData { Name=new StatName(StatNames.STAT_MESSAGING_DISPATCHER_RECEIVED_PER_DIRECTION, Message.Directions.OneWay.ToString()) , UseDeltaValue=true },
            new PerfCounterConfigData { Name=new StatName(StatNames.STAT_MESSAGING_DISPATCHER_RECEIVED_PER_DIRECTION, Message.Directions.Request.ToString()) , UseDeltaValue=true },
            new PerfCounterConfigData { Name=new StatName(StatNames.STAT_MESSAGING_DISPATCHER_RECEIVED_PER_DIRECTION, Message.Directions.Response.ToString()) , UseDeltaValue=true },
            new PerfCounterConfigData { Name=StatNames.STAT_MESSAGE_CENTER_RECEIVE_QUEUE_LENGTH },
            new PerfCounterConfigData { Name=StatNames.STAT_MESSAGE_CENTER_SEND_QUEUE_LENGTH },
            new PerfCounterConfigData { Name=StatNames.STAT_SCHEDULER_PENDINGWORKITEMS },
            new PerfCounterConfigData { Name=StatNames.STAT_CATALOG_ACTIVATION_COUNT },
            new PerfCounterConfigData { Name=StatNames.STAT_CATALOG_DUPLICATE_ACTIVATIONS},
            new PerfCounterConfigData { Name=StatNames.STAT_RUNTIME_GC_TOTALMEMORYKB },
            new PerfCounterConfigData { Name=StatNames.STAT_RUNTIME_DOT_NET_THREADPOOL_INUSE_WORKERTHREADS },
            new PerfCounterConfigData { Name=StatNames.STAT_RUNTIME_DOT_NET_THREADPOOL_INUSE_COMPLETIONPORTTHREADS },

            new PerfCounterConfigData { Name=StatNames.STAT_STORAGE_READ_TOTAL, UseDeltaValue=true },
            new PerfCounterConfigData { Name=StatNames.STAT_STORAGE_WRITE_TOTAL, UseDeltaValue=true },
            new PerfCounterConfigData { Name=StatNames.STAT_STORAGE_ACTIVATE_TOTAL, UseDeltaValue=true },
            new PerfCounterConfigData { Name=StatNames.STAT_STORAGE_READ_ERRORS, UseDeltaValue=true },
            new PerfCounterConfigData { Name=StatNames.STAT_STORAGE_WRITE_ERRORS, UseDeltaValue=true },

            new PerfCounterConfigData { Name=StatNames.STAT_AZURE_SERVER_BUSY, UseDeltaValue=true },
        };

        /// <summary>
        /// Have the perf counters we will use previously been registered with Windows? 
        /// </summary>
        /// <returns><c>true</c> if Windows perf counters are registered and available for Orleans</returns>
        public static bool AreWindowsPerfCountersAvailable()
        {
            try
            {
                return PerformanceCounterCategory.Exists(CategoryName);
            }
            catch (Exception exc)
            {
                logger.Warn(ErrorCode.PerfCounterCategoryCheckError,
                    string.Format("Ignoring error checking for {0} perf counter category", CategoryName), exc);
            }
            return false;
        }

        public static void PrecreateCounters()
        {
            GetCounterData();

            foreach (var cd in perfCounterData)
            {
                var perfCounterName = GetPerfCounterName(cd);
                cd.perfCounter = CreatePerfCounter(perfCounterName);
            }
        }

        internal static void GetCounterData()
        {
            perfCounterData.Clear();

            // (1) Start with list of static counters
            perfCounterData.AddRange(staticPerfCounters);

            // (2) Then search for grain DLLs and pre-create activation counters for any grain types found
            var loader = GrainAssemblyLoader.Instance;
            var grainTypes = loader.LoadGrainAssemblies();
            foreach (var gd in grainTypes)
            {
                StatName counterName = new StatName(StatNames.STAT_GRAIN_COUNTS_PER_GRAIN, gd.Key);
                perfCounterData.Add(new PerfCounterConfigData
                {
                    Name = counterName,
                    UseDeltaValue = false,
                    counterStat = CounterStatistic.FindOrCreate(counterName, false),
                });
            }
        }

        internal static CounterCreationData[] GetCounterCreationData()
        {
            GetCounterData();
            List<CounterCreationData> ctrCreationData = new List<CounterCreationData>();
            foreach (PerfCounterConfigData cd in perfCounterData)
            {
                string perfCounterName = GetPerfCounterName(cd);
                string desc = cd.Name.Name; // string.IsNullOrEmpty(cd.Description) ? cd.Name : cd.Description;

                string msg = string.Format("Registering perf counter {0}", perfCounterName);
                Console.WriteLine(msg);

                ctrCreationData.Add(new CounterCreationData(perfCounterName, desc, PerformanceCounterType.NumberOfItems32));
            }
            return ctrCreationData.ToArray();
        }

        internal static PerformanceCounter CreatePerfCounter(string perfCounterName)
        {
            logger.Verbose(ErrorCode.PerfCounterRegistering, "Creating perf counter {0}", perfCounterName);
            return new PerformanceCounter(CategoryName, perfCounterName, false);
        }

        /// <summary>
        /// Register Orleans perf counters with Windows
        /// </summary>
        /// <remarks>Note: Program needs to be running as Administrator to be able to delete Windows perf counters.</remarks>
        public static void InstallCounters()
        {
            CounterCreationDataCollection col = new CounterCreationDataCollection();
            col.AddRange(GetCounterCreationData());

            PerformanceCounterCategoryType catType = PerformanceCounterCategoryType.SingleInstance;

            PerformanceCounterCategory category = PerformanceCounterCategory.Create(
                CategoryName,
                CategoryDescription,
                catType,
                col);
        }

        /// <summary>
        /// Delete any existing perf counters registered with Windows
        /// </summary>
        /// <remarks>Note: Program needs to be running as Administrator to be able to delete Windows perf counters.</remarks>
        public static void DeleteCounters()
        {
            PerformanceCounterCategory.Delete(CategoryName);
        }

        public static int WriteCounters()
        {
            if(logger.IsVerbose) logger.Verbose("Writing Windows perf counters.");

            int numWriteErrors = 0;

            foreach (PerfCounterConfigData cd in perfCounterData)
            {
                StatName name = cd.Name;
                string perfCounterName = GetPerfCounterName(cd);

                try
                {
                    if (cd.perfCounter == null)
                    {
                        if (logger.IsVerbose) logger.Verbose(ErrorCode.PerfCounterUnableToConnect, "No perf counter found for {0}", name);
                        cd.perfCounter = CreatePerfCounter(perfCounterName);
                        //continue;
                    }

                    if (cd.counterStat == null)
                    {
                        if (logger.IsVerbose) logger.Verbose(ErrorCode.PerfCounterRegistering, "Searching for statistic {0}", name);

                        IOrleansCounter<long> ctr = IntValueStatistic.Find(name);

                        cd.counterStat = ctr ?? CounterStatistic.FindOrCreate(name);
                    }

                    long val;
                    if (cd.UseDeltaValue)
                    {
                        var ignore = ((CounterStatistic)cd.counterStat).GetCurrentValueAndDeltaAndResetDelta(out val);
                    }
                    else
                    {
                        val = cd.counterStat.GetCurrentValue();
                    }
                    if (logger.IsVerbose3) logger.Verbose3(ErrorCode.PerfCounterWriting, "Writing perf counter {0} Value={1}", perfCounterName, val);
                    cd.perfCounter.RawValue = val;
                }
                catch (Exception ex)
                {
                    numWriteErrors++;
                    logger.Error(ErrorCode.PerfCounterUnableToWrite, string.Format("Unable to write to Windows perf counter '{0}'", name), ex);
                }
            }
            return numWriteErrors;
        }

        private static string GetPerfCounterName(PerfCounterConfigData cd)
        {
            return cd.Name.Name + "." + (cd.UseDeltaValue ? "Delta" : "Current");
        }
    }
}
