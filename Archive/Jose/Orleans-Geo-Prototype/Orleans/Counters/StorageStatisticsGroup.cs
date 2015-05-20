using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Orleans.Storage;

namespace Orleans.Counters
{
    internal class StorageStatisticsGroup
    {
        internal static CounterStatistic StorageReadTotal;
        internal static CounterStatistic StorageWriteTotal;
        internal static CounterStatistic StorageActivateTotal;
        internal static CounterStatistic StorageClearTotal;
        internal static CounterStatistic StorageReadErrors;
        internal static CounterStatistic StorageWriteErrors;
        internal static CounterStatistic StorageActivateErrors;
        internal static CounterStatistic StorageClearErrors;
        internal static AverageTimeSpanStatistic StorageReadLatency;
        internal static AverageTimeSpanStatistic StorageWriteLatency;
        internal static AverageTimeSpanStatistic StorageClearLatency;

        internal static void Init()
        {
            StorageReadTotal = CounterStatistic.FindOrCreate(StatNames.STAT_STORAGE_READ_TOTAL);
            StorageWriteTotal = CounterStatistic.FindOrCreate(StatNames.STAT_STORAGE_WRITE_TOTAL);
            StorageActivateTotal = CounterStatistic.FindOrCreate(StatNames.STAT_STORAGE_ACTIVATE_TOTAL);
            StorageReadErrors = CounterStatistic.FindOrCreate(StatNames.STAT_STORAGE_READ_ERRORS);
            StorageWriteErrors = CounterStatistic.FindOrCreate(StatNames.STAT_STORAGE_WRITE_ERRORS);
            StorageActivateErrors = CounterStatistic.FindOrCreate(StatNames.STAT_STORAGE_ACTIVATE_ERRORS);
            StorageReadLatency = AverageTimeSpanStatistic.FindOrCreate(StatNames.STAT_STORAGE_READ_LATENCY);
            StorageWriteLatency = AverageTimeSpanStatistic.FindOrCreate(StatNames.STAT_STORAGE_WRITE_LATENCY);
            StorageClearTotal = CounterStatistic.FindOrCreate(StatNames.STAT_STORAGE_CLEAR_TOTAL);
            StorageClearErrors = CounterStatistic.FindOrCreate(StatNames.STAT_STORAGE_CLEAR_ERRORS);
            StorageClearLatency = AverageTimeSpanStatistic.FindOrCreate(StatNames.STAT_STORAGE_CLEAR_LATENCY);
        }

        internal static void OnStorageRead(IStorageProvider storage, string grainType, GrainId grain, TimeSpan latency)
        {
            StorageReadTotal.Increment();
            if (latency > TimeSpan.Zero)
            {
                StorageReadLatency.AddSample(latency);
            }
        }
        internal static void OnStorageWrite(IStorageProvider storage, string grainType, GrainId grain, TimeSpan latency)
        {
            StorageWriteTotal.Increment();
            if (latency > TimeSpan.Zero)
            {
                StorageWriteLatency.AddSample(latency);
            }
        }
        internal static void OnStorageActivate(IStorageProvider storage, string grainType, GrainId grain, TimeSpan latency)
        {
            StorageActivateTotal.Increment();
            if (latency > TimeSpan.Zero)
            {
                StorageReadLatency.AddSample(latency);
            }
        }
        internal static void OnStorageReadError(IStorageProvider storage, string grainType, GrainId grain)
        {
            StorageReadErrors.Increment();
        }
        internal static void OnStorageWriteError(IStorageProvider storage, string grainType, GrainId grain)
        {
            StorageWriteErrors.Increment();
        }
        internal static void OnStorageActivateError(IStorageProvider storage, string grainType, GrainId grain)
        {
            StorageActivateErrors.Increment();
        }
        internal static void OnStorageDelete(IStorageProvider storage, string grainType, GrainId grain, TimeSpan latency)
        {
            StorageClearTotal.Increment();
            if (latency > TimeSpan.Zero)
            {
                StorageClearLatency.AddSample(latency);
            }
        }
        internal static void OnStorageDeleteError(IStorageProvider storage, string grainType, GrainId grain)
        {
            StorageClearErrors.Increment();
        }
    }
}
