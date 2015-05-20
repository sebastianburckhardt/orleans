using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans.Counters;
using Orleans.Scheduler;


namespace Orleans.Runtime.Coordination
{
    /// <summary>
    /// This class collects runtime statistics for all silos in the current deployment for use by placement and others.
    /// </summary>
    internal static class DeploymentLoadCollector
    {
        private static ISiloStatusOracle membership;
        private static OrleansTaskScheduler scheduler;
        private static Dictionary<SiloAddress, SiloRuntimeStatistics> periodicStats;
        private static DateTime lastPeriodicStatsUpdate;
        private static bool updatingPeriodicStats;
        private static object updateLock;
        private static TimeSpan statisticsFreshnessTime;
        private static readonly Logger logger;

        //        private static int counter = 0;

        static DeploymentLoadCollector()
        {
            logger = Logger.GetLogger("DeploymentLoadCollector", Logger.LoggerType.Runtime);
        }

        public static Task Initialize(TimeSpan freshnessTime)
        {
            if (updateLock == null)
            {
                updateLock = new object();
                membership = Silo.CurrentSilo.LocalSiloStatusOracle;
                scheduler = Silo.CurrentSilo.LocalScheduler;
                statisticsFreshnessTime = freshnessTime;
                periodicStats = new Dictionary<SiloAddress, SiloRuntimeStatistics>();
                // [mlr] i wait here to ensure that there's a value to start with. this hasn't caused anything to
                // deadlock and this only occurs when a silo is starting up, so it seems safe to do.
                return FetchStatistics().AsTask();
            }
            return TaskDone.Done;
        }

        public static IEnumerable<KeyValuePair<SiloAddress, SiloRuntimeStatistics>> PeriodicStatistics
        {
            get
            {
                UpdateStatisticsIfStale();
                return periodicStats;
            }
        }

        private static void UpdateStatistics(Dictionary<SiloAddress, SiloRuntimeStatistics> stats)
        {
            lock (updateLock)
            {
                lastPeriodicStatsUpdate = DateTime.UtcNow;
                periodicStats = stats;
                updatingPeriodicStats = false;

                //foreach (var i in stats)
                //    System.Diagnostics.Trace.WriteLine(
                //        string.Format(
                //            "[mlr] self={0} n={1} #{2} {3} -> cpu%={4}, ol?={5}", 
                //            Silo.CurrentSilo.SiloAddress,
                //            stats.Count,
                //            counter, 
                //            i.Key, 
                //            i.Value.CpuUsage, 
                //            i.Value.IsOverloaded));
                //++counter;
            }
        }

        private static void UpdateStatisticsIfStale()
        {
            lock (updateLock)
            {
                // [mlr] i'm not certain this code is free of races. it should, however, be safe to ignore them 
                // in this situation. all that matters is that a request is triggered so that the cache is updated.
                if (!updatingPeriodicStats && DateTime.UtcNow.Subtract(lastPeriodicStatsUpdate) < statisticsFreshnessTime)
                {
                    updatingPeriodicStats = true;
                    FetchStatistics().Ignore();
                }
            }
        }

        public static AsyncValue<Dictionary<SiloAddress, SiloRuntimeStatistics>> FetchStatistics()
        {
            var members = membership.GetApproximateSiloStatuses(true).Keys.ToList();
            // [mlr][caution] the fact that this can be Dictionary<> and not ConcurrentDictionary<> has to do with 
            // the fact that running the delegate in the context of a system target coincidentally serializes access 
            // to the dictionary.
            var accum = new Dictionary<SiloAddress, SiloRuntimeStatistics>();
            return
                scheduler.RunOrQueueAsyncValue(
                    () =>
                    {
                        var promises =
                            members.Select(
                                siloAddress =>
                                     AsyncValue.FromTask(SiloControlFactory.GetSystemTarget(Constants.SiloControlId, siloAddress)
                                        .GetRuntimeStatistics()).ContinueWith(
                                        stats =>
                                        {
                                            accum.Add(siloAddress, stats);
                                            return AsyncCompletion.Done;
                                        },
                                        ex =>
                                        {
                                            SiloRuntimeStatistics stats;
                                            if (periodicStats.TryGetValue(siloAddress, out stats))
                                                accum.Add(siloAddress, stats);
                                            logger.Warn(
                                                ErrorCode.Dispatcher_RuntimeStatisticsUnavailable,
                                                "An unexpected exception was thrown by ISiloControl.GetRuntimeStatistics({0}). Stale statistics will be substituted. Exception details follow:\n{1}",
                                                ex);
                                            return AsyncCompletion.Done;
                                        }));
                        return AsyncCompletion.JoinAll(promises).ContinueWith(() => accum);
                    },
                // todo:[mlr] Gabi has mentioned that this should not be run on the membership grain's scheduing
                // context but it's not a priority to refactor this just yet.
                    ((SystemTarget)membership).SchedulingContext).
            ContinueWith(
                () =>
                {
                    UpdateStatistics(accum);
                    return accum;
                });
        }
    }
}
