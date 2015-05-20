using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using GrainClientGenerator;
using Orleans.Counters;
using Orleans.Runtime.Coordination;


namespace Orleans.Runtime
{
    internal class TargetDirectory : IEnumerable<KeyValuePair<ActivationId, ActivationData>>
    {
        private static readonly Logger logger = Logger.GetLogger("TargetDirectory", Logger.LoggerType.Runtime);

        private readonly ConcurrentDictionary<ActivationId, ActivationData> activations;                // Activation data (app grains) only.
        private readonly ConcurrentDictionary<ActivationId, SystemTarget> systemTargets;                // SystemTarget only.
        private readonly ConcurrentDictionary<GrainId, List<ActivationData>> grainToActivationsMap;     // Activation data (app grains) only.
        private readonly ConcurrentDictionary<string, CounterStatistic> grainCounts;                    // simple statistics type->count

        internal TargetDirectory()
        {
            activations = new ConcurrentDictionary<ActivationId, ActivationData>();
            systemTargets = new ConcurrentDictionary<ActivationId, SystemTarget>();
            grainToActivationsMap = new ConcurrentDictionary<GrainId, List<ActivationData>>();
            grainCounts = new ConcurrentDictionary<string, CounterStatistic>();
        }

        public int Count
        {
            get { return activations.Count; }
        }

        public IEnumerable<SystemTarget> AllSystemTargets()
        {
            return systemTargets.Values;
        }

        public ActivationData FindTarget(ActivationId key)
        {
            ActivationData target;
            return activations.TryGetValue(key, out target) ? target : null;
        }

        public SystemTarget FindSystemTarget(ActivationId key)
        {
            SystemTarget target;
            return systemTargets.TryGetValue(key, out target) ? target : null;
        }

        internal void IncrementGrainCounter(string grainTypeName)
        {
            if (logger.IsVerbose2) logger.Verbose2("Increment Grain Counter {0}", grainTypeName);
            CounterStatistic ctr = FindGrainCounter(grainTypeName);
            ctr.Increment();
        }
        internal void DecrementGrainCounter(string grainTypeName)
        {
            if (logger.IsVerbose2) logger.Verbose2("Decrement Grain Counter {0}", grainTypeName);
            CounterStatistic ctr = FindGrainCounter(grainTypeName);
            ctr.IncrementBy(-1);
        }

        private CounterStatistic FindGrainCounter(string grainTypeName)
        {
            if (grainTypeName.EndsWith(GrainClientGenerator.GrainInterfaceData.ActivationClassNameSuffix))
            {
                grainTypeName = grainTypeName.Substring(0, grainTypeName.Length - GrainClientGenerator.GrainInterfaceData.ActivationClassNameSuffix.Length);
            }
            CounterStatistic ctr;
            if (!grainCounts.TryGetValue(grainTypeName, out ctr))
            {
                var counterName = new StatName(StatNames.STAT_GRAIN_COUNTS_PER_GRAIN, grainTypeName);
                ctr = grainCounts[grainTypeName] = CounterStatistic.FindOrCreate(counterName, false);
            }
            return ctr;
        }

        public void RecordNewTarget(ActivationData target)
        {
            if (!activations.TryAdd(target.ActivationId, target))
                return;
            grainToActivationsMap.AddOrUpdate(target.Grain,
                g => new List<ActivationData> { target },
                (g, list) => { lock (list) { list.Add(target); } return list; });
        }

        public void RecordNewSystemTarget(SystemTarget target)
        {
            systemTargets.TryAdd(target.ActivationId, target);
        }

        public void RemoveSystemTarget(SystemTarget target)
        {
            SystemTarget ignore;
            systemTargets.TryRemove(target.ActivationId, out ignore);
        }

        public void RemoveTarget(ActivationData target)
        {
            ActivationData ignore;
            if (!activations.TryRemove(target.ActivationId, out ignore))
                return;
            List<ActivationData> list;
            if (grainToActivationsMap.TryGetValue(target.Grain, out list))
            {
                lock (list)
                {
                    list.Remove(target);
                    if (list.Count == 0)
                    {
                        List<ActivationData> list2; // == list
                        if (grainToActivationsMap.TryRemove(target.Grain, out list2))
                        {
                            lock (list2)
                            {
                                if (list2.Count > 0)
                                {
                                    grainToActivationsMap.AddOrUpdate(target.Grain,
                                        g => list2,
                                        (g, list3) => { lock (list3) { list3.AddRange(list2); } return list3; });
                                }
                            }
                        }
                    }
                }
            }
        }

        // Returns null if no activations exist for this grain ID, rather than an empty list
        public List<ActivationData> FindTargets(GrainId key)
        {
            List<ActivationData> tmp;
            if (grainToActivationsMap.TryGetValue(key, out tmp))
            {
                lock (tmp)
                {
                    return tmp.ToList();
                }
            }
            return null;
        }

        public IEnumerable<KeyValuePair<string, long>> GetSimpleGrainStatistics()
        {
            return grainCounts
                .Select(s =>
                {
                    var grainTypeName = s.Key;
                    if (grainTypeName.EndsWith(GrainClientGenerator.GrainInterfaceData.ActivationClassNameSuffix))
                    {
                        grainTypeName = grainTypeName.Substring(0, grainTypeName.Length - GrainClientGenerator.GrainInterfaceData.ActivationClassNameSuffix.Length);
                    }

                    return new KeyValuePair<string, long>(grainTypeName, s.Value.GetCurrentValue());
                })
                .Where(p => p.Value > 0);
        }

        public void PrintActivationDirectory()
        {
            if (logger.IsInfo)
            {
                string stats = Utils.IEnumerableToString(activations.Values.OrderBy(act => act.Name), act => string.Format("++{0}", act.DumpStatus()), "\r\n");
                if (stats.Length > 0)
                {
                    logger.LogWithoutBulkingAndTruncating(OrleansLogger.Severity.Info, (ErrorCode)0, String.Format("ActivationDirectory:\n{0}", stats));
                }
            }
        }

        #region Implementation of IEnumerable

        public IEnumerator<KeyValuePair<ActivationId, ActivationData>> GetEnumerator()
        {
            return activations.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion
    }
}
