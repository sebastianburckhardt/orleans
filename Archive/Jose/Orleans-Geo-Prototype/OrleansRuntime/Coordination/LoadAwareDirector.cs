using System;
using System.Linq;
using System.Threading.Tasks;

using Orleans.Counters;


namespace Orleans.Runtime.Coordination
{
    internal class LoadAwareDirector : RandomPlacementDirector
    {
        internal static readonly TimeSpan STATISTICS_FRESHNESS_TIME = TimeSpan.FromMinutes(5);

        private static int PlacementWeight(SiloRuntimeStatistics stats)
        {
            // Initial version: we only look at CPU usage, using a simple formula
            // CpuUsage ranges from 0-100; this formula gives a weight of 10 to a fully loaded silo,
            // and of 110 (11 times that) to a fully idle silo
            // [mlr] second verison: we don't allow overloaded silos to be candidates for selection.
            if (stats.IsOverloaded)
                return 0;
            else
                return Math.Max((int)(110 - stats.CpuUsage), 0);
        }

        protected override Task<PlacementResult>
            OnAddTarget(PlacementStrategy strategy, GrainId grain, IPlacementContext context)
        {
            var currentLoad =
                DeploymentLoadCollector.PeriodicStatistics.Where(
                    kvp =>
                        context.AllSilos.Contains(kvp.Key)).ToArray();
            var grainType = context.GetGrainTypeName(grain);

            var weights =
                currentLoad.Select(
                    kvp =>
                        PlacementWeight(kvp.Value)).ToArray();
            var totalWeight = weights.Sum();

            var ticket = (int)(_rng.NextDouble() * totalWeight);
            var n = 0;
            for (var i = 0; i < currentLoad.Length; ++i)
            {
                n += weights[i];
                if (n >= ticket)
                {
                    return
                        Task.FromResult(
                            PlacementResult.SpecifyCreation(
                                    currentLoad[i].Key,
                                    strategy,
                                    grainType));
                }
            }

            throw new InvalidOperationException("OnAddTarget() failed to complete as expected.");
        }
    }
}