using System;
using System.Collections.Generic;
using System.Threading.Tasks;



namespace Orleans.Runtime.Coordination
{
    internal abstract class PlacementDirector
    {
        private static readonly Dictionary<Type, PlacementDirector> Directors = new Dictionary<Type, PlacementDirector>();

        static PlacementDirector()
        {
            Register<RandomPlacement, RandomPlacementDirector>();
            Register<PreferLocalPlacement, PreferLocalPlacementDirector>();
            Register<ExplicitPlacement, ExplicitPlacementDirector>();
            Register<LocalPlacement, LocalPlacementDirector>();
            Register<GraphPartitionPlacement, GraphPartitionDirector>();
            Register<LoadAwarePlacement, LoadAwareDirector>();
        }

        private static void Register<TStrategy, TDirector>()
            where TDirector : PlacementDirector, new()
            where TStrategy : PlacementStrategy
        {
            Directors.Add(typeof(TStrategy), new TDirector());
        }

        private static PlacementDirector ResolveDirector(PlacementStrategy strategy)
        {
            return Directors[strategy.GetType()];
        }

        public static async Task<PlacementResult>
            SelectOrAddTarget(
                ActivationAddress sendingAddress,
                GrainId targetGrain,
                IPlacementContext context,
                PlacementStrategy strategy)
        {
            var actualStrategy = strategy ?? PlacementStrategy.GetDefault();
            var result = await SelectTarget(sendingAddress, targetGrain, context, actualStrategy);
                if (result != null)
                    return result;
            return await AddTarget(targetGrain, context, actualStrategy);
        }

        private static Task<PlacementResult>
            SelectTarget(
                ActivationAddress sendingAddress,
                GrainId targetGrain,
                IPlacementContext context,
                PlacementStrategy strategy)
        {
            PlacementDirector director = ResolveDirector(strategy);
            return director.OnSelectTarget(strategy, targetGrain, context);
        }

        private static Task<PlacementResult>
            AddTarget(
                GrainId grain,
                IPlacementContext context,
                PlacementStrategy strategy)
        {
            if (grain.IsClient)
                throw
                    new InvalidOperationException(
                        "Client grains are not activated using the placement subsystem.");

            PlacementDirector director = ResolveDirector(strategy);
            return director.OnAddTarget(strategy, grain, context);
        }

        protected abstract Task<PlacementResult>
            OnSelectTarget(PlacementStrategy strategy, GrainId target, IPlacementContext context);
        protected abstract Task<PlacementResult>
            OnAddTarget(PlacementStrategy strategy, GrainId grain, IPlacementContext context);
    }
}
