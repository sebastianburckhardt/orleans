using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;


namespace Orleans.Runtime.Coordination
{
    internal class ExplicitPlacementDirector : PlacementDirector
    {
        protected override async Task<PlacementResult>
            OnSelectTarget(PlacementStrategy strategy, GrainId target, IPlacementContext context)
        {
            if (target.IsClient)
                throw new InvalidOperationException("Cannot use ExplicitPlacementStrategy to route messages to client grains.");

            var places = await context.Lookup(target);
            // [mlr] in this directory, the grain is guaranteed to be on one and only one silo. `places` contains activations that already exist-- if anything
            // is in this list, it needs to match the silo provided by the caller. also, places should contain no more than one activation.
            var placesAsList = places as IList<ActivationAddress> ?? places.ToList();
            switch (placesAsList.Count)
            {
                case 0:
                    // [mlr] we didn't find a suitable candidate; pass along to the next handler.
                    return (PlacementResult)null;
                case 1:
                    var a = placesAsList.First();
                    var explicitParams = (ExplicitPlacement)strategy;
                    if (a.Silo.Matches(explicitParams.Silo))
                        return PlacementResult.IdentifySelection(a);
                    else
                    throw new OrleansException(string.Format("Grain {0} is expected to be on silo {1} but was found on silo {2} instead. Please verify that there isn't a mismatch in explicit placement strategy specifications.", target.ToString(), explicitParams.Endpoint.ToString(), a.Silo.ToString()));
                default:
                    // [mlr] there should not be more than one activation when its placement is explicitly specified.
                    // todo:[mlr] how do i make explicit placement work with stateless grains?
                    throw new OrleansException(string.Format("All activations associated with grain {0} should reside on a single silo (due to an explicit placement strategy)-- instead they reside on {1} silos; this suggests a bug in Orleans.", target.ToString(), placesAsList.Count));
            }
        }

        protected override Task<PlacementResult>
            OnAddTarget(PlacementStrategy strategy, GrainId grain, IPlacementContext context)
        {
            var explicitParams = (ExplicitPlacement)strategy;
            int port = explicitParams.Silo.Endpoint.Port;
            var addr = explicitParams.Silo.Endpoint.Address;
            var allSilos = context.AllSilos;
            foreach (var siloAddress in allSilos)
            {
                if(siloAddress.Matches(explicitParams.Silo))
                    return
                        Task.FromResult(
                            PlacementResult.SpecifyCreation(
                                explicitParams.Silo,
                                strategy,
                                context.GetGrainTypeName(grain)));
            }
            
            throw new OrleansException(string.Format("No suitable silo for requested explicit placement strategy: {0}", explicitParams.Silo));
        }
    }
}
