using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Orleans.Runtime.Coordination
{
    internal class RandomPlacementDirector : PlacementDirector
    {
        protected readonly SafeRandom _rng = new SafeRandom();

        protected override async Task<PlacementResult> 
            OnSelectTarget(PlacementStrategy strategy, GrainId target, IPlacementContext context)
        {
            // [mlr] Random placement requires no parameters but if it did, this is where we would obtain access to them.
            //var myParams = strategy as RandomPlacement;
            List<ActivationAddress> places = await context.Lookup(target);
            if (places.Count <= 0)
            {
                if (target.IsClient)
                    throw new KeyNotFoundException("No client activation for " + target);
                // [mlr] we return null to indicate that we were unable to select a target from places activations.
                return (PlacementResult)null;
            }
            else if (places.Count == 1)
            {
                return PlacementResult.IdentifySelection(places[0]);
            }
            else // places.Count > 0
            {
                // choose randomly if there is one, else make a new activation of the target
                // todo: remove hack? pick local if available
                var here = context.LocalSilo;
                //var placesAsList = places as IList<ActivationAddress> ?? places.ToList();
                // todo:[mlr] there's some distrust of how this algorithm was implemented. we've decided to keep the current
                // behavior for now but at a later point, we want to consider making this a purely random assignment of grains.
                var local = places.Where(a => a.Silo.Equals(here)).ToList();
                if (local.Count > 0)
                    return PlacementResult.IdentifySelection(local[_rng.Next(local.Count)]);
                if (places.Count > 0)
                    return PlacementResult.IdentifySelection(places[_rng.Next(places.Count)]);
                if (target.IsClient)
                    throw new KeyNotFoundException("No client activation for grain " + target.ToString());
                // [mlr] we return null to indicate that we were unable to select a target from places activations.
                return (PlacementResult)null;
            }
        }

        protected override Task<PlacementResult> 
            OnAddTarget(PlacementStrategy strategy, GrainId grain, IPlacementContext context)
        {
            var grainType = context.GetGrainTypeName(grain);
            var allSilos = context.AllSilos;
            return
                Task.FromResult(
                    PlacementResult.SpecifyCreation(
                        allSilos[_rng.Next(allSilos.Count)],
                        strategy,
                        grainType));
        }
    }
}
