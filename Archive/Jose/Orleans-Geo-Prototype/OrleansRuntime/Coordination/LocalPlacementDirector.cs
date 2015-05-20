using System;
using System.Collections.Generic;
using System.Threading.Tasks;


namespace Orleans.Runtime.Coordination
{
    internal class LocalPlacementDirector : PlacementDirector
    {
        private readonly SafeRandom _rng = new SafeRandom();

        protected override Task<PlacementResult>
            OnSelectTarget(PlacementStrategy strategy, GrainId target, IPlacementContext context)
        {
            if (target.IsClient)
                throw new InvalidOperationException("Cannot use LocalPlacementStrategy to route messages to client grains.");

            // todo:[mlr] summary of algorithm taken from the original version. once i've refactored the algorithm,
            // i'll finalize the description.
            //
            // Returns an existing activation of the grain or requests creation of a new one.
            // If there are available (not busy with a request) activations, it returns the first one
            // that exceeds the MinAvailable limit. E.g. if MinAvailable=1, it returns the second available.
            // If MinAvailable is less than 1, it returns the first available.
            // If the number of local activations reached or exceeded MaxLocal, it randomly returns one of them.
            // Otherwise, it requests creation of a new activation.
            ActivationData info;
            List<ActivationData> local;

            if (!context.LocalLookup(target, out local) || local.Count == 0)
                return Task.FromResult((PlacementResult)null);

            var localityPlacement = (LocalPlacement)strategy;
            List<ActivationId> available = null;

            foreach (var activation in local)
            {
                if (context.TryGetActivationData(activation.ActivationId, out info) && info.IsInactive)
                {
                    if (localityPlacement.MinAvailable < 1 || (available != null && available.Count >= localityPlacement.MinAvailable))
                        return Task.FromResult(PlacementResult.IdentifySelection(ActivationAddress.GetAddress(context.LocalSilo, target, activation.ActivationId)));

                    if (available == null)
                        available = new List<ActivationId>();
                    available.Add(info.ActivationId);
                }
            }

            if (local.Count >= localityPlacement.MaxLocal)
            {
                var id = local[local.Count == 1 ? 0 : _rng.Next(local.Count)].ActivationId;
                return Task.FromResult(PlacementResult.IdentifySelection(ActivationAddress.GetAddress(context.LocalSilo, target, id)));
            }

            return Task.FromResult((PlacementResult)null);
        }

        protected override Task<PlacementResult> OnAddTarget(PlacementStrategy strategy, GrainId grain, IPlacementContext context)
        {
            var grainType = context.GetGrainTypeName(grain);
            return
                Task.FromResult(
                    PlacementResult.SpecifyCreation(
                        context.LocalSilo,
                        strategy,
                        grainType));
        }
    }
}
