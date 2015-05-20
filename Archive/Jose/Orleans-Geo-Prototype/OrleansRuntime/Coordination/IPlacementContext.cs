using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;



namespace Orleans.Runtime.Coordination
{
    internal interface IPlacementContext
    {
        Logger Logger { get; }

        /// <summary>
        /// Lookup locally known directory information for a target grain
        /// </summary>
        /// <param name="grain"></param>
        /// <param name="addresses">Local addresses will always be complete, remote may be partial</param>
        /// <returns>True if remote addresses are complete within freshness constraint</returns>
        bool FastLookup(GrainId grain, out List<ActivationAddress> addresses);

        AsyncValue<List<ActivationAddress>> FullLookup(GrainId grain);

        bool LocalLookup(GrainId grain, out List<ActivationData> addresses);

        List<SiloAddress> AllSilos { get; }

        SiloAddress LocalSilo { get; }

        /// <summary>
        /// Try to get the transaction state of the activation if it is available on this silo
        /// </summary>
        /// <param name="id"></param>
        /// <param name="activation"></param>
        /// <returns></returns>
        bool TryGetActivationData(ActivationId id, out ActivationData activationData);

        void GetGrainTypeInfo(int typeCode, out string grainClass, out PlacementStrategy placement, string genericArguments = null);
    }

    internal static class PlacementContextExtensions
    {
        public static Task<List<ActivationAddress>> Lookup(this IPlacementContext @this, GrainId grainId)
        {
            List<ActivationAddress> l;
            if (@this.FastLookup(grainId, out l))
                return Task.FromResult(l);
            else
                return @this.FullLookup(grainId).AsTask();
        }

        public static PlacementStrategy GetGrainPlacementStrategy(this IPlacementContext @this, int typeCode, string genericArguments = null)
        {
            string unused;
            PlacementStrategy placement;
            @this.GetGrainTypeInfo(typeCode, out unused, out placement, genericArguments);
            return placement;
        }

        public static PlacementStrategy GetGrainPlacementStrategy(this IPlacementContext @this, GrainId grainId, string genericArguments = null)
        {
            return @this.GetGrainPlacementStrategy(grainId.GetTypeCode(), genericArguments);
        }

        public static string GetGrainTypeName(this IPlacementContext @this, int typeCode, string genericArguments = null)
        {
            string grainClass;
            PlacementStrategy unused;
            @this.GetGrainTypeInfo(typeCode, out grainClass, out unused, genericArguments);
            return grainClass;
        }

        public static string GetGrainTypeName(this IPlacementContext @this, GrainId grainId, string genericArguments = null)
        {
            return @this.GetGrainTypeName(grainId.GetTypeCode(), genericArguments);
        }

        public static void GetGrainTypeInfo(this IPlacementContext @this, GrainId grainId, out string grainClass, out PlacementStrategy placement, string genericArguments = null)
        {
            @this.GetGrainTypeInfo(grainId.GetTypeCode(), out grainClass, out placement, genericArguments);
        }
    }
}