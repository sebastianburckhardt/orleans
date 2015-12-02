/*
Project Orleans Cloud Service SDK ver. 1.0
 
Copyright (c) Microsoft Corporation
 
All rights reserved.
 
MIT License

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and 
associated documentation files (the ""Software""), to deal in the Software without restriction,
including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense,
and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.GrainDirectory;

namespace Orleans.Runtime.Placement
{
    internal interface IPlacementContext
    {
        TraceLogger Logger { get; }

        /// <summary>
        /// Lookup locally known directory information for a target grain
        /// </summary>
        /// <param name="grain"></param>
        /// <param name="addresses">Local addresses will always be complete, remote may be partial</param>
        /// <returns>True if remote addresses are complete within freshness constraint</returns>
        bool FastLookup(GrainId grain, out List<ActivationAddress> addresses);

        Task<Tuple<List<ActivationAddress>, int>> FullLookup(GrainId grain);

        bool LocalLookup(GrainId grain, out List<ActivationData> addresses);

        List<SiloAddress> AllActiveSilos { get; }

        SiloAddress LocalSilo { get; }

        /// <summary>
        /// Try to get the transaction state of the activation if it is available on this silo
        /// </summary>
        /// <param name="id"></param>
        /// <param name="activationData"></param>
        /// <returns></returns>
        bool TryGetActivationData(ActivationId id, out ActivationData activationData);

        void GetGrainTypeInfo(int typeCode, out string grainClass, out PlacementStrategy placement, out MultiClusterRegistrationStrategy strategy, string genericArguments = null);
    }

    internal static class PlacementContextExtensions
    {
        public static async Task<List<ActivationAddress>> Lookup(this IPlacementContext @this, GrainId grainId)
        {
            List<ActivationAddress> l;
            if (@this.FastLookup(grainId, out l))
            {
                return l;
            }
            else
            {
                var result = await @this.FullLookup(grainId);
                return result == null ? null : result.Item1;
            }
            
        }

        public static PlacementStrategy GetGrainPlacementStrategy(this IPlacementContext @this, int typeCode, string genericArguments = null)
        {
            string unused;
            PlacementStrategy placement;
            MultiClusterRegistrationStrategy unusedActivationStrategy;
            @this.GetGrainTypeInfo(typeCode, out unused, out placement, out unusedActivationStrategy, genericArguments);
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
            MultiClusterRegistrationStrategy unusedActivationStrategy;
            @this.GetGrainTypeInfo(typeCode, out grainClass, out unused, out unusedActivationStrategy, genericArguments);
            return grainClass;
        }

        public static string GetGrainTypeName(this IPlacementContext @this, GrainId grainId, string genericArguments = null)
        {
            return @this.GetGrainTypeName(grainId.GetTypeCode(), genericArguments);
        }

        public static void GetGrainTypeInfo(this IPlacementContext @this, GrainId grainId, out string grainClass, out PlacementStrategy placement, out MultiClusterRegistrationStrategy activationStrategy, string genericArguments = null)
        {
            @this.GetGrainTypeInfo(grainId.GetTypeCode(), out grainClass, out placement, out activationStrategy, genericArguments);
        }
    }
}
