using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Orleans;

using Orleans.Management;
using Orleans.Runtime;


using UnitTestGrainInterfaces;

namespace UnitTestGrains
{
    internal abstract class PlacementTestGrainBase : GrainBase
    {
        public Task<IPEndPoint> GetEndpoint()
        {
            return Task.FromResult(_Data.Silo.Endpoint);
        }

        public Task<string> GetRuntimeInstanceId()
        {
            return Task.FromResult(RuntimeIdentity);
        }

        public Task<ActivationId> GetActivationId()
        {
            return Task.FromResult(_Data.ActivationId);
        }

        public Task Nop()
        {
            return TaskDone.Done;
        }

        public Task StartLocalGrains(List<long> keys)
        {
            // [mlr] we call Nop() on the grain references to ensure that they're instantiated before the promise is delivered.
            var grains = keys.Select(i => LocalPlacementTestGrainFactory.GetGrain(i));
            var promises = grains.Select(g => g.Nop());
            return Task.WhenAll(promises);
        }

        public async Task<long> StartPreferLocalGrain(long key)
        {
            // [mlr] we call Nop() on the grain references to ensure that they're instantiated before the promise is delivered.
            await PreferLocalPlacementTestGrainFactory.GetGrain(key).Nop();
            return key;
        }

        private static IEnumerable<Task<IPEndPoint>> SampleLocalGrainEndpoint(ILocalPlacementTestGrain grain, int sampleSize)
        {
            for (var i = 0; i < sampleSize; ++i)
                yield return grain.GetEndpoint();
        }

        public async Task<List<IPEndPoint>> SampleLocalGrainEndpoint(long key, int sampleSize)
        {
            var grain = LocalPlacementTestGrainFactory.GetGrain(key);
            var p = await Task<IPEndPoint>.WhenAll(SampleLocalGrainEndpoint(grain, sampleSize));
            return p.ToList();
        }

        private static async Task PropigateStatisticsToCluster()
        {
            // [mlr] force the latched statistics to propigate throughout the cluster.
            IOrleansManagementGrain mgmtGrain =
                OrleansManagementGrainFactory.GetGrain(RuntimeInterfaceConstants.SystemManagementId);

            var hosts = await mgmtGrain.GetHosts(true);
            var keys = hosts.Select(kvp => kvp.Key).ToArray();
            await mgmtGrain.ForceRuntimeStatisticsCollection(keys);
        }

        public Task LatchOverloaded()
        {
            Silo.CurrentSilo.Metrics.LatchIsOverload(true);
            return PropigateStatisticsToCluster();
        }

        public Task UnlatchOverloaded()
        {
            Silo.CurrentSilo.Metrics.UnlatchIsOverloaded();
            return PropigateStatisticsToCluster();
        }

        public Task LatchCpuUsage(float value)
        {
            Silo.CurrentSilo.Metrics.LatchCpuUsage(value);
            return PropigateStatisticsToCluster();
        }

        public Task UnlatchCpuUsage()
        {
            Silo.CurrentSilo.Metrics.UnlatchCpuUsage();
            return PropigateStatisticsToCluster();
        }
    }

    internal class RandomPlacementTestGrain : 
        PlacementTestGrainBase, IRandomPlacementTestGrain
    {}

    internal class PreferLocalPlacementTestGrain :
       PlacementTestGrainBase, IPreferLocalPlacementTestGrain
    { }

    internal class LocalPlacementTestGrain : 
        PlacementTestGrainBase, ILocalPlacementTestGrain
    {}

    internal class LoadAwarePlacementTestGrain : 
        PlacementTestGrainBase, ILoadAwarePlacementTestGrain
    {}

    internal class ExplicitPlacementTestGrain : 
        PlacementTestGrainBase, IExplicitPlacementTestGrain
    {}


    //----------------------------------------------------------//
    // Grains for LocalContent grain case, when grain is activated on every silo by bootstrap provider.

    public class LocalContentGrain : GrainBase, ILocalContentGrain
    {
        private OrleansLogger logger;
        private object cachedContent;
        internal static ILocalContentGrain InstanceIdForThisSilo;
        
        public override Task ActivateAsync()
        {
            this.logger = GetLogger();
            logger.Info("ActivateAsync");
            DelayDeactivation(TimeSpan.MaxValue);   // make sure this activation is not collected.
            cachedContent = RuntimeIdentity;        // store your silo identity as a local cached content in this grain.
            InstanceIdForThisSilo = LocalContentGrainFactory.Cast(this.AsReference());
            return Task.FromResult(0);
        }

        public Task Init()
        {
            logger.Info("Init LocalContentGrain on silo " + RuntimeIdentity);
            return Task.FromResult(0);
        }

        public Task<object> GetContent()
        {
            return Task<object>.FromResult(cachedContent);
        }
    }

    public class TestContentGrain : GrainBase, ITestContentGrain
    {
        public Task<string> GetRuntimeInstanceId()
        {
            return Task.FromResult(RuntimeIdentity);
        }

        public Task<object> FetchContentFromLocalGrain()
        {
            return LocalContentGrain.InstanceIdForThisSilo.GetContent();
        }
    }

}
