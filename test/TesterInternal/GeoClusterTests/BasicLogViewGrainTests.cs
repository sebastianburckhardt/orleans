using System;
using System.IO;
using System.Threading.Tasks;
using System.Collections.Generic;
using UnitTests.GrainInterfaces;
using Orleans.TestingHost;
using Xunit;
using Assert = Xunit.Assert;

namespace Tests.GeoClusterTests
{
    public class BasicLogViewGrainTests : TestingSiloHost
    {
        public BasicLogViewGrainTests() :
            base(
                new TestingSiloOptions
                {
                    StartFreshOrleans = true,
                    StartPrimary = true,
                    StartSecondary = false,
                    SiloConfigFile = new FileInfo("OrleansConfigurationForTesting.xml"),
                    DataConnectionString = StorageTestConstants.DataConnectionString,
                    AdjustConfig = ReplicationProviderConfiguration.ConfigureLogViewProvidersForTesting
                }
            )

        {
            this.random = new Random();
        }

        private Random random;

        [Fact, TestCategory("GeoCluster")]
        public async Task BasicLogViewGrainTest_DefaultStorage()
        {
            await DoBasicLogViewGrainTest("UnitTests.Grains.SimpleLogViewGrainDefaultStorage");
        }
        [Fact, TestCategory("GeoCluster")]
        public async Task BasicLogViewGrainTest_MemoryStorage()
        {
            await DoBasicLogViewGrainTest("UnitTests.Grains.SimpleLogViewGrainMemoryStorage");
        }
        [Fact, TestCategory("GeoCluster")]
        public async Task BasicLogViewGrainTest_SharedStorage()
        {
            await DoBasicLogViewGrainTest("UnitTests.Grains.SimpleLogViewGrainSharedStorage");
        }
        [Fact, TestCategory("GeoCluster")]
        public async Task BasicLogViewGrainTest_CustomStorage()
        {
            await DoBasicLogViewGrainTest("UnitTests.Grains.SimpleLogViewGrainCustomStorage");
        }

        private int GetRandom()
        {
            lock (random)
                return random.Next();
        }


        private async Task DoBasicLogViewGrainTest(string grainClass, int phases = 100)
        {
            await ThreeCheckers(grainClass, phases);
        }

        private async Task ThreeCheckers(string grainClass, int phases)
        {
            // Global 
            Func<Task> checker1 = async () =>
            {
                int x = GetRandom();
                var grain = GrainFactory.GetGrain<ISimpleLogViewGrain>(x, grainClass);
                await grain.SetAGlobal(x);
                int a = await grain.GetAGlobal();
                Assert.Equal(x, a); // value of A survive grain call
                Assert.Equal(1, await grain.GetConfirmedVersion());
            };

            // Local
            Func<Task> checker2 = async () =>
            {
                int x = GetRandom();
                var grain = GrainFactory.GetGrain<ISimpleLogViewGrain>(x, grainClass);
                Assert.Equal(0, await grain.GetConfirmedVersion());
                await grain.SetALocal(x);
                int a = await grain.GetALocal();
                Assert.Equal(x, a); // value of A survive grain call
            };

            // Local then Global
            Func<Task> checker3 = async () =>
            {
                // Local then Global
                int x = GetRandom();
                var grain = GrainFactory.GetGrain<ISimpleLogViewGrain>(x, grainClass);
                await grain.SetALocal(x);
                int a = await grain.GetAGlobal();
                Assert.Equal(x, a);
                Assert.Equal(1, await grain.GetConfirmedVersion());
            };

            // test them in sequence
            await checker1();
            await checker2();
            await checker3();

            // test (phases) instances of each checker, all in parallel
            var tasks = new List<Task>();
            for (int i = 0; i < phases; i++)
            {
                tasks.Add(checker1());
                tasks.Add(checker2());
                tasks.Add(checker3());
            }
            await Task.WhenAll(tasks);
        }
    }
}
