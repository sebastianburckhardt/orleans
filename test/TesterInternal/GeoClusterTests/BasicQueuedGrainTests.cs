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
using System.IO;
using System.Threading.Tasks;
using System.Collections.Generic;
using UnitTests.GrainInterfaces;
using Orleans.TestingHost;
using Xunit;
using Assert = Xunit.Assert;

namespace Tests.GeoClusterTests
{
    public class BasicQueuedGrainTests : TestingSiloHost
    {
        public BasicQueuedGrainTests() :
            base(
                new TestingSiloOptions
                {
                    StartFreshOrleans = true,
                    StartPrimary = true,
                    StartSecondary = false,
                    SiloConfigFile = new FileInfo("OrleansConfigurationForTesting.xml"),
                    DataConnectionString = StorageTestConstants.DataConnectionString,
                    AdjustConfig = ReplicationProviderConfiguration.ConfigureAllReplicationProvidersForTesting
                }
            )

        {
            this.random = new Random();
        }

        private Random random;

        [Fact, TestCategory("GeoCluster")]
        public async Task BasicQueuedGrainTest_LocalMemoryStorage()
        {
            await DoBasicQueuedGrainTest("UnitTests.Grains.SimpleQueuedGrainLocalMemoryStorage");
        }
        [Fact, TestCategory("GeoCluster")]
        public async Task BasicQueuedGrainTest_DefaultStorage()
        {
            await DoBasicQueuedGrainTest("UnitTests.Grains.SimpleQueuedGrainDefaultStorage");
        }
        [Fact, TestCategory("GeoCluster")]
        public async Task BasicQueuedGrainTest_MemoryStorage()
        {
            await DoBasicQueuedGrainTest("UnitTests.Grains.SimpleQueuedGrainMemoryStorage");
        }
        [Fact, TestCategory("GeoCluster")]
        public async Task BasicQueuedGrainTest_SharedStorage()
        {
            await DoBasicQueuedGrainTest("UnitTests.Grains.SimpleQueuedGrainSharedStorage");
        }
        [Fact, TestCategory("GeoCluster")]
        public async Task BasicQueuedGrainTest_CustomStorage()
        {
            await DoBasicQueuedGrainTest("UnitTests.Grains.SimpleQueuedGrainCustomStorage");
        }

        private int GetRandom()
        {
            lock (random)
                return random.Next();
        }


        private async Task DoBasicQueuedGrainTest(string grainClass, int phases = 100)
        {
            await ThreeCheckers(grainClass, phases);
        }

        private async Task ThreeCheckers(string grainClass, int phases)
        {
            // Global 
            Func<Task> checker1 = async () =>
            {
                int x = GetRandom();
                var grain = GrainFactory.GetGrain<ISimpleQueuedGrain>(x, grainClass);
                await grain.SetAGlobal(x);
                int a = await grain.GetAGlobal();
                Assert.Equal(x, a); // value of A survive grain call
                Assert.Equal(1, await grain.GetConfirmedVersion());
            };

            // Local
            Func<Task> checker2 = async () =>
            {
                int x = GetRandom();
                var grain = GrainFactory.GetGrain<ISimpleQueuedGrain>(x, grainClass);
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
                var grain = GrainFactory.GetGrain<ISimpleQueuedGrain>(x, grainClass);
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
