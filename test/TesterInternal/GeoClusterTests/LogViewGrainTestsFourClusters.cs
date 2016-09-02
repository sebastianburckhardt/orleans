using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;
using Orleans.TestingHost;
using UnitTests.GrainInterfaces;
using Orleans.Runtime;
using Tests.GeoClusterTests;
using Xunit;
using Xunit.Abstractions;
using Orleans.Runtime.Configuration;
using Tester;

namespace Tests.GeoClusterTests
{
    public class LogViewGrainTestsFourClusters :
       IClassFixture<LogViewGrainTestsFourClusters.Fixture>
    {

        public LogViewGrainTestsFourClusters(ITestOutputHelper output, Fixture fixture)
        {
            this.fixture = fixture;
            fixture.StartClustersIfNeeded(4, output);
        }
        Fixture fixture;

        public class Fixture : LogViewProviderTestFixture
        {
        }

        const int phases = 100;

        [Fact, TestCategory("GeoCluster")]
        public async Task TestBattery_SharedStorageProvider()
        {
            await fixture.RunChecksOnGrainClass("UnitTests.Grains.SimpleLogViewGrainSharedStorage", true, phases);
        }

        [Fact, TestCategory("GeoCluster")]
        public async Task TestBattery_SingleInstanceSharedStorage()
        {
            await fixture.RunChecksOnGrainClass("UnitTests.Grains.SimpleLogViewGrainSingleInstance", true, phases);
        }
        [Fact, TestCategory("GeoCluster")]
        public async Task TestBattery_SharedMemoryProvider()
        {
            await fixture.RunChecksOnGrainClass("UnitTests.Grains.SimpleLogViewGrainSharedMemory", true, phases);
        }

        [Fact, TestCategory("GeoCluster")]
        public async Task TestBattery_CustomStorageProvider()
        {
            await fixture.RunChecksOnGrainClass("UnitTests.Grains.SimpleLogViewGrainCustomStorage", true, phases);
        }

        [Fact, TestCategory("GeoCluster")]
        public async Task TestBattery_CustomStorageProvider_PrimaryCluster()
        {
            await fixture.RunChecksOnGrainClass("UnitTests.Grains.SimpleLogViewGrainCustomStoragePrimaryCluster", false, phases);
        }

    }
}
