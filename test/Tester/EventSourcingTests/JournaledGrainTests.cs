using System;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using TestGrainInterfaces;
using UnitTests.Tester;
using Orleans.Runtime.Configuration;
using Orleans.Providers.EventStores;
using Orleans.TestingHost;
using System.IO;
using Orleans.Runtime;
using Xunit;
using Assert = Xunit.Assert;

namespace UnitTests.EventSourcingTests
{
    public class JournaledGrainTests : TestingSiloHost
    {
        public JournaledGrainTests()
        : base(new TestingSiloOptions
                {
                    StartFreshOrleans = true,
                    StartPrimary = true,
                    StartSecondary = false,
                    SiloConfigFile = new FileInfo("OrleansConfigurationForTesting.xml"),
                    DataConnectionString = StorageTestConstants.DataConnectionString,
                    AdjustConfig = (ClusterConfiguration config) => {
                        config.Globals.RegisterLogViewProvider<MemoryEventStore>("TestEventStore");
                        foreach (var o in config.Overrides)
                            o.Value.TraceLevelOverrides.Add(new Tuple<string, Severity>("LogViews", Severity.Verbose2));
                    }
                }
            )
        { }
      
        [Fact, TestCategory("EventSourcing"), TestCategory("Functional")]
        public async Task JournaledGrainTests_Activate()
        {
            var grainWithState = GrainClient.GrainFactory.GetGrain<IJournaledPersonGrain>(Guid.Empty);

            Assert.NotNull(await grainWithState.GetPersonalAttributes());
        }

        [Fact, TestCategory("EventSourcing"), TestCategory("Functional")]
        public async Task JournaledGrainTests_Persist()
        {
            var grainWithState = GrainClient.GrainFactory.GetGrain<IJournaledPersonGrain>(Guid.Empty);

            await grainWithState.RegisterBirth(new PersonAttributes { FirstName = "Luke", LastName = "Skywalker", Gender = GenderType.Male });

            var attributes = await grainWithState.GetPersonalAttributes();

            Assert.NotNull(attributes);
            Assert.Equal("Luke", attributes.FirstName);
        }

        [Fact, TestCategory("EventSourcing"), TestCategory("Functional")]
        public async Task JournaledGrainTests_AppendMoreEvents()
        {
            var leia = GrainClient.GrainFactory.GetGrain<IJournaledPersonGrain>(Guid.NewGuid());
            await leia.RegisterBirth(new PersonAttributes { FirstName = "Leia", LastName = "Organa", Gender = GenderType.Female });

            var han = GrainClient.GrainFactory.GetGrain<IJournaledPersonGrain>(Guid.NewGuid());
            await han.RegisterBirth(new PersonAttributes { FirstName = "Han", LastName = "Solo", Gender = GenderType.Male });

            await leia.Marry(han);

            var attributes = await leia.GetPersonalAttributes();
            Assert.NotNull(attributes);
            Assert.Equal("Leia", attributes.FirstName);
            Assert.Equal("Solo", attributes.LastName);
        }

        [Fact, TestCategory("EventSourcing"), TestCategory("Functional")]
        public async Task JournaledGrainTests_TentativeConfirmedState()
        {
            var leia = GrainClient.GrainFactory.GetGrain<IJournaledPersonGrain>(Guid.NewGuid());

            Assert.Equal(0, await leia.GetConfirmedVersion());
            Assert.Equal(0, await leia.GetVersion());
            Assert.Equal(null, (await leia.GetConfirmedPersonalAttributes()).LastName);
            Assert.Equal(null, (await leia.GetPersonalAttributes()).LastName);

            await leia.ChangeLastName("Organa");

            Assert.Equal(0, await leia.GetConfirmedVersion());
            Assert.Equal(1, await leia.GetVersion());
            Assert.Equal(null, (await leia.GetConfirmedPersonalAttributes()).LastName);
            Assert.Equal("Organa", (await leia.GetPersonalAttributes()).LastName);

            await leia.SaveChanges();

            Assert.Equal(1, await leia.GetConfirmedVersion());
            Assert.Equal(1, await leia.GetVersion());
            Assert.Equal("Organa", (await leia.GetConfirmedPersonalAttributes()).LastName);
            Assert.Equal("Organa", (await leia.GetPersonalAttributes()).LastName);

            await leia.ChangeLastName("Solo");

            Assert.Equal(1, await leia.GetConfirmedVersion());
            Assert.Equal(2, await leia.GetVersion());
            Assert.Equal("Organa", (await leia.GetConfirmedPersonalAttributes()).LastName);
            Assert.Equal("Solo", (await leia.GetPersonalAttributes()).LastName);

            await leia.SaveChanges();

            Assert.Equal(2, await leia.GetConfirmedVersion());
            Assert.Equal(2, await leia.GetVersion());
            Assert.Equal("Solo", (await leia.GetConfirmedPersonalAttributes()).LastName);
            Assert.Equal("Solo", (await leia.GetPersonalAttributes()).LastName);
        }
    }
}