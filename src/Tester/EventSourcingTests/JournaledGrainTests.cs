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

namespace UnitTests.EventSourcingTests
{
    [TestClass]
    [DeploymentItem("OrleansGetEventStore.dll")]
    [DeploymentItem("EventStore.ClientAPI.dll")]
    public class JournaledGrainTests : HostedTestClusterPerFixture
    {
        public static TestingSiloHost CreateSiloHost()
        {
            return new TestingSiloHost(
                new TestingSiloOptions
                {
                    StartFreshOrleans = true,
                    StartPrimary = true,
                    StartSecondary = false,
                    SiloConfigFile = new FileInfo("OrleansConfigurationForTesting.xml"),
                    DataConnectionString = StorageTestConstants.DataConnectionString,
                    AdjustConfig = (ClusterConfiguration config) => {
                        config.Globals.RegisterLogViewProvider<MemoryEventStore>("TestEventStore");
                    }
                }
            );
        }
      
        [TestMethod, TestCategory("Functional"), TestCategory("EventSourcing")]
        public async Task JournaledGrainTests_Activate()
        {
            var grainWithState = GrainClient.GrainFactory.GetGrain<IJournaledPersonGrain>(Guid.Empty);

            Assert.IsNotNull(await grainWithState.GetPersonalAttributes());
        }

        [TestMethod, TestCategory("Functional"), TestCategory("EventSourcing")]
        public async Task JournaledGrainTests_Persist()
        {
            var grainWithState = GrainClient.GrainFactory.GetGrain<IJournaledPersonGrain>(Guid.Empty);

            await grainWithState.RegisterBirth(new PersonAttributes { FirstName = "Luke", LastName = "Skywalker", Gender = GenderType.Male });

            var attributes = await grainWithState.GetPersonalAttributes();

            Assert.IsNotNull(attributes);
            Assert.AreEqual("Luke", attributes.FirstName);
        }

        [TestMethod, TestCategory("Functional"), TestCategory("EventSourcing")]
        public async Task JournaledGrainTests_AppendMoreEvents()
        {
            var leia = GrainClient.GrainFactory.GetGrain<IJournaledPersonGrain>(Guid.NewGuid());
            await leia.RegisterBirth(new PersonAttributes { FirstName = "Leia", LastName = "Organa", Gender = GenderType.Female });

            var han = GrainClient.GrainFactory.GetGrain<IJournaledPersonGrain>(Guid.NewGuid());
            await han.RegisterBirth(new PersonAttributes { FirstName = "Han", LastName = "Solo", Gender = GenderType.Male });

            await leia.Marry(han);

            var attributes = await leia.GetPersonalAttributes();
            Assert.IsNotNull(attributes);
            Assert.AreEqual("Leia", attributes.FirstName);
            Assert.AreEqual("Solo", attributes.LastName);
        }

        [TestMethod, TestCategory("Functional"), TestCategory("EventSourcing")]
        public async Task JournaledGrainTests_TentativeConfirmedState()
        {
            var leia = GrainClient.GrainFactory.GetGrain<IJournaledPersonGrain>(Guid.NewGuid());

            Assert.AreEqual(0, await leia.GetConfirmedVersion());
            Assert.AreEqual(0, await leia.GetVersion());
            Assert.AreEqual(null, (await leia.GetConfirmedPersonalAttributes()).LastName);
            Assert.AreEqual(null, (await leia.GetPersonalAttributes()).LastName);

            await leia.ChangeLastName("Organa");

            Assert.AreEqual(0, await leia.GetConfirmedVersion());
            Assert.AreEqual(1, await leia.GetVersion());
            Assert.AreEqual(null, (await leia.GetConfirmedPersonalAttributes()).LastName);
            Assert.AreEqual("Organa", (await leia.GetPersonalAttributes()).LastName);

            await leia.SaveChanges();

            Assert.AreEqual(1, await leia.GetConfirmedVersion());
            Assert.AreEqual(1, await leia.GetVersion());
            Assert.AreEqual("Organa", (await leia.GetConfirmedPersonalAttributes()).LastName);
            Assert.AreEqual("Organa", (await leia.GetPersonalAttributes()).LastName);

            await leia.ChangeLastName("Solo");

            Assert.AreEqual(1, await leia.GetConfirmedVersion());
            Assert.AreEqual(2, await leia.GetVersion());
            Assert.AreEqual("Organa", (await leia.GetConfirmedPersonalAttributes()).LastName);
            Assert.AreEqual("Solo", (await leia.GetPersonalAttributes()).LastName);

            await leia.SaveChanges();

            Assert.AreEqual(2, await leia.GetConfirmedVersion());
            Assert.AreEqual(2, await leia.GetVersion());
            Assert.AreEqual("Solo", (await leia.GetConfirmedPersonalAttributes()).LastName);
            Assert.AreEqual("Solo", (await leia.GetPersonalAttributes()).LastName);
        }
    }
}