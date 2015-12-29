using System;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using TestGrainInterfaces;
using UnitTests.Tester;

namespace UnitTests.EventSourcingTests
{
    [TestClass]
    public class JournaledGrainTests : UnitTestSiloHost
    {
        [TestMethod, TestCategory("Functional")]
        public async Task JournaledGrainTests_Activate()
        {
            var grainWithState = GrainClient.GrainFactory.GetGrain<IJournaledPersonGrain>(Guid.Empty);

            Assert.IsNotNull(await grainWithState.GetPersonalAttributes());
        }

        [TestMethod, TestCategory("Functional")]
        public async Task JournaledGrainTests_Persist()
        {
            var grainWithState = GrainClient.GrainFactory.GetGrain<IJournaledPersonGrain>(Guid.Empty);

            await grainWithState.RegisterBirth(new PersonAttributes { FirstName = "Luke", LastName = "Skywalker", Gender = GenderType.Male });

            var attributes = await grainWithState.GetPersonalAttributes();

            Assert.IsNotNull(attributes);
            Assert.AreEqual("Luke", attributes.FirstName);
        }

        [TestMethod, TestCategory("Functional")]
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
    }
}