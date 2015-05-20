using System;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using UnitTestGrainInterfaces;

namespace UnitTests.General
{
    [TestClass]
    public class KeyExtensionTests : UnitTestBase
    {
        [TestCleanup]
        public void TestCleanup()
        {
            CheckForUnobservedPromises();
        }

        [ClassCleanup]
        public static void ClassCleanup()
        {
            ResetDefaultRuntimes();
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("PrimaryKeyExtension")]
        public async Task PrimaryKeyExtensionsShouldDifferentiateGrainsUsingTheSameBasePrimaryKey()
        {
            var baseKey = Guid.NewGuid();

            const string kx1 = "1";
            const string kx2 = "2";

            var grain1 = KeyExtensionTestGrainFactory.GetGrain(baseKey, kx1);
            var grainId1 = await grain1.GetGrainId();
            var activationId1 = await grain1.GetActivationId();

            var grain2 = KeyExtensionTestGrainFactory.GetGrain(baseKey, kx2);
            var grainId2 = await grain2.GetGrainId();
            var activationId2 = await grain2.GetActivationId();

            Assert.AreNotEqual(
                grainId1,
                grainId2,
                "Mismatched key extensions should differentiate an identical base primary key.");

            Assert.AreNotEqual(
                activationId1,
                activationId2,
                "Mismatched key extensions should differentiate an identical base primary key.");
        }
    }
}
