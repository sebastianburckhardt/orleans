using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans.Samples.Tweeter.GrainInterfaces;
using Orleans;
using UnitTests;

namespace TweeterUnitTests
{
    [TestClass]
    public class TweeterTests : UnitTestBase
    {
        [TestCleanup]
        public void Cleanup()
        {
            //ResetDefaultRuntimes();
        }

        [TestMethod, TestCategory("Revisit"), TestCategory("Apps")]
        public void TweeterLookupAccountGrain()
        {
            //ITweeterTestAccountGrain g0 = TweeterTestAccountGrainFactory.CreateGrain(1111, "One");
            //g0.Wait();

            //ITweeterTestAccountGrain g1 = TweeterTestAccountGrainFactory.LookupUserId(1111);
            //g1.Wait();
            //Assert.AreEqual(1111, g1.UserId.Result);
            //Assert.AreEqual("One", g1.UserAlias.Result);

            //ITweeterTestAccountGrain g2 = TweeterTestAccountGrainFactory.LookupUserAlias("One");
            //g2.Wait();
            //Assert.AreEqual(1111, g2.UserId.Result);
            //Assert.AreEqual("One", g2.UserAlias.Result);

            //TweeterTestAccountGrainFactory.Delete(g0).Wait();
        }
    }
}
