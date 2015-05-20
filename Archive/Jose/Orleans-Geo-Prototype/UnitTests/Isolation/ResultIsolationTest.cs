using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;

using UnitTestGrainInterfaces;

namespace UnitTests.Isolation
{
    [TestClass]
    public class ResultIsolationTest : UnitTestBase
    {
        [TestCleanup]
        public void MyTestCleanup()
        {
            //ResetDefaultRuntimes();
        }

        [TestMethod, TestCategory("Revisit"), TestCategory("Isolation")]
        public void Isolation_ImmutableResult()
        {
            //var grain = ResultIsolationGrainFactory.CreateGrain();
            //grain.Wait();
            //var grain2 = ResultIsolationGrain2Factory.CreateGrain(new[] { GrainStrategy.AffinityPlacement(grain) });
            //var promise = grain.CheckResultIsolation(grain2);
            //try
            //{
            //    promise.Wait();
            //}
            //catch (AggregateException ex)
            //{
            //    Assert.Fail(ex.GetBaseException().Message);
            //}
        }
    }
}
