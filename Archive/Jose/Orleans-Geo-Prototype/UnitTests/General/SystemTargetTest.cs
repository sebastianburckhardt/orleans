using System;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;


using UnitTestGrainInterfaces;

namespace UnitTests.General
{
    [TestClass]
    public class SystemTargetTest : UnitTestBase
    {
        public SystemTargetTest()
            : base(true)
        {
        }

        // Use ClassInitialize to run code before running the first test in the class
        [ClassInitialize]
        public static void MyClassInitialize(TestContext testContext)
        {
            ResetDefaultRuntimes();
        }

        // Use ClassCleanup to run code after all tests in a class have run
        [ClassCleanup]
        public static void MyClassCleanup()
        {
            ResetDefaultRuntimes();
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public void SystemTarget_TaskBased()
        {
            var proxyGrain = TestSystemTargetProxyGrainFactory.GetGrain(1);
            proxyGrain.SimpleVoidMethod(Primary.Silo.SiloAddress).Wait();
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public void SystemTarget_TaskBasedInt()
        {
            var proxyGrain = TestSystemTargetProxyGrainFactory.GetGrain(1);
            var t = proxyGrain.GetTwo(Primary.Silo.SiloAddress);

            t.Wait();

            Assert.AreEqual(2, t.Result);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public void SystemTarget_TaskBasedInt_List()
        {
            var proxyGrain = TestSystemTargetProxyGrainFactory.GetGrain(1);
            var list = new List<SiloAddress> { Secondary.Silo.SiloAddress, Primary.Silo.SiloAddress, Secondary.Silo.SiloAddress };
            var t = proxyGrain.GetCount(Primary.Silo.SiloAddress, list);

            t.Wait();

            Assert.AreEqual(4, t.Result);
        }
    }
}