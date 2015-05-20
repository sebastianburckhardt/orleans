using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;


using ProxyErrorGrain;
using UnitTestGrains;

namespace UnitTests
{
    /// <summary>
    /// Summary description for LocalCallsTests
    /// </summary>
    [TestClass]
    public class LocalCallsTests : UnitTestBase
    {
        public LocalCallsTests()
            : base(true)
        {
        }

        [TestCleanup]
        public void Cleanup()
        {
            //ResetAllAdditionalRuntimes();
            //ResetDefaultRuntimes();
        }


        #region Additional test attributes
        //
        // You can use the following additional attributes as you write your tests:
        //
        // Use ClassInitialize to run code before running the first test in the class
        // [ClassInitialize()]
        // public static void MyClassInitialize(TestContext testContext) { }
        //
        // Use ClassCleanup to run code after all tests in a class have run
        // [ClassCleanup()]
        // public static void MyClassCleanup() { }
        //
        // Use TestInitialize to run code before running each test 
        // [TestInitialize()]
        // public void MyTestInitialize() { }
        //
        // Use TestCleanup to run code after each test has run
        // [TestCleanup()]
        // public void MyTestCleanup() { }
        //
        #endregion

        [TestMethod, TestCategory("Revisit"), TestCategory("General")]
        public void LocalRetry()
        {
            //TimeSpan timeout = TimeSpan.FromMilliseconds(5000);

            //IProxyErrorGrain client = ProxyErrorGrainFactory.CreateGrain(Strategies: new[] { GrainStrategy.PartitionPlacement(0, 2) });

            //IErrorGrain reference = ErrorGrainFactory.CreateGrain(Strategies: new[] { GrainStrategy.PartitionPlacement(1, 2) });

            ////string hostId = client.GetRuntimeInstanceId().GetValue(timeout);
            ////string[] parts = hostId.Split(':');
            ////Assert.AreEqual("32445", parts[1]);
            //client.ConnectTo(reference).Wait(timeout);

            //client.SetA(1).Wait();

            //StartAdditionalOrleans();
            //client.SetA(2).Wait(timeout);
        }

        // TODO: [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        [TestMethod, TestCategory("Revisit"),]
        public void LocalError()
        {
            //TimeSpan timeout = TimeSpan.FromMilliseconds(15000);

            //IProxyErrorGrain client = ProxyErrorGrainFactory.CreateGrain(Strategies: new[] { GrainStrategy.PartitionPlacement(0, 2) });

            //IErrorGrain reference = ErrorGrainFactory.CreateGrain(Strategies: new[] { GrainStrategy.PartitionPlacement(1, 2) });
            //((GrainReference)reference).Wait();

            ////string hostId = client.GetRuntimeInstanceId().GetValue(timeout);
            ////string[] parts = hostId.Split(':');
            ////Assert.AreEqual("32445", parts[1]);
            //client.ConnectTo(reference).Wait(timeout);

            //client.SetA(1).Wait();

            //StartAdditionalOrleans();
            //try
            //{
            //    client.SetAError(2).Wait(timeout);
            //}
            //catch (Exception exc)
            //{
            //    Console.WriteLine(Logger.PrintException(exc));

            //    Exception e = exc.GetBaseException();
            //    if (e is TimeoutException)
            //        Assert.Fail("Timeout exception thrown instead of the expected error exception.");
            //    else if (e.Message != "SetAError-Exception")
            //    {
            //        Assert.Fail(String.Format("Unexpected exception: {0}", e));
            //    }
            //}
        }
    }
}
