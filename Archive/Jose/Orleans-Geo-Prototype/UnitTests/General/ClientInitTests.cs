using System;
using System.Collections.Generic;
using System.Net;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;


namespace UnitTests
{
    [TestClass]
    public class ClientInitTests : UnitTestBase
    {
        public ClientInitTests()
            : base(new Options { StartSecondary = false })
        {
        }

        [TestCleanup()]
        public void Cleanup()
        {
            ResetAllAdditionalRuntimes();
        }

        [ClassCleanup()]
        public static void MyClassCleanup()
        {
            ResetDefaultRuntimes();
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public void ClientInit_IsInitialized()
        {
            // First initialize will have been done by orleans unit test base class

            Assert.IsTrue(OrleansClient.IsInitialized);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public void ClientInit_Uninitialize()
        {
            OrleansClient.Uninitialize();
            Assert.IsFalse(OrleansClient.IsInitialized);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public void ClientInit_UnThenReinitialize()
        {
            OrleansClient.Uninitialize();
            Assert.IsFalse(OrleansClient.IsInitialized);

            OrleansClient.Initialize();
            Assert.IsTrue(OrleansClient.IsInitialized);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public void ClientInit_MultiInitialize()
        {
            // First initialize will have been done by orleans unit test base class

            OrleansClient.Initialize();
            Assert.IsTrue(OrleansClient.IsInitialized);

            OrleansClient.Initialize();
            Assert.IsTrue(OrleansClient.IsInitialized);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public void ClientInit_ErrorDuringInitialize()
        {
            ClientConfiguration cfg = new ClientConfiguration
            {
                TraceFileName = "TestOnlyThrowExceptionDuringInit.log",
                Gateways = new List<IPEndPoint>
                {
                    new IPEndPoint(IPAddress.Loopback, 30000)                        
                },
            };

            // First initialize will have been done by orleans unit test base class, so uninitialize back to null state
            OrleansClient.Uninitialize();
            Assert.IsFalse(OrleansClient.IsInitialized, "OrleansClient.IsInitialized");
            Assert.IsFalse(Logger.IsInitialized, "Logger.IsInitialized");

            try
            {
                OutsideGrainClient.TestOnlyThrowExceptionDuringInit = true;
                try
                {
                    OrleansClient.Initialize(cfg);
                    Assert.Fail("Expected to get exception during Client.Initialize when TestOnlyThrowExceptionDuringInit=true");
                }
                catch (Exception exc)
                {
                    Console.WriteLine("Expected to get exception during Client.Initialize: {0}", exc);
                }
                Assert.IsFalse(OrleansClient.IsInitialized, "OrleansClient.IsInitialized");
                Assert.IsFalse(Logger.IsInitialized, "Logger.IsInitialized");

                OutsideGrainClient.TestOnlyThrowExceptionDuringInit = false;

                OrleansClient.Initialize(cfg);
                Assert.IsTrue(OrleansClient.IsInitialized, "OrleansClient.IsInitialized");
                Assert.IsTrue(Logger.IsInitialized, "Logger.IsInitialized");
            }
            finally
            {
                OutsideGrainClient.TestOnlyThrowExceptionDuringInit = false;
            }
        }
        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public void ClientInit_InitializeUnThenReInit()
        {
            OrleansClient.Initialize();
            Assert.IsTrue(OrleansClient.IsInitialized);

            OrleansClient.Uninitialize();
            Assert.IsFalse(OrleansClient.IsInitialized);

            OrleansClient.Initialize();
            Assert.IsTrue(OrleansClient.IsInitialized);

            OrleansClient.Uninitialize();
            Assert.IsFalse(OrleansClient.IsInitialized);
        }


        [TestMethod, TestCategory("Revisit"), TestCategory("General")]
        //[ExpectedException(typeof(InvalidOperationException))]
        public void ClientInit_TryToCreateGrainWhenUninitialized()
        {
            //OrleansClient.Uninitialize();
            //Assert.IsFalse(OrleansClient.IsInitialized);
            //ISimpleGrain grain = SimpleGrainFactory.GetGrain(GetRandomGrainId());
            //grain.Wait();
            //Assert.Fail("CreateGrain should have failed before this point");
        }
    }
}
