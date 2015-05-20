#if !DISABLE_STREAMS

using System;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;

using Orleans.Streams;
using UnitTestGrains;
using System.IO;

namespace UnitTests.Streaming
{
    [DeploymentItem("Config_DevStorage.xml")]
    [DeploymentItem("Config_StreamProviders.xml")]
    [DeploymentItem("OrleansProviderInterfaces.dll")]
    [DeploymentItem("OrleansProviders.dll")]
    [TestClass]
    public class StreamProvidersTests_ProviderConfigNotLoaded : UnitTestBase
    {
        private static FileInfo SiloConfig = new FileInfo("Config_DevStorage.xml");

        public static readonly string STREAM_PROVIDER_NAME = "SMSProvider";

        public StreamProvidersTests_ProviderConfigNotLoaded()
            : base(new Options
                {
                    SiloConfigFile = SiloConfig
                })
        {
            // loading the default config, without stream providers.
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

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Streaming")]
        public void ProvidersTests_ConfigNotLoaded()
        {
            bool hasThrown = false;
            StreamId streamId = StreamId.NewRandomStreamId();
            // consumer joins first, producer later
            IStreaming_ConsumerGrain consumer = Streaming_ConsumerGrainFactory.GetGrain(2, "UnitTestGrains.Streaming_ConsumerGrain");
            try
            {
                consumer.BecomeConsumer(streamId, STREAM_PROVIDER_NAME).Wait();
            }catch(Exception exc)
            {
                hasThrown = true;
                Exception baseException = exc.GetBaseException();
                Assert.AreEqual(typeof(KeyNotFoundException), baseException.GetType());
            }
            Assert.IsTrue(hasThrown, "Should have thrown.");
        }
    }

    [DeploymentItem("Config_StreamProviders.xml")]
    [DeploymentItem("OrleansProviderInterfaces.dll")]
    [DeploymentItem("OrleansProviders.dll")]
    [TestClass]
    public class StreamProvidersTests_ProviderConfigLoaded : UnitTestBase
    {
        public StreamProvidersTests_ProviderConfigLoaded()
            : base(new Options {
                SiloConfigFile = new FileInfo("Config_StreamProviders.xml")
            })
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

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Streaming")]
        public void ProvidersTests_ProviderWrongName()
        {
            bool hasThrown = false;
            StreamId streamId = StreamId.NewRandomStreamId();
            // consumer joins first, producer later
            IStreaming_ConsumerGrain consumer = Streaming_ConsumerGrainFactory.GetGrain(2, "UnitTestGrains.Streaming_ConsumerGrain");
            try
            {
                consumer.BecomeConsumer(streamId, "WrongProviderName").Wait();
            }
            catch (Exception exc)
            {
                Exception baseException = exc.GetBaseException();
                Assert.AreEqual(typeof(KeyNotFoundException), baseException.GetType());
            }
            hasThrown = true;
            Assert.IsTrue(hasThrown);
        }
    }
}

#endif