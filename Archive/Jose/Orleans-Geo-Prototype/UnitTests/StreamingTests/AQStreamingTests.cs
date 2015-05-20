#if !DISABLE_STREAMS

using System;
using System.IO;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using Orleans.Providers;
using Orleans.AzureUtils;
using UnitTestGrains;
using UnitTests.StorageTests;
using Orleans.Streams;
using Orleans.Providers.Streams.Persistent.AzureQueueAdapter;

namespace UnitTests.Streaming
{
    [DeploymentItem("Config_StreamProviders.xml")]
    [DeploymentItem("ClientConfig_StreamProviders.xml")]
    [DeploymentItem("OrleansProviderInterfaces.dll")]
    [DeploymentItem("OrleansProviders.dll")]
    [TestClass]
    public class AQStreamingTests : UnitTestBase
    {
        private SingleStreamTestRunner runner;
        private static Options aqSiloOptions = new Options
            {
                StartFreshOrleans = true,
                SiloConfigFile = SMSStreamingTests.SiloConfig,
                PickNewDeploymentId = true
            };
        private static ClientOptions aqClientOptions = new ClientOptions
            {
                ClientConfigFile = SMSStreamingTests.ClientConfig
            };
        private static Options aqSiloOption_OnlyPrimary = new Options
            {
                StartFreshOrleans = true,
                SiloConfigFile = SMSStreamingTests.SiloConfig,
                StartSecondary = false,
                PickNewDeploymentId = true,
            };

        private static Options aqSiloOption_NoClient = new Options
            {
                StartFreshOrleans = true,
                SiloConfigFile = SMSStreamingTests.SiloConfig,
                PickNewDeploymentId = true,
                StartClient = false
            };

        public AQStreamingTests()
            : base(aqSiloOptions, aqClientOptions)
        {
            runner = new SingleStreamTestRunner(SingleStreamTestRunner.AQ_STREAM_PROVIDER_NAME, 0, true);
        }

        public AQStreamingTests(int dummy)
            : base(aqSiloOption_OnlyPrimary, aqClientOptions)
        {
            runner = new SingleStreamTestRunner(SingleStreamTestRunner.AQ_STREAM_PROVIDER_NAME, 0, true);
        }

        public AQStreamingTests(string dummy)
            : base(aqSiloOption_NoClient)
        {
            runner = new SingleStreamTestRunner(SingleStreamTestRunner.AQ_STREAM_PROVIDER_NAME, 0, true);
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

        //[TestInitialize]
        //public void TestInitialize()
        //{
        //    //DeleteAllQueues();
        //}

        [TestCleanup]
        public void TestCleanup()
        {
            DeleteAllAzureQueues((new AzureQueueStreamQueueMapper()).GetAllQueues(), UnitTestBase.DeploymentId, StandaloneAzureQueueTests.DataConnectionString);
        }

        ////------------------------ One to One ----------------------//

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Streaming")]
        public async Task AQ_01_OneProducerGrainOneConsumerGrain()
        {
            await runner.StreamTest_01_OneProducerGrainOneConsumerGrain();
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Streaming")]
        public async Task AQ_02_OneProducerGrainOneConsumerClient()
        {
            await runner.StreamTest_02_OneProducerGrainOneConsumerClient();
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Streaming")]
        public async Task AQ_03_OneProducerClientOneConsumerGrain()
        {
            await runner.StreamTest_03_OneProducerClientOneConsumerGrain();
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Streaming")]
        public async Task AQ_04_OneProducerClientOneConsumerClient()
        {
            await runner.StreamTest_04_OneProducerClientOneConsumerClient();
        }

        //------------------------ MANY to Many different grains ----------------------//

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Streaming")]
        public async Task AQ_05_ManyDifferent_ManyProducerGrainsManyConsumerGrains()
        {
            await runner.StreamTest_05_ManyDifferent_ManyProducerGrainsManyConsumerGrains();
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Streaming")]
        public async Task AQ_06_ManyDifferent_ManyProducerGrainManyConsumerClients()
        {
            await runner.StreamTest_06_ManyDifferent_ManyProducerGrainManyConsumerClients();
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Streaming")]
        public async Task AQ_07_ManyDifferent_ManyProducerClientsManyConsumerGrains()
        {
            await runner.StreamTest_07_ManyDifferent_ManyProducerClientsManyConsumerGrains();
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Streaming")]
        public async Task AQ_08_ManyDifferent_ManyProducerClientsManyConsumerClients()
        {
            await runner.StreamTest_08_ManyDifferent_ManyProducerClientsManyConsumerClients();
        }

        //------------------------ MANY to Many Same grains ----------------------//
        [TestMethod, TestCategory("Nightly"), TestCategory("Streaming")]
        public async Task AQ_09_ManySame_ManyProducerGrainsManyConsumerGrains()
        {
            await runner.StreamTest_09_ManySame_ManyProducerGrainsManyConsumerGrains();
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Streaming")]
        public async Task AQ_10_ManySame_ManyConsumerGrainsManyProducerGrains()
        {
            await runner.StreamTest_10_ManySame_ManyConsumerGrainsManyProducerGrains();
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Streaming")]
        public async Task AQ_11_ManySame_ManyProducerGrainsManyConsumerClients()
        {
            await runner.StreamTest_11_ManySame_ManyProducerGrainsManyConsumerClients();
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Streaming")]
        public async Task AQ_12_ManySame_ManyProducerClientsManyConsumerGrains()
        {
            await runner.StreamTest_12_ManySame_ManyProducerClientsManyConsumerGrains();
        }

        //------------------------ MANY to Many producer consumer same grain ----------------------//

        [TestMethod, TestCategory("Nightly"), TestCategory("Streaming")]
        public async Task AQ_13_SameGrain_ConsumerFirstProducerLater()
        {
            await runner.StreamTest_13_SameGrain_ConsumerFirstProducerLater(false);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Streaming")]
        public async Task AQ_14_SameGrain_ProducerFirstConsumerLater()
        {
            await runner.StreamTest_14_SameGrain_ProducerFirstConsumerLater(false);
        }

        //----------------------------------------------//

        [TestMethod, TestCategory("Nightly"), TestCategory("Streaming")]
        public async Task AQ_15_ConsumeAtProducersRequest()
        {
            await runner.StreamTest_15_ConsumeAtProducersRequest();
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Streaming")]
        public async Task AQ_16_MultipleStreams_ManyDifferent_ManyProducerGrainsManyConsumerGrains()
        {
            var multiRunner = new MultipleStreamsTestRunner(SingleStreamTestRunner.AQ_STREAM_PROVIDER_NAME, 16, false);
            await multiRunner.StreamTest_MultipleStreams_ManyDifferent_ManyProducerGrainsManyConsumerGrains();
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Streaming")]
        public async Task AQ_17_MultipleStreams_1J_ManyProducerGrainsManyConsumerGrains()
        {
            var multiRunner = new MultipleStreamsTestRunner(SingleStreamTestRunner.AQ_STREAM_PROVIDER_NAME, 17, false);
            await multiRunner.StreamTest_MultipleStreams_ManyDifferent_ManyProducerGrainsManyConsumerGrains(
                StartAdditionalOrleans);
        }

        //[TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Streaming")]
        public async Task AQ_18_MultipleStreams_1J_1F_ManyProducerGrainsManyConsumerGrains()
        {
            var multiRunner = new MultipleStreamsTestRunner(SingleStreamTestRunner.AQ_STREAM_PROVIDER_NAME, 18, false);
            await multiRunner.StreamTest_MultipleStreams_ManyDifferent_ManyProducerGrainsManyConsumerGrains(
                StartAdditionalOrleans, 
                StopRuntime);
        }
    }
}

#endif
