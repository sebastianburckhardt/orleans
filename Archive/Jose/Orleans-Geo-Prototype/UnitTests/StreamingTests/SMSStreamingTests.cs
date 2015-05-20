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
using Orleans.Providers.Streams.SimpleMessageStream;

namespace UnitTests.Streaming
{
    [DeploymentItem("Config_StreamProviders.xml")]
    [DeploymentItem("ClientConfig_StreamProviders.xml")]
    [DeploymentItem("OrleansProviderInterfaces.dll")]
    [DeploymentItem("OrleansProviders.dll")]
    [TestClass]
    public class SMSStreamingTests : UnitTestBase
    {
        private SingleStreamTestRunner runner;
        internal static FileInfo SiloConfig = new FileInfo("Config_StreamProviders.xml");
        internal static FileInfo ClientConfig = new FileInfo("ClientConfig_StreamProviders.xml");
        private bool fireAndForgetDeliveryProperty;

        private static Options smsSiloOption = new Options
                    {
                        StartFreshOrleans = true,
                        SiloConfigFile = SiloConfig
                    };
        private static ClientOptions smsClientOptions = new ClientOptions
                    {
                        ClientConfigFile = ClientConfig
                    };
        private static Options smsSiloOption_OnlyPrimary = new Options
                    {
                        StartFreshOrleans = true,
                        SiloConfigFile = SiloConfig,
                        StartSecondary = false, 
                    };

        public SMSStreamingTests()
            : base(smsSiloOption, smsClientOptions)
        {
            runner = new SingleStreamTestRunner(SingleStreamTestRunner.SMS_STREAM_PROVIDER_NAME);
            fireAndForgetDeliveryProperty = ExtractFireAndForgetDeliveryProperty();
        }

        public SMSStreamingTests(int dummy)
            : base(smsSiloOption_OnlyPrimary, smsClientOptions)
        {
            runner = new SingleStreamTestRunner(SingleStreamTestRunner.SMS_STREAM_PROVIDER_NAME, 0, false);
            fireAndForgetDeliveryProperty = ExtractFireAndForgetDeliveryProperty();

        }

        // Use ClassInitialize to run code before running the first test in the class
        [ClassInitialize]
        public static void MyClassInitialize(TestContext testContext)
        {
            //ResetDefaultRuntimes();
        }

        // Use ClassCleanup to run code after all tests in a class have run
        [ClassCleanup]
        public static void MyClassCleanup()
        {
            ResetDefaultRuntimes();
        }

        #region Simple Message Stream Tests

        //------------------------ One to One ----------------------//

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Streaming")]
        public async Task SMS_01_OneProducerGrainOneConsumerGrain()
        {
            await runner.StreamTest_01_OneProducerGrainOneConsumerGrain();
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Streaming")]
        public async Task SMS_02_OneProducerGrainOneConsumerClient()
        {
            await runner.StreamTest_02_OneProducerGrainOneConsumerClient();
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Streaming")]
        public async Task SMS_03_OneProducerClientOneConsumerGrain()
        {
            await runner.StreamTest_03_OneProducerClientOneConsumerGrain();
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Streaming")]
        public async Task SMS_04_OneProducerClientOneConsumerClient()
        {
            await runner.StreamTest_04_OneProducerClientOneConsumerClient();
        }

        //------------------------ MANY to Many different grains ----------------------//

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Streaming")]
        public async Task SMS_05_ManyDifferent_ManyProducerGrainsManyConsumerGrains()
        {
            await runner.StreamTest_05_ManyDifferent_ManyProducerGrainsManyConsumerGrains();
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Streaming")]
        public async Task SMS_06_ManyDifferent_ManyProducerGrainManyConsumerClients()
        {
            await runner.StreamTest_06_ManyDifferent_ManyProducerGrainManyConsumerClients();
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Streaming")]
        public async Task SMS_07_ManyDifferent_ManyProducerClientsManyConsumerGrains()
        {
            await runner.StreamTest_07_ManyDifferent_ManyProducerClientsManyConsumerGrains();
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Streaming")]
        public async Task SMS_08_ManyDifferent_ManyProducerClientsManyConsumerClients()
        {
            await runner.StreamTest_08_ManyDifferent_ManyProducerClientsManyConsumerClients();
        }

        //------------------------ MANY to Many Same grains ----------------------//
        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Streaming")]
        public async Task SMS_09_ManySame_ManyProducerGrainsManyConsumerGrains()
        {
            await runner.StreamTest_09_ManySame_ManyProducerGrainsManyConsumerGrains();
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Streaming")]
        public async Task SMS_10_ManySame_ManyConsumerGrainsManyProducerGrains()
        {
            await runner.StreamTest_10_ManySame_ManyConsumerGrainsManyProducerGrains();
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Streaming")]
        public async Task SMS_11_ManySame_ManyProducerGrainsManyConsumerClients()
        {
            await runner.StreamTest_11_ManySame_ManyProducerGrainsManyConsumerClients();
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Streaming")]
        public async Task SMS_12_ManySame_ManyProducerClientsManyConsumerGrains()
        {
            await runner.StreamTest_12_ManySame_ManyProducerClientsManyConsumerGrains();
        }

        //------------------------ MANY to Many producer consumer same grain ----------------------//

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Streaming")]
        public async Task SMS_13_SameGrain_ConsumerFirstProducerLater()
        {
            await runner.StreamTest_13_SameGrain_ConsumerFirstProducerLater(!fireAndForgetDeliveryProperty);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Streaming")]
        public async Task SMS_14_SameGrain_ProducerFirstConsumerLater()
        {
            await runner.StreamTest_14_SameGrain_ProducerFirstConsumerLater(!fireAndForgetDeliveryProperty);
        }

        private bool ExtractFireAndForgetDeliveryProperty()
        {
            OrleansConfiguration orleansConfig = new OrleansConfiguration();
            orleansConfig.LoadFromFile(SiloConfig.FullName);
            ProviderCategoryConfiguration providerConfigs = orleansConfig.Globals.ProviderConfigurations["Stream"];
            IProviderConfiguration provider = providerConfigs.Providers[SingleStreamTestRunner.SMS_STREAM_PROVIDER_NAME];

            string fireAndForgetProperty = null;
            bool fireAndForget = SimpleMessageStreamProvider.DEFAULT_FIRE_AND_FORGET_DELIVERY_VALUE;
            if (provider.Properties.TryGetValue(SimpleMessageStreamProvider.FIRE_AND_FORGET_DELIVERY, out fireAndForgetProperty))
            {
                fireAndForget = Boolean.Parse(fireAndForgetProperty);
            }
            return fireAndForget;
        }

        //----------------------------------------------//

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Streaming")]
        public async Task SMS_15_ConsumeAtProducersRequest()
        {
            await runner.StreamTest_15_ConsumeAtProducersRequest();
        }

        //[TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Streaming")]
        public async Task SMS_16_Deactivation_OneProducerGrainOneConsumerGrain()
        {
            await runner.StreamTest_16_Deactivation_OneProducerGrainOneConsumerGrain();
        }

        //public async Task SMS_17_Persistence_OneProducerGrainOneConsumerGrain()
        //{
        //    await runner.StreamTest_17_Persistence_OneProducerGrainOneConsumerGrain();
        //}

        #endregion Simple Message Stream Tests
    }
}

#endif
