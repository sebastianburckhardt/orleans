#if !DISABLE_STREAMS

using System;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;

using Orleans.Streams;
using Orleans.AzureUtils;
using UnitTests.StorageTests;
using Orleans.Providers.Streams.Persistent.AzureQueueAdapter;

namespace UnitTests.SampleStreaming
{
    [DeploymentItem("Config_StreamProviders.xml")]
    [DeploymentItem("OrleansProviderInterfaces.dll")]
    [DeploymentItem("OrleansProviders.dll")]
    [TestClass]
    public class SampleStreamingTests : UnitTestBase
    {
        public static readonly string SMS_STREAM_PROVIDER_NAME = "SMSProvider";
        public static readonly string AZURE_QUEUE_STREAM_PROVIDER_NAME = "AzureQueueProvider";
        private static readonly TimeSpan _timeout = TimeSpan.FromSeconds(30);

        private StreamId streamId;
        private string streamProvider;

        public SampleStreamingTests()
            : base(new Options
            {
                StartFreshOrleans = true,
                SiloConfigFile = new FileInfo("Config_StreamProviders.xml"),
                PickNewDeploymentId = true
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

        [TestCleanup]
        public void TestCleanup()
        {
            if (streamProvider != null &&  streamProvider.Equals(AZURE_QUEUE_STREAM_PROVIDER_NAME))
            {
                DeleteAllAzureQueues((new AzureQueueStreamQueueMapper()).GetAllQueues(), UnitTestBase.DeploymentId, StandaloneAzureQueueTests.DataConnectionString);
            }
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Streaming")]
        public async Task SampleStreamingTests_1()
        {
            logger.Info("\n\n************************ SampleStreamingTests_1 ********************************* \n\n");
            streamId = StreamId.NewRandomStreamId();
            streamProvider = SMS_STREAM_PROVIDER_NAME;
            await StreamingTests_Consumer_Producer(streamId, streamProvider);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Streaming")]
        public async Task SampleStreamingTests_2()
        {
            logger.Info("\n\n************************ SampleStreamingTests_2 ********************************* \n\n");
            streamId = StreamId.NewRandomStreamId();
            streamProvider = SMS_STREAM_PROVIDER_NAME;
            await StreamingTests_Producer_Consumer(streamId, streamProvider);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Streaming")]
        public async Task SampleStreamingTests_3()
        {
            logger.Info("\n\n************************ SampleStreamingTests_3 ********************************* \n\n");
            streamId = StreamId.NewRandomStreamId();
            streamProvider = AZURE_QUEUE_STREAM_PROVIDER_NAME;
            await StreamingTests_Consumer_Producer(streamId, streamProvider);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Streaming")]
        public async Task SampleStreamingTests_4()
        {
            logger.Info("\n\n************************ SampleStreamingTests_4 ********************************* \n\n");
            streamId = StreamId.NewRandomStreamId();
            streamProvider = AZURE_QUEUE_STREAM_PROVIDER_NAME;
            await StreamingTests_Producer_Consumer(streamId, streamProvider);
        }

        private async Task StreamingTests_Consumer_Producer(StreamId streamId, string streamProvider)
        {
            // consumer joins first, producer later
            ISampleStreaming_ConsumerGrain consumer = SampleStreaming_ConsumerGrainFactory.GetGrain(Guid.NewGuid());
            consumer.BecomeConsumer(streamId, streamProvider).Wait();

            ISampleStreaming_ProducerGrain producer = SampleStreaming_ProducerGrainFactory.GetGrain(Guid.NewGuid());
            producer.BecomeProducer(streamId, streamProvider).Wait();

            producer.StartPeriodicProducing().Wait();

            Thread.Sleep(1000);

            producer.StopPeriodicProducing().Wait();

            await UnitTestBase.WaitUntilAsync(() => CheckCounters(producer, consumer, assertAreEqual: false), _timeout);
            await CheckCounters(producer, consumer);

            consumer.StopConsuming().Wait();
            //await consumer.StopConsuming();
        }

        private async Task<bool> CheckCounters(ISampleStreaming_ProducerGrain producer, ISampleStreaming_ConsumerGrain consumer, bool assertAreEqual = true)
        {
            var numProduced = await producer.NumberProduced;
            var numConsumed = await consumer.NumberConsumed;
            logger.Info("CheckCounters: numProduced = {0}, numConsumed = {1}", numProduced, numConsumed);
            if (assertAreEqual)
            {
                Assert.AreEqual(numProduced, numConsumed, String.Format("numProduced = {0}, numConsumed = {1}", numProduced, numConsumed));
                return true;
            }
            else
            {
                return numProduced == numConsumed;
            }
        }

        private async Task StreamingTests_Producer_Consumer(StreamId streamId, string streamProvider)
        {
            // producer joins first, consumer later
            ISampleStreaming_ProducerGrain producer = SampleStreaming_ProducerGrainFactory.GetGrain(Guid.NewGuid());
            producer.BecomeProducer(streamId, streamProvider).Wait();

            ISampleStreaming_ConsumerGrain consumer = SampleStreaming_ConsumerGrainFactory.GetGrain(Guid.NewGuid());
            consumer.BecomeConsumer(streamId, streamProvider).Wait();

            producer.StartPeriodicProducing().Wait();

            Thread.Sleep(1000);

            producer.StopPeriodicProducing().Wait();
            //int numProduced = producer.NumberProduced.Result;

            await UnitTestBase.WaitUntilAsync(() => CheckCounters(producer, consumer, assertAreEqual: false), _timeout);
            await CheckCounters(producer, consumer);

            consumer.StopConsuming().Wait();
            //await consumer.StopConsuming();
        }
    }
}

#endif