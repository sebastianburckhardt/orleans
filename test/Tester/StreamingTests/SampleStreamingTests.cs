using System;
using System.IO;
using System.Threading.Tasks;
using Assert = Microsoft.VisualStudio.TestTools.UnitTesting.Assert;
using Xunit;
using Orleans;
using Orleans.Providers.Streams.AzureQueue;
using Orleans.TestingHost;
using UnitTests.GrainInterfaces;
using Tester;
using UnitTests.Tester;

namespace UnitTests.StreamingTests
{
    public class SampleStreamingTestsFixture : BaseClusterFixture
    {
        public SampleStreamingTestsFixture()
            : base(new TestingSiloHost(
                new TestingSiloOptions
                {
                    SiloConfigFile = new FileInfo("OrleansConfigurationForStreamingUnitTests.xml"),
                },
                new TestingClientOptions()
                {
                    ClientConfigFile = new FileInfo("ClientConfigurationForStreamTesting.xml")
                }))
        {
        }        
    }

    public class SampleStreamingTests : OrleansTestingBase, IClassFixture<SampleStreamingTestsFixture>, IDisposable
    {
        private const string StreamNamespace = "SampleStreamNamespace"; 
        private static readonly TimeSpan _timeout = TimeSpan.FromSeconds(30);

        private Guid streamId;
        private string streamProvider;
        
        private string deploymentId;

        public SampleStreamingTests(SampleStreamingTestsFixture fixture)
        {
            deploymentId = fixture.HostedCluster.DeploymentId;
        }
        
        public void Dispose()
        {
            if (streamProvider != null && streamProvider.Equals(StreamTestsConstants.AZURE_QUEUE_STREAM_PROVIDER_NAME))
            {
                AzureQueueStreamProviderUtils.DeleteAllUsedAzureQueues(StreamTestsConstants.AZURE_QUEUE_STREAM_PROVIDER_NAME, deploymentId, StorageTestConstants.DataConnectionString, logger).Wait();
            }
        }

        [Fact, TestCategory("BVT"), TestCategory("Functional"), TestCategory("Streaming")]
        public async Task SampleStreamingTests_1()
        {
            logger.Info("************************ SampleStreamingTests_1 *********************************");
            streamId = Guid.NewGuid();
            streamProvider = StreamTestsConstants.SMS_STREAM_PROVIDER_NAME;
            await StreamingTests_Consumer_Producer(streamId, streamProvider);
        }

        [Fact, TestCategory("Functional"), TestCategory("Streaming")]
        public async Task SampleStreamingTests_2()
        {
            logger.Info("************************ SampleStreamingTests_2 *********************************");
            streamId = Guid.NewGuid();
            streamProvider = StreamTestsConstants.SMS_STREAM_PROVIDER_NAME;
            await StreamingTests_Producer_Consumer(streamId, streamProvider);
        }

        [Fact, TestCategory("Functional"), TestCategory("Streaming" )]
        public async Task SampleStreamingTests_3()
        {
            logger.Info("************************ SampleStreamingTests_3 *********************************" );
            streamId = Guid.NewGuid();
            streamProvider = StreamTestsConstants.SMS_STREAM_PROVIDER_NAME;
            await StreamingTests_Producer_InlineConsumer(streamId, streamProvider );
        }

        [Fact, TestCategory("Functional"), TestCategory("Streaming")]
        public async Task SampleStreamingTests_4()
        {
            logger.Info("************************ SampleStreamingTests_4 *********************************");
            streamId = Guid.NewGuid();
            streamProvider = StreamTestsConstants.AZURE_QUEUE_STREAM_PROVIDER_NAME;
            await StreamingTests_Consumer_Producer(streamId, streamProvider);
        }

        [Fact, TestCategory("Functional"), TestCategory("Streaming")]
        public async Task SampleStreamingTests_5()
        {
            logger.Info("************************ SampleStreamingTests_5 *********************************");
            streamId = Guid.NewGuid();
            streamProvider = StreamTestsConstants.AZURE_QUEUE_STREAM_PROVIDER_NAME;
            await StreamingTests_Producer_Consumer(streamId, streamProvider);
        }

        [Fact, TestCategory("Functional"), TestCategory("Streaming")]
        public async Task MultipleImplicitSubscriptionTest()
        {
            logger.Info("************************ MultipleImplicitSubscriptionTest *********************************");
            streamId = Guid.NewGuid();
            const int nRedEvents = 5, nBlueEvents = 3;

            var provider = GrainClient.GetStreamProvider(StreamTestsConstants.SMS_STREAM_PROVIDER_NAME);
            var redStream = provider.GetStream<int>(streamId, "red");
            var blueStream = provider.GetStream<int>(streamId, "blue");

            for (int i = 0; i < nRedEvents; i++)
                await redStream.OnNextAsync(i);
            for (int i = 0; i < nBlueEvents; i++)
                await blueStream.OnNextAsync(i);

            var grain = GrainClient.GrainFactory.GetGrain<IMultipleImplicitSubscriptionGrain>(streamId);
            var counters = await grain.GetCounters();

            Assert.AreEqual(nRedEvents, counters.Item1);
            Assert.AreEqual(nBlueEvents, counters.Item2);
        }


        private async Task StreamingTests_Consumer_Producer(Guid streamId, string streamProvider)
        {
            // consumer joins first, producer later
            var consumer = GrainClient.GrainFactory.GetGrain<ISampleStreaming_ConsumerGrain>(Guid.NewGuid());
            await consumer.BecomeConsumer(streamId, StreamNamespace, streamProvider);

            var producer = GrainClient.GrainFactory.GetGrain<ISampleStreaming_ProducerGrain>(Guid.NewGuid());
            await producer.BecomeProducer(streamId, StreamNamespace, streamProvider);

            await producer.StartPeriodicProducing();

            await Task.Delay(TimeSpan.FromMilliseconds(1000));

            await producer.StopPeriodicProducing();

            await TestingUtils.WaitUntilAsync(lastTry => CheckCounters(producer, consumer, lastTry), _timeout);

            await consumer.StopConsuming();
        }

        private async Task StreamingTests_Producer_Consumer(Guid streamId, string streamProvider)
        {
            // producer joins first, consumer later
            var producer = GrainClient.GrainFactory.GetGrain<ISampleStreaming_ProducerGrain>(Guid.NewGuid());
            await producer.BecomeProducer(streamId, StreamNamespace, streamProvider);

            var consumer = GrainClient.GrainFactory.GetGrain<ISampleStreaming_ConsumerGrain>(Guid.NewGuid());
            await consumer.BecomeConsumer(streamId, StreamNamespace, streamProvider);

            await producer.StartPeriodicProducing();

            await Task.Delay(TimeSpan.FromMilliseconds(1000));

            await producer.StopPeriodicProducing();
            //int numProduced = await producer.NumberProduced;

            await TestingUtils.WaitUntilAsync(lastTry => CheckCounters(producer, consumer, lastTry), _timeout);

            await consumer.StopConsuming();
        }

        private async Task StreamingTests_Producer_InlineConsumer(Guid streamId, string streamProvider)
        {
            // producer joins first, consumer later
            var producer = GrainClient.GrainFactory.GetGrain<ISampleStreaming_ProducerGrain>(Guid.NewGuid());
            await producer.BecomeProducer(streamId, StreamNamespace, streamProvider);

            var consumer = GrainClient.GrainFactory.GetGrain<ISampleStreaming_InlineConsumerGrain>(Guid.NewGuid());
            await consumer.BecomeConsumer(streamId, StreamNamespace, streamProvider);

            await producer.StartPeriodicProducing();

            await Task.Delay(TimeSpan.FromMilliseconds(1000));

            await producer.StopPeriodicProducing();
            //int numProduced = await producer.NumberProduced;

            await TestingUtils.WaitUntilAsync(lastTry => CheckCounters(producer, consumer, lastTry), _timeout);

            await consumer.StopConsuming();
        }

        private async Task<bool> CheckCounters(ISampleStreaming_ProducerGrain producer, ISampleStreaming_ConsumerGrain consumer, bool assertIsTrue)
        {
            var numProduced = await producer.GetNumberProduced();
            var numConsumed = await consumer.GetNumberConsumed();
            logger.Info("CheckCounters: numProduced = {0}, numConsumed = {1}", numProduced, numConsumed);
            if (assertIsTrue)
            {
                Assert.AreEqual(numProduced, numConsumed, String.Format("numProduced = {0}, numConsumed = {1}", numProduced, numConsumed));
                return true;
            }
            else
            {
                return numProduced == numConsumed;
            }
        }
    }
}
