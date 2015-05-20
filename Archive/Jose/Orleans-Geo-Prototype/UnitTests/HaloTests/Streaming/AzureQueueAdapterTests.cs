#if !DISABLE_STREAMS

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using Orleans.AzureUtils;
using Orleans.Providers;
using Orleans.Providers.Streams.Persistent.AzureQueueAdapter;
using Orleans.Runtime;
using Orleans.Streams;

namespace UnitTests.StorageTests
{
    [TestClass]
    public class AzureQueueAdapterTests
    {
        private const int NumBatches = 100;
        private const int NumMessagesPerBatch = 100;
        private const int NumMessages = NumBatches * NumMessagesPerBatch;
        private string _deploymentId = MakeDeploymentId();

        private static readonly SafeRandom Random = new SafeRandom();

        [TestInitialize]
        public void InitializeForTesting()
        {
            Logger.Initialize(new NodeConfiguration());
            var loader = GrainAssemblyLoader.Instance;
            loader.ProcessLoadedAssemblies();
            BufferPool.InitGlobalBufferPool(new MessagingConfiguration(false));
        }

        [TestCleanup]
        public void TestCleanup()
        {
            UnitTestBase.DeleteAllAzureQueues((new AzureQueueStreamQueueMapper()).GetAllQueues(), _deploymentId, StandaloneAzureQueueTests.DataConnectionString, null);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Halo"), TestCategory("Azure"), TestCategory("Streaming")]
        public async Task Halo_SendAndReceiveFromAzureQueue()
        {
            _deploymentId = MakeDeploymentId();
            var properties = new Dictionary<string, string>
                {
                    {AzureQueueAdapterFactory.DATA_CONNECTION_STRING, StandaloneAzureQueueTests.DataConnectionString},
                    {AzureQueueAdapterFactory.DEPLOYMENT_ID, _deploymentId}
                };
            var config = new ProviderConfiguration(properties, "type", "name");
            await SendAndReceiveFromQueueAdapter(new AzureQueueAdapterFactory(), config);
        }

        private async Task SendAndReceiveFromQueueAdapter(IQueueAdapterFactory adapterFactory, IProviderConfiguration config)
        {
            IQueueAdapter adapter = await adapterFactory.Create(config);

            // Create receiver per queue
            var mapper = adapter.GetStreamQueueMapper();
            IEnumerable<Task<IQueueAdapterReceiver>> receiversTasks = mapper.GetAllQueues()
                .Select(adapter.CreateReceiver);
            IEnumerable<IQueueAdapterReceiver> receivers = await Task.WhenAll(receiversTasks);

            int receivedBatches = 0;

            var work = receivers.Select(receiver => Task.Factory.StartNew(() =>
            {
                while (receivedBatches != NumBatches)
                {
                    var batches = receiver.GetQueueMessagesAsync().Result.ToArray();
                    foreach (IBatchContainer batch in batches)
                    {
                        Console.WriteLine("Queue {0} received message on stream {1}", receiver.Id, batch.StreamId);
                        Assert.AreEqual(NumMessagesPerBatch / 2, batch.GetEvents<int>().Count(), "Half the events were ints");
                        Assert.AreEqual(NumMessagesPerBatch / 2, batch.GetEvents<string>().Count(), "Half the events were strings");
                    }
                    lock (typeof(AzureQueueAdapterTests))
                    {
                        receivedBatches += batches.Length;
                    }
                }
            })).ToList();

            // send events
            IEnumerable<object> events = CreateEvents(NumMessages);
            work.AddRange(Enumerable.Range(0, NumBatches)
                                    .Select(i => adapter.QueueMessageBatchAsync(StreamId.NewRandomStreamId(), events.Take(NumMessagesPerBatch).ToArray())));
            await Task.WhenAll(work);

            // Make sure we got back everything we sent
            Assert.AreEqual(NumBatches, receivedBatches);
        }

        private IEnumerable<object> CreateEvents(int count)
        {
            return Enumerable.Range(0, count).Select(i =>
            {
                if (i % 2 == 0)
                {
                    return Random.Next(int.MaxValue) as object;
                }
                else
                {
                    return Random.Next(int.MaxValue).ToString(CultureInfo.InvariantCulture);
                }
            });
        }

        private static string MakeDeploymentId()
        {
            const string DeploymentIdFormat = "azure-queue-adapter-test-{0}";
            string now = DateTime.UtcNow.ToString("yyyy-MM-dd-hh-mm-ss-ffff");
            return String.Format(DeploymentIdFormat, now);
        }
    }
}
#endif