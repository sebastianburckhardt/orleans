using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.StorageClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using Orleans.AzureUtils;


namespace UnitTests.StorageTests
{
    [TestClass]
    public class StandaloneAzureQueueTests
    {
        private readonly Logger logger;
        public static string DeploymentId = "standaloneaqtests".ToLower();
        public static string DataConnectionString = "DefaultEndpointsProtocol=https;AccountName=orleanstestdata;AccountKey=qFJFT+YAikJPCE8V5yPlWZWBRGns4oti9tqG6/oYAYFGI4kFAnT91HeiWMa6pddUzDcG5OAmri/gk7owTOQZ+A==";
        private string queueName;

        public StandaloneAzureQueueTests()
        {
            ClientConfiguration config = new ClientConfiguration();
            config.TraceFilePattern = null;
            Logger.Initialize(config);
            logger = Logger.GetLogger("StandaloneAzureQueueTests", Logger.LoggerType.Application);
        }

        [TestCleanup]
        public void TestCleanup()
        {
            CleanupAfterTest();
        }

        private void CleanupAfterTest()
        {
            AzureQueueDataManager manager = GetTableManager(queueName).Result;
            manager.DeleteQueue().Wait();   
        }

        private async Task<AzureQueueDataManager> GetTableManager(string qName)
        {
            AzureQueueDataManager manager = new AzureQueueDataManager(qName, DeploymentId, DataConnectionString);
            await manager.InitQueue_Async();
            return manager;
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Persistence"), TestCategory("Azure"), TestCategory("Queue")]
        public async Task AQ_Standalone_1()
        {
            queueName = "Test-1-".ToLower() + Guid.NewGuid();
            AzureQueueDataManager manager = await GetTableManager(queueName);
            Assert.AreEqual(0, await manager.GetApproximateMessageCount());

            CloudQueueMessage inMessage = new CloudQueueMessage("Hello, World");
            await manager.AddQueueMessage(inMessage);
            //Nullable<int> count = manager.ApproximateMessageCount;
            Assert.AreEqual(1, await manager.GetApproximateMessageCount());

            CloudQueueMessage outMessage1 = await manager.PeekQueueMessage();
            logger.Info("PeekQueueMessage 1: {0}", AzureStorageUtils.PrintCloudQueueMessage(outMessage1));
            Assert.AreEqual(inMessage.AsString, outMessage1.AsString);

            CloudQueueMessage outMessage2 = await manager.PeekQueueMessage();
            logger.Info("PeekQueueMessage 2: {0}", AzureStorageUtils.PrintCloudQueueMessage(outMessage2));
            Assert.AreEqual(inMessage.AsString, outMessage2.AsString);

            CloudQueueMessage outMessage3 = await manager.GetQueueMessage();
            logger.Info("GetQueueMessage 3: {0}", AzureStorageUtils.PrintCloudQueueMessage(outMessage3));
            Assert.AreEqual(inMessage.AsString, outMessage3.AsString);
            Assert.AreEqual(1, await manager.GetApproximateMessageCount());

            CloudQueueMessage outMessage4 = await manager.GetQueueMessage();
            Assert.IsNull(outMessage4);

            Assert.AreEqual(1, await manager.GetApproximateMessageCount());

            await manager.DeleteQueueMessage(outMessage3);
            Assert.AreEqual(0, await manager.GetApproximateMessageCount());
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Persistence"), TestCategory("Azure"), TestCategory("Queue")]
        public async Task AQ_Standalone_2()
        {
            queueName = "Test-2-".ToLower() + Guid.NewGuid();
            AzureQueueDataManager manager = await GetTableManager(queueName);

            IEnumerable<CloudQueueMessage> msgs = await manager.GetQueueMessages();
            Assert.IsTrue(msgs == null || msgs.Count() == 0);

            int numMsgs = 10;
            List<Task> promises = new List<Task>();
            for (int i = 0; i < numMsgs; i++)
            {
                promises.Add(manager.AddQueueMessage(new CloudQueueMessage(i.ToString())));
            }
            Task.WaitAll(promises.ToArray());
            Assert.AreEqual(numMsgs, await manager.GetApproximateMessageCount());

            msgs = new List<CloudQueueMessage>(await manager.GetQueueMessages(numMsgs));
            Assert.AreEqual(numMsgs, msgs.Count());
            Assert.AreEqual(numMsgs, await manager.GetApproximateMessageCount());

            promises = new List<Task>();
            foreach (var msg in msgs)
            {
                promises.Add(manager.DeleteQueueMessage(msg));
            }
            Task.WaitAll(promises.ToArray());
            Assert.AreEqual(0, await manager.GetApproximateMessageCount());
        }

        //[TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Persistence"), TestCategory("Azure"), TestCategory("Queue")]
        //public async Task AQ_Standalone_3_CreateDelete()
        //{
        //    queueName = "Test-3-".ToLower() + Guid.NewGuid();
        //    AzureQueueDataManager manager = new AzureQueueDataManager(queueName, DeploymentId, DataConnectionString);
        //    await manager.InitQueue_Async();
        //    await manager.DeleteQueue();

        //    AzureQueueDataManager manager2 = new AzureQueueDataManager(queueName, DeploymentId, DataConnectionString);
        //    await manager2.InitQueue_Async();
        //    await manager2.DeleteQueue();
            
        //    AzureQueueDataManager manager3 = await GetTableManager(queueName);
        //    await manager3.DeleteQueue();
        //    await manager3.DeleteQueue();
        //    await manager3.DeleteQueue();
        //}

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Persistence"), TestCategory("Azure"), TestCategory("Queue"), TestCategory("Stress"),]
        public async Task AQ_Standalone_4_Init_MultipleThreads()
        {
            queueName = "Test-4-".ToLower() + Guid.NewGuid();

            const int NumThreads = 100;
            Task[] promises = new Task[NumThreads];

            for (int i = 0; i < NumThreads; i++) 
            {
                promises[i] = Task.Run(() =>
                {
                    AzureQueueDataManager manager = new AzureQueueDataManager(queueName, DeploymentId, DataConnectionString);
                    return manager.InitQueue_Async();
                });
            }
            await Task.WhenAll(promises);
        }
    }
}
