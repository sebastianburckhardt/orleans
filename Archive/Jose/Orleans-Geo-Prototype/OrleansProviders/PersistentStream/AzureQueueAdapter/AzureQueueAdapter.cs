#if !DISABLE_STREAMS
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.StorageClient;
using Orleans.AzureUtils;
using Orleans.Serialization;
using Orleans.Streams;

namespace Orleans.Providers.Streams.Persistent.AzureQueueAdapter
{
    public class AzureQueueAdapter : IQueueAdapter
    {
        private readonly string _deploymentId;
        private readonly string _dataConnectionString;
        private readonly AzureQueueStreamQueueMapper _streamQueueMapper = new AzureQueueStreamQueueMapper();
        private readonly ConcurrentDictionary<QueueId, AzureQueueDataManager> _queues = new ConcurrentDictionary<QueueId, AzureQueueDataManager>();

        public string Name { get { return typeof(AzureQueueAdapter).Name; } }

        public bool IsRewindable { get { return false; } }

        public AzureQueueAdapter(string dataConnectionString, string deploymentId)
        {
            if (String.IsNullOrEmpty(dataConnectionString))
            {
                throw new ArgumentNullException("dataConnectionString");
            }
            if (String.IsNullOrEmpty(deploymentId))
            {
                throw new ArgumentNullException("deploymentId");
            }
            _dataConnectionString = dataConnectionString;
            _deploymentId = deploymentId;
        }

        public Task<IQueueAdapterReceiver> CreateReceiver(QueueId queueId)
        {
            return AzureQueueAdapterReceiver.Create(queueId, _dataConnectionString, _deploymentId);
        }

        public IStreamQueueMapper GetStreamQueueMapper()
        {
            return new AzureQueueStreamQueueMapper();
        }

        public Task QueueMessageAsync<T>(StreamId streamId, T item)
        {
            return QueueMessageBatchAsync(streamId, new T[] { item });
        }

        public async Task QueueMessageBatchAsync<T>(StreamId streamId, IEnumerable<T> events)
        {
            QueueId queueId = _streamQueueMapper.GetQueueForStream(streamId);
            AzureQueueDataManager queue;
            if (!_queues.TryGetValue(queueId, out queue))
            {
                AzureQueueDataManager tmpQueue = new AzureQueueDataManager(queueId.ToString(), _deploymentId, _dataConnectionString);
                await tmpQueue.InitQueue_Async();
                queue = _queues.GetOrAdd(queueId, tmpQueue);
            }
            CloudQueueMessage cloudMsg = BatchContainer.ToCloudQueueMessage(streamId, events);
            await queue.AddQueueMessage(cloudMsg);
        }
    }
}
#endif
