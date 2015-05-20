#if !DISABLE_STREAMS
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.StorageClient;
using Orleans.AzureUtils;
using Orleans.Streams;

namespace Orleans.Providers.Streams.Persistent.AzureQueueAdapter
{
    /// <summary>
    /// Recieves batches of messages from a single partition of a message queue.  
    /// </summary>
    public class AzureQueueAdapterReceiver : IQueueAdapterReceiver
    {
        private readonly AzureQueueDataManager _queue;

        public QueueId Id { get; private set; }

        public static async Task<IQueueAdapterReceiver> Create(QueueId queueId, string dataConnectionString, string deploymentId)
        {
            if (queueId == null)
            {
                throw new ArgumentNullException("queueId");
            }
            if (String.IsNullOrEmpty(dataConnectionString))
            {
                throw new ArgumentNullException("dataConnectionString");
            }
            if (String.IsNullOrEmpty(deploymentId))
            {
                throw new ArgumentNullException("deploymentId");
            }
            var queue = new AzureQueueDataManager(queueId.ToString(), deploymentId, dataConnectionString);
            await queue.InitQueue_Async();
            return new AzureQueueAdapterReceiver(queueId, queue);
        }

        private AzureQueueAdapterReceiver(QueueId queueId, AzureQueueDataManager queue)
        {
            if (queueId == null)
            {
                throw new ArgumentNullException("queueId");
            }
            if (queue == null)
            {
                throw new ArgumentNullException("queue");
            }
            Id = queueId;
            _queue = queue;
        }

        public async Task<IEnumerable<IBatchContainer>> GetQueueMessagesAsync()
        {
            IEnumerable<CloudQueueMessage> messages = await _queue.GetQueueMessages();
            if (messages == null )
            {
                return new BatchContainer[0];
            }
            return messages.Select((CloudQueueMessage msg) => BatchContainer.FromCloudQueueMessage(msg));
        }

        public Task OnDeliveryComplete(IEnumerable<IBatchContainer> msgs)
        {
            List<Task> tasks = new List<Task>();
            foreach (IBatchContainer msg in msgs)
            {
                tasks.Add(_queue.DeleteQueueMessage(((BatchContainer)msg).CloudQueueMessage));
            }
            return Task.WhenAll(tasks);
        }

        public Task Rewind(StreamSequenceToken token)
        {
            throw new NotSupportedException("AzureQueueAdapterReceiver is a non-rewindable queue receiver and does not support Rewind");
        }
    }
}
#endif
