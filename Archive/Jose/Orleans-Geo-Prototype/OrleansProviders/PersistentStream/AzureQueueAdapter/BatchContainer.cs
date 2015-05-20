#if !DISABLE_STREAMS
using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.WindowsAzure.StorageClient;
using Orleans.Serialization;
using Orleans.Streams;

namespace Orleans.Providers.Streams.Persistent.AzureQueueAdapter
{
    [Serializable]
    public class BatchContainer : IBatchContainer
    {
        public StreamId StreamId { get; private set; }

        public StreamSequenceToken Token { get; private set;  }

        private readonly List<object> _events;

        [NonSerialized]
        // Need to store reference to the original AQ CloudQueueMessage to be able to delete it later on.
        // Don't need to serialize it, since we are never interested in sending it to stream consumers.
        internal CloudQueueMessage CloudQueueMessage;

        private BatchContainer(StreamId streamId, List<object> events)
        {
            if (streamId == null)
            {
                throw new ArgumentNullException("streamId");
            }
            if (events == null)
            {
                throw new ArgumentNullException("events", "Message contains no events");
            }
            this.StreamId = streamId;
            this._events = events;
        }

        public IEnumerable<T> GetEvents<T>()
        {
            return _events.Where(e => e is T).Cast<T>();
        }

        internal static CloudQueueMessage ToCloudQueueMessage<T>(StreamId streamId, IEnumerable<T> events)
        {
            BatchContainer batchMessage = new BatchContainer(streamId, events.Cast<object>().ToList());
            byte[] rawBytes = SerializationManager.SerializeToByteArray(batchMessage);
            return new CloudQueueMessage(rawBytes);
        }

        internal static BatchContainer FromCloudQueueMessage(CloudQueueMessage cloudMsg)
        {
            BatchContainer batch = SerializationManager.DeserializeFromByteArray<BatchContainer>(cloudMsg.AsBytes);
            batch.CloudQueueMessage = cloudMsg;
            return batch;
        }

        public override string ToString()
        {
            return string.Format("BatchContainer:Stream={0},#Items={1}", StreamId, _events.Count);
        }
    }
}
#endif
