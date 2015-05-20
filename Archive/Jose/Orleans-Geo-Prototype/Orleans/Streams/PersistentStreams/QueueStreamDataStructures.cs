#if !DISABLE_STREAMS

using System;
using System.Collections.Generic;
using Orleans;

namespace Orleans.Streams
{
    [Serializable]
    internal class StreamConsumerData
    {
        public StreamId StreamId;
        public IStreamConsumerExtension StreamConsumer;
        public StreamSequenceToken Token;

        public StreamConsumerData(StreamId streamId, IStreamConsumerExtension streamConsumer, StreamSequenceToken token)
        {
            this.StreamId = streamId;
            this.StreamConsumer = streamConsumer;
            this.Token = token;
        }
    }

    [Serializable]
    internal class StreamConsumerCollection
    {
        private readonly Dictionary<GrainReference, StreamConsumerData> queueData; // map of consumers for one queue: from Guid ConsumerId to StreamConsumerData

        public StreamConsumerCollection()
        {
            queueData = new Dictionary<GrainReference, StreamConsumerData>();
        }

        public void AddConsumer(StreamId streamId, IStreamConsumerExtension streamConsumer, StreamSequenceToken token)
        {
            var consumerData = new StreamConsumerData(streamId, streamConsumer, token);
            queueData.Add(streamConsumer.AsReference(), consumerData);
        }

        public bool RemoveConsumer(IAddressable consumer)
        {
            return queueData.Remove(consumer.AsReference());
        }

        public bool Contains(IAddressable consumer)
        {
            return queueData.ContainsKey(consumer.AsReference());
        }

        public bool TryGetConsumer(IAddressable consumer, out StreamConsumerData data)
        {
            return queueData.TryGetValue(consumer.AsReference(), out data);
        }

        public IEnumerable<StreamConsumerData> AllConsumers()
        {
            return queueData.Values;
        }

        public int Count()
        {
            return queueData.Count;
        }
    }
}

#endif