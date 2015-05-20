#if !DISABLE_STREAMS 

using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Orleans.Streams
{
    //[Reentrant]
    //internal class QueueStreamRendezvousGrain : GrainBase, IQueueStreamRendezvousGrain
    //{
    //    private OrleansLogger                                               logger;
    //    private StreamQueueMapper                                           streamQueueMapper;
    //    private Dictionary<QueueId, IStreamProducerExtension>               queuesToProducers;                  
    //    private Dictionary<QueueId, StreamConsumerCollection>                        queuesToConsumers;                  

    //    public override Task ActivateAsync()
    //    {
    //        logger = base.GetLogger(this.GetType().Name + " " + base.RuntimeIdentity + "/" + base.IdentityString);
    //        InitQueueMaps();
    //        return TaskDone.Done;
    //    }

    //    // Return all queues for this range
    //    public Task<Dictionary<QueueId, StreamConsumerCollection>> UpdateQueueProducer(IRingRange producerRange, ProducerId producerId, IStreamProducerExtension streamProducer)
    //    {
    //        logger.Info("UpdateQueueProducer {0} with range {1}.", producerId.ToShortString(), producerRange);

    //        Dictionary<QueueId, StreamConsumerCollection> myQueues = new Dictionary<QueueId, StreamConsumerCollection>();
    //        foreach (QueueId myQueue in streamQueueMapper.GetQueuesForRange(producerRange))
    //        {
    //            // update queuesToProducers
    //            queuesToProducers[myQueue] = streamProducer;
    //            // retreave StreamQueueData
    //            StreamConsumerCollection myStreams = queuesToConsumers[myQueue];
    //            myQueues.Add(myQueue, myStreams);
    //        }
    //        return Task.FromResult(myQueues);
    //    }

    //    public Task RegisterConsumer(StreamId streamId, ConsumerId consumerId, IStreamConsumerExtension streamConsumer)
    //    {
    //        logger.Info("RegisterConsumer {0} for stream {1}.", consumerId.ToShortString(), streamId);

    //        // update queuesToConsumers
    //        QueueId queueId = streamQueueMapper.GetQueueForStream(streamId);
    //        StreamConsumerCollection queueData = queuesToConsumers[queueId];
    //        queueData.AddConsumer(consumerId, streamId, streamConsumer);

    //        // notify producer
    //        IStreamProducerExtension producerGrain = queuesToProducers[queueId];
    //        return producerGrain.AddSubscriber(consumerId, streamId, streamConsumer);
    //    }

    //    public Task UnregisterConsumer(StreamId streamId, ConsumerId consumerId)
    //    {
    //        logger.Info("UnregisterConsumer {0} for stream {1}.", consumerId.ToShortString(), streamId);

    //        // update queuesToConsumers
    //        QueueId queueId = streamQueueMapper.GetQueueForStream(streamId);
    //        StreamConsumerCollection queueData = queuesToConsumers[queueId];
    //        queueData.RemoveConsumer(consumerId);

    //        // notify producer
    //        IStreamProducerExtension producerGrain = queuesToProducers[queueId];
    //        return producerGrain.RemoveSubscriber(consumerId, streamId);
    //    }

    //    public Task<int> ProducerCount(StreamId streamId)
    //    {
    //        //return Task.FromResult(0);
    //        throw new NotImplementedException("");
    //    }

    //    public Task<int> ConsumerCount(StreamId streamId)
    //    {
    //        //return Task.FromResult(0);
    //        throw new NotImplementedException("");
    //    }

    //    private void InitQueueMaps()
    //    {
    //        streamQueueMapper = new StreamQueueMapper();
    //        logger.Info("{0}", streamQueueMapper);
    //        queuesToProducers = new Dictionary<QueueId, IStreamProducerExtension>();
    //        queuesToConsumers = new Dictionary<QueueId, StreamConsumerCollection>();
    //        foreach (QueueId queueId in streamQueueMapper.GetAllQueues())
    //        {
    //            queuesToProducers.Add(queueId, null);
    //            queuesToConsumers.Add(queueId, new StreamConsumerCollection());
    //        }
    //    }
    //}
}

#endif
