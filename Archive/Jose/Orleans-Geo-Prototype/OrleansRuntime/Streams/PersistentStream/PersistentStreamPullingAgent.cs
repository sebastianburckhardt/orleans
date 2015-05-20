#if !DISABLE_STREAMS
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Orleans.Runtime;

namespace Orleans.Streams
{
    internal class QueueTimerData
    {
        public QueueId                  QueueId     { get; private set; }
        public IQueueAdapterReceiver    Receiver    { get; private set; }
        public int                      NumMessages;
        private HashSet<StreamId>       streams;
        private IOrleansTimer           timer;

        public QueueTimerData(QueueId queueId, IQueueAdapterReceiver reciever)
        {
            this.QueueId = queueId;
            this.Receiver = reciever;
            this.NumMessages = 0;
            this.streams = new HashSet<StreamId>();
        }

        public void SetTimer(IOrleansTimer tmr)
        {
            timer = tmr;
        }

        public void StopTimer()
        {
            if (timer != null)
            {
                IOrleansTimer tmp = timer;
                timer = null;
                tmp.Dispose();
            }
        }

        public bool AddStream(StreamId streamId)
        {
            return streams.Add(streamId);
        }

        public bool ContainsStream(StreamId streamId)
        {
            return streams.Contains(streamId);
        }

        public IEnumerable<StreamId> GetAllStreams()
        {
            return streams;
        }
    }

    internal class PersistentStreamPullingAgent : SystemTarget, IPersistentStreamPullingAgent, IGrainRingRangeListener
    {
        private readonly Dictionary<QueueId, QueueTimerData>            queuesDataMap;
        private readonly Dictionary<StreamId, StreamConsumerCollection> pubSubCache;
        private IStreamQueueMapper                                      streamQueueMapper;

        private readonly IStreamProviderRuntime                         providerRuntime;
        private IStreamPubSub                                           pubSub;
        private IConsistentRingProviderForGrains                        consistentRingProvider;
        private IQueueAdapter                                           queueAdapter;

        private IRingRange                                              myRange;
        private readonly OrleansLogger                                  logger;
        public const int QUEUE_GET_PERIOD_MS                            = 100; // TODO: Get from config?
        private static readonly TimeSpan QUEUE_GET_PERIOD               = TimeSpan.FromMilliseconds(QUEUE_GET_PERIOD_MS);

        internal PersistentStreamPullingAgent(GrainId id, IStreamProviderRuntime runtime)
            : base(id, runtime.ExecutingSiloAddress)
        {
            if (runtime == null) throw new ArgumentNullException("runtime", "PersistentStreamPullingAgent: runtime reference should not be null");
            this.providerRuntime = runtime;
            this.queuesDataMap = new Dictionary<QueueId, QueueTimerData>();
            this.pubSubCache = new Dictionary<StreamId, StreamConsumerCollection>();
            this.logger = providerRuntime.GetLogger(this.GetType().Name + "-" + base.CurrentSilo, Logger.LoggerType.Runtime);
            Log("Created PersistentStreamPullingAgent on silo {0}", base.CurrentSilo);
        }

        public Task Init(Immutable<IQueueAdapter> qAdapter)
        {
            if (qAdapter.Value == null) throw new ArgumentNullException("qAdapter", "Init: queueAdapter should not be null");

            logger.Info("Init of PersistentStreamPullingAgent.");
            this.pubSub = providerRuntime.PubSub(StreamPubSubType.GRAINBASED);
            this.consistentRingProvider = providerRuntime.ConsistentRingProvider;

            this.queueAdapter = qAdapter.Value;
            this.streamQueueMapper = queueAdapter.GetStreamQueueMapper();

            IGrainRingRangeListener meAsRingRangeListener = GrainRingRangeListenerFactory.Cast(this.AsReference());
            consistentRingProvider.SubscribeToRangeChangeEvents(meAsRingRangeListener);
            this.myRange = consistentRingProvider.GetMyRange();

            List<QueueId> myQueues = streamQueueMapper.GetQueuesForRange(myRange).ToList();
            Log("Got my range {0} from RingProvider. I am now responsible for {1} queues: {2}.",
                myRange, myQueues.Count, OrleansUtils.IEnumerableToString(myQueues, q => q.ToStringWithHashCode()));

            return InitQueues(myQueues);
        }

        private async Task InitQueues(IEnumerable<QueueId> myQueues)
        {
            // get receivers for queues in range that we dont yet have.
            var receiverTasks = new List<Task<IQueueAdapterReceiver>>();
            foreach (QueueId queueId in myQueues.Where(queueId => !queuesDataMap.ContainsKey(queueId)))
            {
                receiverTasks.Add(this.queueAdapter.CreateReceiver(queueId));
            }
            IQueueAdapterReceiver[] receivers = await Task.WhenAll(receiverTasks);

            Log("Taking {0} queues under my responsibility: {1}", receivers.Length, OrleansUtils.IEnumerableToString(receivers, receiver => receiver.Id.ToStringWithHashCode()));
            // setup readers for new receivers
            foreach(IQueueAdapterReceiver receiver in receivers)
            {
                QueueTimerData queueTimerData = new QueueTimerData(receiver.Id, receiver);
                queuesDataMap.Add(receiver.Id, queueTimerData);

                IDisposable timer = providerRuntime.RegisterTimer(AsyncTimerCallback, receiver.Id, TimeSpan.Zero, QUEUE_GET_PERIOD);
                queueTimerData.SetTimer((IOrleansTimer)timer);
            }
        }

        #region Change in membership, e.g., failure of predecessor
        /// <summary>
        /// Actions to take when the range of this silo changes on the ring due to a failure or a join
        /// </summary>
        /// <param name="old">my previous responsibility range</param>
        /// <param name="now">my new/current responsibility range</param>
        /// <param name="increased">True: my responsibility increased, false otherwise</param>
        public async Task RangeChangeNotification(IRingRange old, IRingRange now, bool increased)
        {
            myRange = now;

            List<QueueId> myQueues = streamQueueMapper.GetQueuesForRange(myRange).ToList();
            Log("Got RangeChangeNotification from RingProvider. Old range: {0} New range: {1}{2}. I am now responsible for queues {3}: {4}",
                old, now, (increased ? ", increased" : ", decreased"), myQueues.Count, OrleansUtils.IEnumerableToString(myQueues, q => q.ToStringWithHashCode()));

            if (increased)
            {
                await InitQueues(myQueues);
            }
           
            // stop pullig from queues that are not in my range anymore.
            List<QueueId> queuesToRemove = queuesDataMap.Keys.Where((QueueId queueId) => !myRange.InRange(queueId.GetUniformHashCode())).ToList();
            Log("Removing {0} queues from my responsibility: {1}", queuesToRemove.Count, OrleansUtils.IEnumerableToString(queuesToRemove, q => q.ToStringWithHashCode()));
            foreach (QueueId queueId in queuesToRemove)
            {
                QueueTimerData queueTimerData;
                if (queuesDataMap.TryGetValue(queueId, out queueTimerData))
                {
                    queuesDataMap.Remove(queueId);
                    queueTimerData.StopTimer();
                }
                IStreamProducerExtension meAsStreamProducer = StreamProducerExtensionFactory.Cast(this.AsReference());
                foreach (StreamId streamId in queueTimerData.GetAllStreams())
                {
                    Log("UnregisterProducer for stream {0}.", streamId);
                    await pubSub.UnregisterProducer(streamId, meAsStreamProducer);
                }
            }
        }

        #endregion

        public Task AddSubscriber(StreamId streamId, IStreamConsumerExtension streamConsumer, StreamSequenceToken token)
        {
            Log("AddSubscriber: Stream={0} Subscriber={1} Token={2}.", streamId, streamConsumer, token);
            AddSubscriber_Impl(streamId, streamConsumer, token);
            return TaskDone.Done;
        }

        // Called by rendezvous when new remote subscriber subscribes to this stream.
        private void AddSubscriber_Impl(StreamId streamId, IStreamConsumerExtension streamConsumer, StreamSequenceToken token)
        {
            StreamConsumerCollection streamDataCollection;
            if (!pubSubCache.TryGetValue(streamId, out streamDataCollection))
            {
                streamDataCollection = new StreamConsumerCollection();
                pubSubCache.Add(streamId, streamDataCollection);
            }
            StreamConsumerData data;
            if (!streamDataCollection.TryGetConsumer(streamConsumer, out data))
            {
                streamDataCollection.AddConsumer(streamId, streamConsumer, token);
            }
            else
            {
                data.Token = token;
            }
            //Log("Added Subscriber={0} for Stream {1}.", streamConsumer, streamId);

            if (queueAdapter.IsRewindable)
            {
                UpdateQueueReceiverAboutNewSequenceToken(streamId, token);
            }
        }

        // This is a first crude implemenation of suporting going back in time.
        // Just rewind the queue receiver back to re-read the queue upon new subsription with new token request.
        // Smarter impl. will check the current position of the receiver, create a new cold receiver, use in-memory cache to catch up, ...
        private void UpdateQueueReceiverAboutNewSequenceToken(StreamId streamId, StreamSequenceToken token)
        {
            foreach (QueueTimerData queueTimerData in queuesDataMap.Values)
            {
                if (queueTimerData.ContainsStream(streamId))
                {
                    queueTimerData.Receiver.Rewind(token);
                }
            }
        }

        public Task RemoveSubscriber(StreamId streamId, IStreamConsumerExtension streamConsumer)
        {
            StreamConsumerCollection streamData;
            if (!pubSubCache.TryGetValue(streamId, out streamData))
            {
                return TaskDone.Done;
            }
            // remove consumer
            bool removed = streamData.RemoveConsumer(streamConsumer);
            if (removed)
            {
                Log("Removed Consumer: subscriber={0}, for stream {1}.", streamConsumer, streamId);
            }
            return TaskDone.Done;
        }

        private async Task AsyncTimerCallback(object state)
        {
            try
            {
                var myQueueId = (QueueId)(state);
                QueueTimerData queueTimerData;
                if (!queuesDataMap.TryGetValue(myQueueId, out queueTimerData))
                {
                    return; // timer was already removed, last tick
                }
                IQueueAdapterReceiver receiver = queueTimerData.Receiver;

                // loop through the queue until it is empty.
                while (true)
                {
                    // Retrive one multiBatch from the queue. Every multiBatch has an IEnumerable of IBatchContainers, each IBatchContainer may have multiple events.
                    IEnumerable<IBatchContainer> msgsEnumerable = await receiver.GetQueueMessagesAsync();
                    List<IBatchContainer> multiBatch = null;
                    if (msgsEnumerable != null)
                    {
                        multiBatch = msgsEnumerable.ToList();
                    }
                    if (multiBatch == null || multiBatch.Count == 0)
                    {
                        return;     // queue is empty. Exit the loop. Will attempt again in the next timer callback.
                    }
                    queueTimerData.NumMessages += multiBatch.Count;
                    Log("Got {0} messages from queue {1}. So far {2} msgs from this queue, total {3} from all queues.",
                        multiBatch.Count,
                        myQueueId.ToStringWithHashCode(), 
                        queueTimerData.NumMessages,
                        queuesDataMap.Sum(kv => kv.Value.NumMessages));

                    List<Task> perStreamTasks = new List<Task>();
                    // The multiBatch may include multiple msgs for different streams.
                    // We group them by streamId and send all msgs to the same stream in one DeliverOneStreamBatch operation.
                    // This preserves the order between msgs to the same stream, while msgs to different streams go in parallel.
                    foreach (var group in multiBatch.Where(m => m != null).GroupBy(batch => batch.StreamId))
                    {
                        StreamId streamId = group.Key;
                        IEnumerable<IBatchContainer> streamBatch = group;
                        Task task = Task.Factory.StartNew(() =>
                            {
                                return DeliverOneStreamBatch(streamId, streamBatch, queueTimerData);
                            }).Unwrap();
                        perStreamTasks.Add(task);
                    }

                    // We await all msgs for one multiBatch before retrieving the next multiBatch from the queue via GetQueueMessagesAsync.
                    // This serializes the multiBatches.
                    // As next step we should allow to parallelize the multiBatch, while preserving per-consumer order.
                    await Task.WhenAll(perStreamTasks);

                    // notifiy receiver that messages were delivered.
                    await receiver.OnDeliveryComplete(multiBatch);
                }
            }
            catch (Exception exc)
            {
                logger.Error((int)ErrorCode.AzureQueueStream_Agent_05, 
                    String.Format("Exception while PersistentStreamPullingAgentGrain.AsyncTimerCallback"), exc);
            }
        }

        private async Task DeliverOneStreamBatch(StreamId streamId, IEnumerable<IBatchContainer> msgs, QueueTimerData queueTimerData)
        {
            try
            {
                StreamConsumerCollection streamData;
                if (!pubSubCache.TryGetValue(streamId, out streamData))
                {
                    queueTimerData.AddStream(streamId);
                    streamData = new StreamConsumerCollection();
                    pubSubCache.Add(streamId, streamData);
                    await RetreaveNewStreamFromPubSub(streamId);
                }

                List<Task> perConsumerTasks = new List<Task>();
                var streamConsumers = streamData.AllConsumers().Where(consumer => consumer.StreamId.Equals(streamId)).ToList();
                List<IBatchContainer> batchContainers = msgs.ToList();
#if DEBUG
                int i = 1;
                int numStreamConsumers = streamConsumers.Count;
                int numBatchContainers = batchContainers.Count;
#endif
                foreach (StreamConsumerData consumerData in streamConsumers)
                {
#if DEBUG
                    if (logger.IsVerbose) logger.Verbose("Sending msg from queue {0} stream {1} to {2}/{3} consumer {4} batch size {5}.",
                         queueTimerData.QueueId.ToStringWithHashCode(), streamId,
                         i++, numStreamConsumers,
                         consumerData.StreamConsumer,
                         numBatchContainers);
#endif
                    foreach (IBatchContainer msg in batchContainers)
                    {
                        Task task = consumerData.StreamConsumer.DeliverItem(streamId, msg, msg.Token);
                        perConsumerTasks.Add(task);
                    }
                }
                await Task.WhenAll(perConsumerTasks);
            }
            catch (Exception exc)
            {
                logger.Error((int)ErrorCode.AzureQueueStream_Agent_04,
                       String.Format("Exception while trying to deliver msgs in PersistentStreamPullingAgentGrain.DeliverOneStreamBatch"), exc);
            }
        }

        private async Task RetreaveNewStreamFromPubSub(StreamId streamId)
        {
            if (pubSub == null) throw new NullReferenceException("Found pubSub reference not set up correctly in RetreaveNewStream");
            IStreamProducerExtension meAsStreamProducer = StreamProducerExtensionFactory.Cast(this.AsReference());
            //Log("About to Register myself as a Producer for stream {1}.", streamId);
            // register as producer in the PubSub: tell PubSub what my range is and retreave the list of currently subscribed consumers.
            ISet<PubSubSubscriptionState> streamData = await pubSub.RegisterProducer(streamId, meAsStreamProducer);

            Log("Got back {0} Subscribers for stream {1}.", streamData.Count, streamId);
            foreach (PubSubSubscriptionState item in streamData)
            {
                AddSubscriber_Impl(item.Stream, item.Consumer, item.StreamSequenceToken);
            }
        }

        private void Log(string str, params object[] args)
        {
            string fmt = string.Format("\n\n\n########### {0}\n\n", str);
            logger.Info(fmt, args);
        }
    }
}

#endif
