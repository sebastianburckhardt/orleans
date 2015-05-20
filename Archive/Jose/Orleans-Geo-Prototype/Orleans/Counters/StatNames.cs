using System.Collections.Generic;
using System.Diagnostics;
using System;

namespace Orleans.Counters
{
    internal class StatName
    {
        public string Name { get; private set; }

        public StatName(string name)
        {
            Name = name;
        }

        public StatName(StatName_Format nameFormat, params object[] args)
        {
            Name = String.Format(nameFormat.Name, args);
        }

        public override string ToString()
        {
            return Name;
        }
    }

    internal class StatName_Format : StatName
    {
        public StatName_Format(string name) : base(name)
        {
        }
    }

    internal class StatNames
    {
        // Networking
        public static readonly StatName STAT_NETWORKING_SOCKETS_SILO_SENDING_CLOSED         = new StatName("Networking.Sockets.Silo.Sending.Closed");
        public static readonly StatName STAT_NETWORKING_SOCKETS_SILO_SENDING_OPENED         = new StatName("Networking.Sockets.Silo.Sending.Opened");
        public static readonly StatName STAT_NETWORKING_SOCKETS_SILO_RECEIVING_CLOSED       = new StatName("Networking.Sockets.Silo.Receiving.Closed");
        public static readonly StatName STAT_NETWORKING_SOCKETS_SILO_RECEIVING_OPENED       = new StatName("Networking.Sockets.Silo.Receiving.Opened");
        public static readonly StatName STAT_NETWORKING_SOCKETS_GWTOCLIENT_DUPLEX_CLOSED    = new StatName("Networking.Sockets.GWToClient.Duplex.Closed");
        public static readonly StatName STAT_NETWORKING_SOCKETS_GWTOCLIENT_DUPLEX_OPENED    = new StatName("Networking.Sockets.GWToClient.Duplex.Opened");
        public static readonly StatName STAT_NETWORKING_SOCKETS_CLIENTTOGW_DUPLEX_CLOSED    = new StatName("Networking.Sockets.ClientToGW.Duplex.Closed");
        public static readonly StatName STAT_NETWORKING_SOCKETS_CLIENTTOGW_DUPLEX_OPENED    = new StatName("Networking.Sockets.ClientToGW.Duplex.Opened");

        // Messaging
        public static readonly StatName STAT_MESSAGING_SENT_MESSAGES_TOTAL                  = new StatName("Messaging.Sent.Messages.Total");
        public static readonly StatName_Format STAT_MESSAGING_SENT_MESSAGES_PER_DIRECTION   = new StatName_Format("Messaging.Sent.Direction.{0}");
        public static readonly StatName_Format STAT_MESSAGING_SENT_MESSAGES_PER_SILO        = new StatName_Format("Messaging.Sent.Messages.To.{0}");
        public static readonly StatName STAT_MESSAGING_SENT_BYTES_TOTAL                     = new StatName("Messaging.Sent.Bytes.Total");
        public static readonly StatName STAT_MESSAGING_SENT_BYTES_HEADER                    = new StatName("Messaging.Sent.Bytes.Header");
        public static readonly StatName STAT_MESSAGING_SENT_MESSAGESIZEHISTOGRAM            = new StatName("Messaging.Sent.MessageSizeHistogram.Bytes");
        public static readonly StatName_Format STAT_MESSAGING_SENT_FAILED_PER_DIRECTION     = new StatName_Format("Messaging.Sent.Failed.{0}");
        public static readonly StatName_Format STAT_MESSAGING_SENT_DROPPED_PER_DIRECTION    = new StatName_Format("Messaging.Sent.Dropped.{0}");
        public static readonly StatName_Format STAT_MESSAGING_SENT_BATCH_SIZE_PER_SOCKET_DIRECTION                  = new StatName_Format("Messaging.Sent.BatchSize.PerSocketDirection.{0}");
        public static readonly StatName_Format STAT_MESSAGING_SENT_BATCH_SIZE_BYTES_HISTOGRAM_PER_SOCKET_DIRECTION  = new StatName_Format("Messaging.Sent.BatchSizeBytesHistogram.Bytes.PerSocketDirection.{0}");

        public static readonly StatName STAT_MESSAGING_RECEIVED_MESSAGES_TOTAL                      = new StatName("Messaging.Received.Messages.Total");
        public static readonly StatName_Format STAT_MESSAGING_RECEIVED_MESSAGES_PER_DIRECTION       = new StatName_Format("Messaging.Received.Direction.{0}");
        public static readonly StatName_Format STAT_MESSAGING_RECEIVED_MESSAGES_PER_SILO            = new StatName_Format("Messaging.Received.Messages.From.{0}");
        public static readonly StatName STAT_MESSAGING_RECEIVED_BYTES_TOTAL                         = new StatName("Messaging.Received.Bytes.Total");
        public static readonly StatName STAT_MESSAGING_RECEIVED_BYTES_HEADER                        = new StatName("Messaging.Received.Bytes.Header");
        public static readonly StatName STAT_MESSAGING_RECEIVED_MESSAGESIZEHISTOGRAM                = new StatName("Messaging.Received.MessageSizeHistogram.Bytes");
        public static readonly StatName_Format STAT_MESSAGING_RECEIVED_BATCH_SIZE_PER_SOCKET_DIRECTION                  = new StatName_Format("Messaging.Received.BatchSize.PerSocketDirection.{0}");
        public static readonly StatName_Format STAT_MESSAGING_RECEIVED_BATCH_SIZE_BYTES_HISTOGRAM_PER_SOCKET_DIRECTION  = new StatName_Format("Messaging.Received.BatchSizeBytesHistogram.Bytes.PerSocketDirection.{0}");

        public static readonly StatName_Format STAT_MESSAGING_DISPATCHER_RECEIVED_PER_DIRECTION     = new StatName_Format("Messaging.Processing.Dispatcher.Received.Direction.{0}");
        public static readonly StatName STAT_MESSAGING_DISPATCHER_RECEIVED_TOTAL                    = new StatName("Messaging.Processing.Dispatcher.Received.Total");
        public static readonly StatName STAT_MESSAGING_DISPATCHER_RECEIVED_ON_NULL                  = new StatName("Messaging.Processing.Dispatcher.Received.OnNullContext");
        public static readonly StatName STAT_MESSAGING_DISPATCHER_RECEIVED_ON_ACTIVATION            = new StatName("Messaging.Processing.Dispatcher.Received.OnActivationContext");

        public static readonly StatName_Format STAT_MESSAGING_DISPATCHER_PROCESSED_OK_PER_DIRECTION         = new StatName_Format("Messaging.Processing.Dispatcher.Processed.Ok.Direction.{0}");
        public static readonly StatName_Format STAT_MESSAGING_DISPATCHER_PROCESSED_ERRORS_PER_DIRECTION     = new StatName_Format("Messaging.Processing.Dispatcher.Processed.Errors.Direction.{0}");
        public static readonly StatName_Format STAT_MESSAGING_DISPATCHER_PROCESSED_REROUTE_PER_DIRECTION    = new StatName_Format("Messaging.Processing.Dispatcher.Processed.ReRoute.Direction.{0}");
        public static readonly StatName STAT_MESSAGING_DISPATCHER_PROCESSED_TOTAL                           = new StatName("Messaging.Processing.Dispatcher.Processed.Total");
       
        public static readonly StatName STAT_MESSAGING_IMA_RECEIVED                                 = new StatName("Messaging.Processing.IMA.Received");
        public static readonly StatName STAT_MESSAGING_IMA_ENQUEUED_TO_NULL                         = new StatName("Messaging.Processing.IMA.Enqueued.ToNullContex");
        public static readonly StatName STAT_MESSAGING_IMA_ENQUEUED_TO_SYSTEM_TARGET                = new StatName("Messaging.Processing.IMA.Enqueued.ToSystemTargetContex");
        public static readonly StatName STAT_MESSAGING_IMA_ENQUEUED_TO_ACTIVATION                   = new StatName("Messaging.Processing.IMA.Enqueued.ToActivationContex");

        public static readonly StatName STAT_MESSAGING_IGC_FORWARDED                                = new StatName("Messaging.Processing.IGC.Forwarded");
        public static readonly StatName STAT_MESSAGING_IGC_RESENT                                   = new StatName("Messaging.Processing.IGC.Resent");
        public static readonly StatName STAT_MESSAGING_IGC_REROUTE                                  = new StatName("Messaging.Processing.IGC.ReRoute");
        public static readonly StatName STAT_MESSAGING_PROCESSING_ACTIVATION_DATA_ALL               = new StatName("Messaging.Processing.ActivationData.All");

        public static readonly StatName_Format STAT_MESSAGING_PINGS_SENT_PER_SILO               = new StatName_Format("Messaging.Pings.Sent.{0}");
        public static readonly StatName_Format STAT_MESSAGING_PINGS_RECEIVED_PER_SILO           = new StatName_Format("Messaging.Pings.Received.{0}");
        public static readonly StatName_Format STAT_MESSAGING_PINGS_REPLYRECEIVED_PER_SILO      = new StatName_Format("Messaging.Pings.ReplyReceived.{0}");
        public static readonly StatName_Format STAT_MESSAGING_PINGS_REPLYMISSED_PER_SILO        = new StatName_Format("Messaging.Pings.ReplyMissed.{0}");
        public static readonly StatName STAT_MESSAGING_EXPIRED_ATSENDER                         = new StatName("Messaging.Expired.AtSend");
        public static readonly StatName STAT_MESSAGING_EXPIRED_ATRECEIVER                       = new StatName("Messaging.Expired.AtReceive");
        public static readonly StatName STAT_MESSAGING_EXPIRED_ATDISPATCH                       = new StatName("Messaging.Expired.AtDispatch");
        public static readonly StatName STAT_MESSAGING_EXPIRED_ATINVOKE                         = new StatName("Messaging.Expired.AtInvoke");
        public static readonly StatName STAT_MESSAGING_EXPIRED_ATRESPOND                        = new StatName("Messaging.Expired.AtRespond");
        public static readonly StatName_Format STAT_MESSAGING_REJECTED_PER_DIRECTION            = new StatName_Format("Messaging.Rejected.{0}");
        public static readonly StatName_Format STAT_MESSAGING_REROUTED_PER_DIRECTION            = new StatName_Format("Messaging.Rerouted.{0}");
        public static readonly StatName STAT_MESSAGING_SENT_LOCALMESSAGES                       = new StatName("Messaging.Sent.LocalMessages");
        public static readonly StatName STAT_MESSAGING_SENT_BATCH_SIZE                          = new StatName("Messaging.Sent.BatchSize");

        // MessageCenter
        public static readonly StatName STAT_MESSAGE_CENTER_SEND_QUEUE_LENGTH                   = new StatName("MessageCenter.SendQueueLength");
        public static readonly StatName STAT_MESSAGE_CENTER_RECEIVE_QUEUE_LENGTH                = new StatName("MessageCenter.ReceiveQueueLength");

        // Queues
        public static readonly StatName_Format STAT_QUEUES_QUEUE_SIZE_AVERAGE_PER_QUEUE         = new StatName_Format("Queues.QueueSize.Average.{0}");
        public static readonly StatName_Format STAT_QUEUES_QUEUE_SIZE_INSTANTANEOUS_PER_QUEUE   = new StatName_Format("Queues.QueueSize.Instantaneous.{0}");
        public static readonly StatName_Format STAT_QUEUES_ENQUEUED_PER_QUEUE                   = new StatName_Format("Queues.EnQueued.{0}");
        public static readonly StatName_Format STAT_QUEUES_AVERAGE_ARRIVAL_RATE_PER_QUEUE       = new StatName_Format("Queues.AverageArrivalRate.RequestsPerSecond.{0}");

        // Thread tracking
        public static readonly StatName_Format STAT_THREADS_PROCESSED_REQUESTS_PER_THREAD       = new StatName_Format("Thread.NumProcessedRequests.{0}");
        public static readonly StatName_Format STAT_THREADS_EXECUTION_TIME_TOTAL_CPU_CYCLES     = new StatName_Format("Thread.ExecutionTime.Total.CPUCycles.Milliseconds.{0}");
        public static readonly StatName_Format STAT_THREADS_EXECUTION_TIME_TOTAL_WALL_CLOCK     = new StatName_Format("Thread.ExecutionTime.Total.WallClock.Milliseconds.{0}");
        public static readonly StatName_Format STAT_THREADS_PROCESSING_TIME_TOTAL_CPU_CYCLES    = new StatName_Format("Thread.ProcessingTime.Total.CPUCycles.Milliseconds.{0}");
        public static readonly StatName_Format STAT_THREADS_PROCESSING_TIME_TOTAL_WALL_CLOCK    = new StatName_Format("Thread.ProcessingTime.Total.WallClock.Milliseconds.{0}");
        public static readonly StatName_Format STAT_THREADS_CONTEXT_SWITCHES                    = new StatName_Format("Thread.ContextSwitches.Total.SwitchCount.{0}");

        // Stage analysis
        public static readonly StatName STAT_STAGE_ANALYSIS                                     = new StatName("Thread.StageAnalysis");

        // Gateway
        public static readonly StatName STAT_GATEWAY_CONNECTED_CLIENTS              = new StatName("Gateway.ConnectedClients");
        public static readonly StatName STAT_GATEWAY_SENT                           = new StatName("Gateway.Sent");
        public static readonly StatName STAT_GATEWAY_RECEIVED                       = new StatName("Gateway.Received");
        public static readonly StatName STAT_GATEWAY_LOAD_SHEDDING                  = new StatName("Gateway.LoadShedding");

        // Runtime
        public static readonly StatName STAT_RUNTIME_CPUUSAGE                                           = new StatName("Runtime.CpuUsage");
        public static readonly StatName STAT_RUNTIME_GC_TOTALMEMORYKB                                   = new StatName("Runtime.GC.TotalMemoryKb");
        public static readonly StatName STAT_RUNTIME_GC_GENCOLLECTIONCOUNT                              = new StatName("Runtime.GC.GenCollectonCount");
        public static readonly StatName STAT_RUNTIME_GC_GENSIZESKB                                      = new StatName("Runtime.GC.GenSizesKb");
        public static readonly StatName STAT_RUNTIME_GC_PERCENTOFTIMEINGC                               = new StatName("Runtime.GC.PercentOfTimeInGC");
        public static readonly StatName STAT_RUNTIME_GC_ALLOCATEDBYTESINKBPERSEC                        = new StatName("Runtime.GC.AllocatedBytesInKbPerSec");
        public static readonly StatName STAT_RUNTIME_GC_PROMOTEDMEMORYFROMGEN1KB                        = new StatName("Runtime.GC.PromotedMemoryFromGen1Kb");
        public static readonly StatName STAT_RUNTIME_GC_LARGEOBJECTHEAPSIZEKB                           = new StatName("Runtime.GC.LargeObjectHeapSizeKb");
        public static readonly StatName STAT_RUNTIME_GC_PROMOTEDMEMORYFROMGEN0KB                        = new StatName("Runtime.GC.PromotedFinalizationMemoryFromGen0Kb");
        public static readonly StatName STAT_RUNTIME_GC_NUMBEROFINDUCEDGCS                              = new StatName("Runtime.GC.NumberOfInducedGCs");
        public static readonly StatName STAT_RUNTIME_DOT_NET_THREADPOOL_INUSE_WORKERTHREADS             = new StatName("Runtime.DOT.NET.ThreadPool.InUse.WorkerThreads");
        public static readonly StatName STAT_RUNTIME_DOT_NET_THREADPOOL_INUSE_COMPLETIONPORTTHREADS     = new StatName("Runtime.DOT.NET.ThreadPool.InUse.CompletionPortThreads");
        public static readonly StatName STAT_RUNTIME_IS_OVERLOADED                                      = new StatName("Runtime.IsOverloaded");
        public static readonly StatName_Format STAT_RUNTIME_THREADS_ASYNC_AGENT_PERAGENTTYPE            = new StatName_Format("Runtime.Threads.AsynchAgent.{0}");
        public static readonly StatName STAT_RUNTIME_THREADS_ASYNC_AGENT_TOTAL_THREADS_CREATED          = new StatName_Format("Runtime.Threads.AsynchAgent.TotalThreadsCreated");

        // Scheduler
        public static readonly StatName STAT_SCHEDULER_TURNSEXECUTED_APPLICATION_BYALLWORKERTHREADS     = new StatName("Scheduler.TurnsExecuted.Application.ByAllWorkerThreads");
        public static readonly StatName STAT_SCHEDULER_TURNSEXECUTED_APPLICATION_BYALLWORKITEMGROUPS    = new StatName("Scheduler.TurnsExecuted.Application.ByAllWorkItemGroups");
        public static readonly StatName_Format STAT_SCHEDULER_TURNSEXECUTED_APPLICATION_PERTHREAD       = new StatName_Format("Scheduler.TurnsExecuted.Application.ByThread.{0}");
        public static readonly StatName STAT_SCHEDULER_TURNSEXECUTED_SYSTEM_BYALLWORKERTHREADS          = new StatName("Scheduler.TurnsExecuted.System.ByAllWorkerThreads");
        public static readonly StatName STAT_SCHEDULER_TURNSEXECUTED_SYSTEM_BYALLWORKITEMGROUPS         = new StatName("Scheduler.TurnsExecuted.System.ByAllWorkItemGroups");
        public static readonly StatName_Format STAT_SCHEDULER_TURNSEXECUTED_SYSTEM_PERTHREAD            = new StatName_Format("Scheduler.TurnsExecuted.System.ByThread.{0}");
        public static readonly StatName STAT_SCHEDULER_TURNSEXECUTED_NULL_BYALLWORKERTHREADS            = new StatName("Scheduler.TurnsExecuted.Null.ByAllWorkerThreads");
        public static readonly StatName_Format STAT_SCHEDULER_TURNSEXECUTED_NULL_PERTHREAD              = new StatName_Format("Scheduler.TurnsExecuted.Null.ByThread.{0}");
        public static readonly StatName STAT_SCHEDULER_TURNSEXECUTED_TOTAL_START                        = new StatName("Scheduler.TurnsExecuted.Total.Start");
        public static readonly StatName STAT_SCHEDULER_TURNSEXECUTED_TOTAL_END                          = new StatName("Scheduler.TurnsExecuted.Total.End");
       
        public static readonly StatName_Format STAT_SCHEDULER_ACTIVATION_TURNSEXECUTED_PERACTIVATION    = new StatName_Format("Scheduler.Activation.TurnsExecuted.ByActivation.{0}");
        public static readonly StatName_Format STAT_SCHEDULER_ACTIVATION_STATUS_PERACTIVATION           = new StatName_Format("Scheduler.Activation.Status.ByActivation.{0}");
        public static readonly StatName STAT_SCHEDULER_TURN_LENGTH_HISTOGRAM                            = new StatName("Scheduler.TurnLengthHistogram.Ticks");
        public static readonly StatName STAT_SCHEDULER_PENDINGWORKITEMS                                 = new StatName("Scheduler.PendingWorkItems");
        public static readonly StatName STAT_SCHEDULER_WORKITEMGROUP_COUNT                              = new StatName("Scheduler.WorkItemGroupCount");
        public static readonly StatName STAT_SCHEDULER_NUM_LONG_RUNNING_TURNS                           = new StatName("Scheduler.NumLongRunningTurns");
        public static readonly StatName STAT_SCHEDULER_NUM_LONG_QUEUE_WAIT_TIMES                        = new StatName("Scheduler.NumLongQueueWaitTimes");
        //public static readonly StatName STAT_SCHEDULER_RUN_QUEUE_LENGTH_LEVEL_ONE                       = new StatName("Scheduler.RunQueueLength.LevelOne");
        //public static readonly StatName STAT_SCHEDULER_RUN_QUEUE_LENGTH_LEVEL_TWO                       = new StatName("Scheduler.RunQueueLength.LevelTwo");
        //public static readonly StatName STAT_SCHEDULER_RUN_QUEUE_LENGTH_TOTAL                           = new StatName("Scheduler.RunQueueLength.Total");

        public static readonly StatName STAT_SCHEDULER_ITEMS_ENQUEUED_TOTAL                             = new StatName("Scheduler.Items.EnQueued");
        public static readonly StatName STAT_SCHEDULER_ITEMS_DEQUEUED_TOTAL                             = new StatName("Scheduler.Items.DeQueued");
        public static readonly StatName STAT_SCHEDULER_ITEMS_DROPPED_TOTAL                              = new StatName("Scheduler.Items.Dropped");
        public static readonly StatName STAT_SCHEDULER_CLOSURE_WORK_ITEMS_CREATED                       = new StatName("Scheduler.ClosureWorkItems.Created");
        public static readonly StatName STAT_SCHEDULER_CLOSURE_WORK_ITEMS_EXECUTED                      = new StatName("Scheduler.ClosureWorkItems.Executed");

        // Serialization
        public static readonly StatName STAT_SERIALIZATION_BUFFERPOOL_BUFFERS_INPOOL                            = new StatName("Serialization.BufferPool.BuffersInPool");
        public static readonly StatName STAT_SERIALIZATION_BUFFERPOOL_ALLOCATED_BUFFERS                         = new StatName("Serialization.BufferPool.AllocatedBuffers");
        public static readonly StatName STAT_SERIALIZATION_BUFFERPOOL_CHECKED_OUT_BUFFERS                       = new StatName("Serialization.BufferPool.CheckedOutBuffers");
        public static readonly StatName STAT_SERIALIZATION_BUFFERPOOL_CHECKED_IN_BUFFERS                        = new StatName("Serialization.BufferPool.CheckedInBuffers");
        public static readonly StatName STAT_SERIALIZATION_BUFFERPOOL_CHECKED_IN_DROPPED_BUFFERS                = new StatName("Serialization.BufferPool.CheckedInDroppedBuffers");
        public static readonly StatName STAT_SERIALIZATION_BUFFERPOOL_INUSE_CHECKED_OUT_NOT_CHECKED_IN_BUFFERS  = new StatName("Serialization.BufferPool.InUse.CheckedOutAndNotCheckedIn_Buffers");
        public static readonly StatName STAT_SERIALIZATION_BUFFERPOOL_INUSE_ALLOCATED_NOT_INPOOL_BUFFERS        = new StatName("Serialization.BufferPool.InUse.AllocatedAndNotInPool_Buffers");
        public static readonly StatName STAT_SERIALIZATION_BODY_DEEPCOPIES                      = new StatName("Serialization.Body.DeepCopies");
        public static readonly StatName STAT_SERIALIZATION_BODY_SERIALIZATION                   = new StatName("Serialization.Body.Serializations");
        public static readonly StatName STAT_SERIALIZATION_BODY_DESERIALIZATION                 = new StatName("Serialization.Body.Deserializations");
        public static readonly StatName STAT_SERIALIZATION_HEADER_SERIALIZATION                 = new StatName("Serialization.Header.Serializations");
        public static readonly StatName STAT_SERIALIZATION_HEADER_DESERIALIZATION               = new StatName("Serialization.Header.Deserializations");
        public static readonly StatName STAT_SERIALIZATION_HEADER_SERIALIZATION_NUMHEADERS      = new StatName("Serialization.Header.Serialization.NumHeaders");
        public static readonly StatName STAT_SERIALIZATION_HEADER_DESERIALIZATION_NUMHEADERS    = new StatName("Serialization.Header.Deserialization.NumHeaders");
        public static readonly StatName STAT_SERIALIZATION_BODY_DEEPCOPY_MILLIS                 = new StatName("Serialization.Body.DeepCopy.Milliseconds");
        public static readonly StatName STAT_SERIALIZATION_BODY_SERIALIZATION_MILLIS            = new StatName("Serialization.Body.Serialization.Milliseconds");
        public static readonly StatName STAT_SERIALIZATION_BODY_DESERIALIZATION_MILLIS          = new StatName("Serialization.Body.Deserialization.Milliseconds");
        public static readonly StatName STAT_SERIALIZATION_HEADER_SERIALIZATION_MILLIS          = new StatName("Serialization.Header.Serialization.Milliseconds");
        public static readonly StatName STAT_SERIALIZATION_HEADER_DESERIALIZATION_MILLIS        = new StatName("Serialization.Header.Deserialization.Milliseconds");
        public static readonly StatName STAT_SERIALIZATION_TOTAL_TIME_IN_SERIALIZER_MILLIS      = new StatName("Serialization.TotalTimeInSerializer.Milliseconds");

        public static readonly StatName STAT_SERIALIZATION_BODY_FALLBACK_SERIALIZATION          = new StatName("Serialization.Body.Fallback.Serializations");
        public static readonly StatName STAT_SERIALIZATION_BODY_FALLBACK_DESERIALIZATION        = new StatName("Serialization.Body.Fallback.Deserializations");
        public static readonly StatName STAT_SERIALIZATION_BODY_FALLBACK_DEEPCOPIES             = new StatName("Serialization.Body.Fallback.DeepCopies");
        public static readonly StatName STAT_SERIALIZATION_BODY_FALLBACK_SERIALIZATION_MILLIS   = new StatName("Serialization.Body.Fallback.Serialization.Milliseconds");
        public static readonly StatName STAT_SERIALIZATION_BODY_FALLBACK_DESERIALIZATION_MILLIS = new StatName("Serialization.Body.Fallback.Deserialization.Milliseconds");
        public static readonly StatName STAT_SERIALIZATION_BODY_FALLBACK_DEEPCOPY_MILLIS        = new StatName("Serialization.Body.Fallback.DeepCopy.Milliseconds");

        // Catalog
        public static readonly StatName STAT_CATALOG_ACTIVATION_COUNT                                               = new StatName("Catalog.Activation.CurrentCount");
        public static readonly StatName STAT_CATALOG_ACTIVATION_REGISTRATIONS                                       = new StatName("Catalog.Activation.Registrations");
        public static readonly StatName STAT_CATALOG_ACTIVATION_UNREGISTRATIONS                                     = new StatName("Catalog.Activation.UnRegistrations");
        public static readonly StatName STAT_CATALOG_ACTIVATION_COLLECTION_NUMBER_OF_COLLECTIONS                    = new StatName("Catalog.Activation.Collection.NumberOfCollections");
        public static readonly StatName STAT_CATALOG_ACTIVATION_COLLECTION_NUMBER_OF_COLLECTED_ACTIVATIONS_PROMPT   = new StatName("Catalog.Activation.Collection.NumberOfCollectedActivations.Prompt");
        public static readonly StatName STAT_CATALOG_ACTIVATION_COLLECTION_NUMBER_OF_COLLECTED_ACTIVATIONS_DELAYED  = new StatName("Catalog.Activation.Collection.NumberOfCollectedActivations.Delayed");
        public static readonly StatName STAT_CATALOG_NON_EXISTING_ACTIVATIONS                                       = new StatName("Catalog.NonExistentActivations");
        public static readonly StatName STAT_CATALOG_DUPLICATE_ACTIVATIONS                                          = new StatName("Catalog.DuplicateActivations");

        // Dispatcher
        public static readonly StatName STAT_DISPATCHER_NEW_PLACEMENT_TYPE1                                         = new StatName("Dispatcher.NewPlacement.Type1");
        public static readonly StatName STAT_DISPATCHER_NEW_PLACEMENT_TYPE2                                         = new StatName("Dispatcher.NewPlacement.Type2");

        // Directory
        public static readonly StatName STAT_DIRECTORY_LOOKUPS_LOCAL_ISSUED                     = new StatName("Directory.Lookups.Local.Issued");
        public static readonly StatName STAT_DIRECTORY_LOOKUPS_LOCAL_SUCCESSES                  = new StatName("Directory.Lookups.Local.Successes");
        public static readonly StatName STAT_DIRECTORY_LOOKUPS_FULL_ISSUED                      = new StatName("Directory.Lookups.Full.Issued");
        public static readonly StatName STAT_DIRECTORY_LOOKUPS_REMOTE_SENT                      = new StatName("Directory.Lookups.Remote.Sent");
        public static readonly StatName STAT_DIRECTORY_LOOKUPS_REMOTE_RECEIVED                  = new StatName("Directory.Lookups.Remote.Received");
        public static readonly StatName STAT_DIRECTORY_LOOKUPS_LOCALDIRECTORY_ISSUED            = new StatName("Directory.Lookups.LocalDirectory.Issued");
        public static readonly StatName STAT_DIRECTORY_LOOKUPS_LOCALDIRECTORY_SUCCESSES         = new StatName("Directory.Lookups.LocalDirectory.Successes");
        public static readonly StatName STAT_DIRECTORY_LOOKUPS_CACHE_ISSUED                     = new StatName("Directory.Lookups.Cache.Issued");
        public static readonly StatName STAT_DIRECTORY_LOOKUPS_CACHE_SUCCESSES                  = new StatName("Directory.Lookups.Cache.Successes");
        public static readonly StatName STAT_DIRECTORY_LOOKUPS_CACHE_HITRATIO                   = new StatName("Directory.Lookups.Cache.HitRatio");
        public static readonly StatName STAT_DIRECTORY_VALIDATIONS_CACHE_SENT                   = new StatName("Directory.Validations.Cache.Sent");
        public static readonly StatName STAT_DIRECTORY_VALIDATIONS_CACHE_RECEIVED               = new StatName("Directory.Validations.Cache.Received");
        public static readonly StatName STAT_DIRECTORY_PARTITION_SIZE                           = new StatName("Directory.PartitionSize");
        public static readonly StatName STAT_DIRECTORY_CACHE_SIZE                               = new StatName("Directory.CacheSize");
        public static readonly StatName STAT_DIRECTORY_RING                                     = new StatName("Directory.Ring");
        public static readonly StatName STAT_DIRECTORY_RING_RINGSIZE                            = new StatName("Directory.Ring.RingSize");
        public static readonly StatName STAT_DIRECTORY_RING_MYPORTION_RINGDISTANCE              = new StatName("Directory.Ring.MyPortion.RingDistance");
        public static readonly StatName STAT_DIRECTORY_RING_MYPORTION_RINGPERCENTAGE            = new StatName("Directory.Ring.MyPortion.RingPercentage");
        public static readonly StatName STAT_DIRECTORY_RING_MYPORTION_AVERAGERINGPERCENTAGE     = new StatName("Directory.Ring.MyPortion.AverageRingPercentage");
        public static readonly StatName STAT_DIRECTORY_RING_PREDECESSORS                        = new StatName("Directory.Ring.MyPredecessors");
        public static readonly StatName STAT_DIRECTORY_RING_SUCCESSORS                          = new StatName("Directory.Ring.MySuccessors");

        public static readonly StatName STAT_DIRECTORY_REGISTRATIONS_ISSUED                     = new StatName("Directory.Registrations.Issued");
        public static readonly StatName STAT_DIRECTORY_REGISTRATIONS_LOCAL                      = new StatName("Directory.Registrations.Local");
        public static readonly StatName STAT_DIRECTORY_REGISTRATIONS_REMOTE_SENT                = new StatName("Directory.Registrations.Remote.Sent");
        public static readonly StatName STAT_DIRECTORY_REGISTRATIONS_REMOTE_RECEIVED            = new StatName("Directory.Registrations.Remote.Received");
        public static readonly StatName STAT_DIRECTORY_REGISTRATIONS_SINGLE_ACT_ISSUED          = new StatName("Directory.Registrations.SingleAct.Issued");
        public static readonly StatName STAT_DIRECTORY_REGISTRATIONS_SINGLE_ACT_LOCAL           = new StatName("Directory.Registrations.SingleAct.Local");
        public static readonly StatName STAT_DIRECTORY_REGISTRATIONS_SINGLE_ACT_REMOTE_SENT     = new StatName("Directory.Registrations.SingleAct.Remote.Sent");
        public static readonly StatName STAT_DIRECTORY_REGISTRATIONS_SINGLE_ACT_REMOTE_RECEIVED = new StatName("Directory.Registrations.SingleAct.Remote.Received");
        public static readonly StatName STAT_DIRECTORY_UNREGISTRATIONS_ISSUED                   = new StatName("Directory.UnRegistrations.Issued");
        public static readonly StatName STAT_DIRECTORY_UNREGISTRATIONS_LOCAL                    = new StatName("Directory.UnRegistrations.Local");
        public static readonly StatName STAT_DIRECTORY_UNREGISTRATIONS_REMOTE_SENT              = new StatName("Directory.UnRegistrations.Remote.Sent");
        public static readonly StatName STAT_DIRECTORY_UNREGISTRATIONS_REMOTE_RECEIVED          = new StatName("Directory.UnRegistrations.Remote.Received");
        public static readonly StatName STAT_DIRECTORY_UNREGISTRATIONS_MANY_ISSUED              = new StatName("Directory.UnRegistrationsMany.Issued");
        public static readonly StatName STAT_DIRECTORY_UNREGISTRATIONS_MANY_REMOTE_SENT         = new StatName("Directory.UnRegistrationsMany.Remote.Sent");
        public static readonly StatName STAT_DIRECTORY_UNREGISTRATIONS_MANY_REMOTE_RECEIVED     = new StatName("Directory.UnRegistrationsMany.Remote.Received");

        // MBR
        public static readonly StatName STAT_MEMBERSHIP_ACTIVE_CLUSTER                  = new StatName("Membership.ActiveCluster");
        public static readonly StatName STAT_MEMBERSHIP_ACTIVE_CLUSTER_SIZE             = new StatName("Membership.ActiveClusterSize");
        
        // Watchdog
        public static readonly StatName STAT_WATCHDOG_NUM_HEALTH_CHECKS                 = new StatName("Watchdog.NumHealthChecks");
        public static readonly StatName STAT_WATCHDOG_NUM_FAILED_HEALTH_CHECKS          = new StatName("Watchdog.NumFailedHealthChecks");

        // OrleansTimerInsideGrain
        public static readonly StatName_Format STAT_ORLEANS_TIMER_NUM_TICKS_PERTIMER    = new StatName_Format("OrleansTimerInsideGrain.NumTicks.{0}");

        // Client
        public static readonly StatName STAT_CLIENT_CONNECTED_GW_COUNT                  = new StatName("Client.ConnectedGWCount");

        // Silo
        public static readonly StatName STAT_SILO_START_TIME                            = new StatName("Silo.StartTime");

        // Misc
        public static readonly StatName_Format STAT_GRAIN_COUNTS_PER_GRAIN              = new StatName_Format("Grain.{0}");

        // App requests
        public static readonly StatName STAT_APP_REQUESTS_LATENCY_HISTOGRAM             = new StatName("App.Requests.LatencyHistogram.Millis");
        public static readonly StatName STAT_APP_REQUESTS_TIMED_OUT                     = new StatName("App.Requests.TimedOut");
        public static readonly StatName STAT_APP_REQUESTS_TOTAL_NUMBER_OF_REQUESTS      = new StatName("App.Requests.Total.Requests");
        public static readonly StatName STAT_APP_REQUESTS_TPS_LATEST                    = new StatName("App.Requests.TPS.Latest");
        public static readonly StatName STAT_APP_REQUESTS_TPS_TOTAL_SINCE_START         = new StatName("App.Requests.TPS.Total.SinceStart");

        // Reminders
        public static readonly StatName STAT_REMINDERS_AVERAGE_TARDINESS_SECONDS        = new StatName("Reminders.AverageTardiness.Seconds");
        public static readonly StatName STAT_REMINDERS_COUNTERS_ACTIVE                  = new StatName("Reminders.Counters.Active");
        public static readonly StatName STAT_REMINDERS_COUNTERS_TICKS_DELIVERED         = new StatName("Reminders.Counters.TicksDelivered");

        // Storage
        public static readonly StatName STAT_STORAGE_READ_TOTAL = new StatName("Storage.Read.Total");
        public static readonly StatName STAT_STORAGE_WRITE_TOTAL = new StatName("Storage.Write.Total");
        public static readonly StatName STAT_STORAGE_ACTIVATE_TOTAL = new StatName("Storage.Activate.Total");
        public static readonly StatName STAT_STORAGE_READ_ERRORS = new StatName("Storage.Read.Errors");
        public static readonly StatName STAT_STORAGE_WRITE_ERRORS = new StatName("Storage.Write.Errors");
        public static readonly StatName STAT_STORAGE_ACTIVATE_ERRORS = new StatName("Storage.Activate.Errors");
        public static readonly StatName STAT_STORAGE_READ_LATENCY = new StatName("Storage.Read.Latency");
        public static readonly StatName STAT_STORAGE_WRITE_LATENCY = new StatName("Storage.Write.Latency");
        public static readonly StatName STAT_STORAGE_CLEAR_TOTAL = new StatName("Storage.Clear.Total");
        public static readonly StatName STAT_STORAGE_CLEAR_ERRORS = new StatName("Storage.Clear.Errors");
        public static readonly StatName STAT_STORAGE_CLEAR_LATENCY = new StatName("Storage.Clear.Latency");

        // Azure
        public static readonly StatName STAT_AZURE_SERVER_BUSY = new StatName("Azure.ServerBusy");
    }
}
 