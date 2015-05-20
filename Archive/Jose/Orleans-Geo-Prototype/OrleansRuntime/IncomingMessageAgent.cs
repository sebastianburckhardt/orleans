using System;
using System.Threading;
using Orleans.Runtime.Coordination;
using Orleans.Runtime.Messaging;
using Orleans.Runtime.Scheduler;

using Orleans.Scheduler;
using Orleans.Counters;

namespace Orleans.Runtime
{
    internal class IncomingMessageAgent : AsynchAgent, ISiloShutdownParticipant
    {
        private readonly IMessageCenter messageCenter;
        private readonly TargetDirectory directory;
        private readonly OrleansTaskScheduler scheduler;
        private readonly Dispatcher dispatcher;
        private readonly Message.Categories category;

        internal IncomingMessageAgent(Message.Categories cat, IMessageCenter mc, TargetDirectory ad, OrleansTaskScheduler sched, Dispatcher dispatcher) :
            base(cat.ToString())
        {
            category = cat;
            messageCenter = mc;
            directory = ad;
            scheduler = sched;
            this.dispatcher = dispatcher;
            onFault = FaultBehavior.RestartOnFault;
        }

        public override void Start()
        {
            base.Start();
            if (log.IsVerbose3) log.Verbose3("Started incoming message agent for silo at {0} for {1} messages", messageCenter.MyAddress, category);
        }

        protected override void Run()
        {
            try
            {
#if TRACK_DETAILED_STATS
                if (StatisticsCollector.CollectThreadTimeTrackingStats)
                {
                    threadTracking.OnStartExecution();
                }
#endif
                CancellationToken ct = cts.Token;
                while (true)
                {
                    // Get an application message
                    Message wmsg = messageCenter.WaitMessage(category, ct);
                    if (wmsg == null)
                    {
                        if (log.IsVerbose) log.Verbose("Dequeued a null message, exiting");
                        // Null return means cancelled
                        break;
                    }

#if TRACK_DETAILED_STATS
                    if (StatisticsCollector.CollectThreadTimeTrackingStats)
                    {
                        threadTracking.OnStartProcessing();
                    }
#endif
                    ReceiveMessage(wmsg);
#if TRACK_DETAILED_STATS
                    if (StatisticsCollector.CollectThreadTimeTrackingStats)
                    {
                        threadTracking.OnStopProcessing();
                        threadTracking.IncrementNumberOfProcessed();
                    }
#endif
                }
            }
            finally
            {
#if TRACK_DETAILED_STATS
                if (StatisticsCollector.CollectThreadTimeTrackingStats)
                {
                    threadTracking.OnStopExecution();
                }
#endif
            }
        }

        private void ReceiveMessage(Message wmsg)
        {
            MessagingProcessingStatisticsGroup.OnIMAMessageReceived(wmsg);
            wmsg.AddTimestamp(Message.LifecycleTag.DequeueIncoming);

            ISchedulingContext context;
            // Find the activation it targets; first check for a system activation, then an app activation
            if (wmsg.TargetGrain.IsSystemTarget)
            {
                SystemTarget target = directory.FindSystemTarget(wmsg.TargetActivation);
                if (target == null)
                {
                    MessagingStatisticsGroup.OnRejectedMessage(wmsg);
                    Message response = wmsg.CreateRejectionResponse(Message.RejectionTypes.FutureTransient, String.Format("SystemTarget {0} not active on this silo. Msg={1}", wmsg.TargetGrain, wmsg.ToString()));
                    messageCenter.SendMessage(response);
                    log.Warn(ErrorCode.MessagingMessageFromUnknownActivation, "Received a message for an unknown SystemTarget: {0}", wmsg.TargetAddress);
                    return;
                }
                context = target.SchedulingContext;
                switch (wmsg.Direction)
                {
                    case Message.Directions.Request:
                        wmsg.AddTimestamp(Message.LifecycleTag.EnqueueWorkItem);
                        MessagingProcessingStatisticsGroup.OnIMAMessageEnqueued(context);
                        scheduler.QueueWorkItem(new RequestWorkItem(target, wmsg), context);
                        break;
                    case Message.Directions.Response:
                        wmsg.AddTimestamp(Message.LifecycleTag.EnqueueWorkItem);
                        MessagingProcessingStatisticsGroup.OnIMAMessageEnqueued(context);
                        scheduler.QueueWorkItem(new ResponseWorkItem(target, wmsg), context);
                        break;
                    default:
                        log.Error(ErrorCode.Runtime_Error_100097, "Invalid message: " + wmsg);
                        break;
                }
            }
            else if (wmsg.Result == Message.ResponseTypes.Rejection)
            {
                // [mlr] if a rejection message is passed through the scheduler, it may be dropped. it seems to succeed if we
                // pass it directly to the dispatcher (see the call to dispatcher.RejectMessage a few lines up).
                dispatcher.SendRejectionMessage(wmsg);
            }
            else
            {
                // Run this code on the target activation's context, if it already exists
                ActivationData targetActivation = directory.FindTarget(wmsg.TargetActivation);
                if (targetActivation != null)
                {
                    lock (targetActivation)
                    {
                        ActivationData target = targetActivation;

                        if (target.IsUsable)
                        {
                            var overloadException = target.CheckOverloaded(log);
                            if (overloadException != null)
                            {
                                // Send rejection as soon as we can, to avoid creating additional work for runtime
                                dispatcher.RejectMessage(wmsg, Message.RejectionTypes.FutureTransient, overloadException, "Target activation is overloaded " + target);
                                return;
                            }

                            // Run ReceiveMessage in context of target activation
                            context = new OrleansContext(target);
                        }
                        else
                        {
                            // Can't use this activation - will queue for another activation
                            target = null;
                            context = null;
                            // TODO: Should we null out wmsg.TargetActivation to force redirect?
                        }

                        EnqueueReceiveMessage(wmsg, target, context);
                    }
                }
                else
                {
                    // No usable target activation currently, so run ReceiveMessage in system context
                    EnqueueReceiveMessage(wmsg, null, null);
                }
            }
        }

        private void EnqueueReceiveMessage(Message wmsg, ActivationData targetActivation, ISchedulingContext context)
        {
            MessagingProcessingStatisticsGroup.OnIMAMessageEnqueued(context);

            if (targetActivation != null) targetActivation.IncrementEnqueuedOnDispatcherCount();

            scheduler.QueueWorkItem(new ClosureWorkItem(() =>
            {
                try
                {
                    dispatcher.ReceiveMessage(wmsg);
                }
                finally
                {
                    if (targetActivation != null) targetActivation.DecrementEnqueuedOnDispatcherCount();
                }
            },
            () => "Dispatcher.ReceiveMessage"), context);
        }

        #region Implementation of ISiloShutdownParticipant

        public void BeginShutdown(Action tryFinishShutdown)
        {
            // nothing
        }

        public bool CanFinishShutdown()
        {
            return true;
        }

        public void FinishShutdown()
        {
            Stop();
        }

        public SiloShutdownPhase Phase { get { return SiloShutdownPhase.Messaging; } }

        #endregion
    }
}
