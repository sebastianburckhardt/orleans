using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans.Counters;
using Orleans.Runtime.Coordination;
using Orleans.Runtime.Scheduler;

using Orleans.Scheduler;


namespace Orleans.Runtime
{
    internal class Dispatcher
    {
        internal OrleansTaskScheduler Scheduler { get; private set; }

        internal ISiloMessageCenter Transport { get; private set; }

        //internal ISchedulingContext[] DispatcherSchedulingContexts { get; private set; }

        private readonly Catalog catalog;

        private readonly Logger logger;

        private readonly OrleansConfiguration config;

        private readonly double rejectionInjectionRate;

        private readonly bool errorInjection;

        private readonly double errorInjectionRate;

        private readonly SafeRandom rand;

        public Dispatcher(OrleansTaskScheduler scheduler, ISiloMessageCenter transport, Catalog catalog, OrleansConfiguration config)
        {
            this.Scheduler = scheduler;
            this.catalog = catalog;
            this.Transport = transport;
            this.config = config;
            //this.DispatcherSchedulingContexts = new ISchedulingContext[scheduler.Pool.MaxActiveThreads];
            //for (int i = 0; i < DispatcherSchedulingContexts.Length; i++)
            //{
            //    DispatcherSchedulingContexts[i] = new OrleansContext(i);
            //    scheduler.RegisterWorkContext(DispatcherSchedulingContexts[i]);
            //}
            logger = Logger.GetLogger("Dispatcher", Logger.LoggerType.Runtime);
            rejectionInjectionRate = config.Globals.RejectionInjectionRate;
            double messageLossInjectionRate = config.Globals.MessageLossInjectionRate;
            errorInjection = rejectionInjectionRate > 0.0d || messageLossInjectionRate > 0.0d;
            errorInjectionRate = rejectionInjectionRate + messageLossInjectionRate;
            rand = new SafeRandom();
        }

        #region Receive path

        /// <summary>
        /// Receive a new message:
        /// - validate order constraints, queue (or possibly redirect) if out of order
        /// - validate transactions constraints
        /// - invoke handler if ready, otherwise enqueue for later invocation
        /// </summary>
        /// <param name="message"></param>
        public void ReceiveMessage(Message message)
        {
            MessagingProcessingStatisticsGroup.OnDispatcherMessageReceive(message, AsyncCompletion.Context);
            // Don't process messages that have already timed out
            if (message.IsExpired)
            {
                logger.Warn(ErrorCode.Dispatcher_DroppingExpiredMessage, "Dropping an expired message: {0}", message);
                MessagingProcessingStatisticsGroup.OnDispatcherMessageProcessedError(message, "Expired");
                message.DropExpiredMessage(MessagingStatisticsGroup.Phase.Dispatch);
                return;
            }

            // check if its targeted at a new activation
            if (message.TargetGrain.IsSystemTarget)
            {
                MessagingProcessingStatisticsGroup.OnDispatcherMessageProcessedError(message, "ReceiveMessage on system target.");
                throw new InvalidOperationException("Dispatcher was called ReceiveMessage on system target for " + message);
            }

            if (errorInjection && ShouldInjectError(message))
            {
                if (logger.IsVerbose) logger.Verbose(ErrorCode.Dispatcher_InjectingRejection, "Injecting a rejection");
                MessagingProcessingStatisticsGroup.OnDispatcherMessageProcessedError(message, "ErrorInjection");
                RejectMessage(message, Message.RejectionTypes.FutureTransient, null, "Injected rejection");                
                return;
            }

            try
            {
                AsyncCompletion ignore;
                ActivationData target = catalog.GetOrCreateActivation(message.TargetAddress, message.PlacementStrategy, message.NewGrainType, message.GenericGrainType, out ignore);
                if (ignore != null)
                {
                    ignore.Ignore();
                }
                if (message.Direction == Message.Directions.Response)
                {
                    ReceiveResponse(message, target);
                }
                else // Request or OneWay
                {
                    ReasonDetail reason;
                    if (SiloCanAcceptRequest(message, out reason))
                    {
                        ReceiveRequest(message, target);
                    }
                    else if (message.MayResend(config.Globals))
                    {
                        // Record that this message is no longer flowing through the system
                        MessagingProcessingStatisticsGroup.OnDispatcherMessageProcessedError(message, "Redirecting");
                        // todo:[mlr] there's disagreement amongst the group regarding whether RedirectRequest() is still
                        // needed. we need to take another look at this at another time.
                        //RedirectRequest(message, target, reason);
                        throw new NotImplementedException("RedirectRequest() is believed to be no longer necessary; please contact the Orleans team if you see this error.");
                    }
                    else
                    {
                        // Record that this message is no longer flowing through the system
                        MessagingProcessingStatisticsGroup.OnDispatcherMessageProcessedError(message, "Rejecting");
                        if (reason.ReasonEquals(ReasonDetail.DuplicateRequest))
                        {
                            RejectMessage(message, Message.RejectionTypes.DuplicateRequest, null, "Duplicate");
                        }
                        else
                        {
                            RejectMessage(message, Message.RejectionTypes.FutureTransient, null, reason.ToString());
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                try
                {
                    logger.Warn(ErrorCode.Dispatcher_Intermediate_GetOrCreateActivation,
                                       "Intermediate warning from Catalog.GetOrCreateActivation for message {0}, {1}",
                                       message, ex);

                    MessagingProcessingStatisticsGroup.OnDispatcherMessageProcessedError(message, "NonExistingActivation");
              
                    Catalog.NonExistentActivationException nea = ex as Catalog.NonExistentActivationException;
                    if (nea == null)
                    {
                        throw new OrleansException("Error creating activation for " + message.NewGrainType, ex);
                    }

                     ActivationAddress nonExistentActivation = nea.NonExistentActivation;

                    if (message.Direction != Message.Directions.Response)
                    {
                        // Un-register the target activation so we don't keep getting spurious messages.
                        // The time delay (one minute, as of this writing) is to handle the unlikely but possible race where
                        // this request snuck ahead of another request, with new placement requested, for the same activation.
                        // If the activation registration request from the new placement somehow sneaks ahead of this un-registration,
                        // we want to make sure that we don't un-register the activation we just created.
                        // We would add a counter here, except that there's already a counter for this in the Catalog.
                        // Note that this has to run in a non-null scheduler context, so we always queue it to the catalog's context
                        if (config.Globals.DirectoryLazyDeregistrationDelay > TimeSpan.Zero)
                        {
                            Scheduler.QueueWorkItem(new ClosureWorkItem(
                                                        // don't use message.TargetAddress, cause it may have been removed from the headers by this time!
                                                        () => AsyncCompletion.FromTask(
                                                        Silo.CurrentSilo.LocalGrainDirectory
                                                            .UnregisterConditionallyAsync(nonExistentActivation)).
                                                             LogWarnings(logger,
                                                                         ErrorCode
                                                                             .Dispatcher_FailedToUnregisterNonExistingAct,
                                                                         String.Format(
                                                                             "Failed to un-register NonExistentActivation {0}",
                                                                             nonExistentActivation))
                                                            .Ignore(),
                                                        () => "LocalGrainDirectory.UnregisterConditionallyAsync"),
                                                    catalog.SchedulingContext);
                        }

                        message.AddToCacheInvalidationHeader(nonExistentActivation);
                        // and also clear the local cache, just in case.
                        Silo.CurrentSilo.LocalGrainDirectory.InvalidateCacheEntryPartly(nonExistentActivation.Grain,
                                                                                        nonExistentActivation.Activation);

                        // Record that this message is no longer flowing through the system
                        if (!InsideGrainClient.Current.TryForwardMessage(message))
                        {
                            RejectMessage(message, Message.RejectionTypes.FutureTransient, null, 
                                String.Format("{0}. Tried to forward message {1} for {2} times.", nea.Message, message, message.ForwardCount));
                        }
                    }
                    else
                    {
                        logger.Warn(ErrorCode.Dispatcher_NoTargetActivation,
                                    "No target activation {0} for response message: {1}", nonExistentActivation, message);
                        Silo.CurrentSilo.LocalGrainDirectory.InvalidateCacheEntryPartly(nonExistentActivation.Grain,
                                                                                        nonExistentActivation.Activation);
                    }
                }
                catch (Exception exc)
                {
                    // Unable to create activation for this request - reject message
                    RejectMessage(message, Message.RejectionTypes.FutureTransient, exc);
                }
            }
        }

        public void RejectMessage(Message message, Message.RejectionTypes rejectType, Exception exc, string rejectInfo = null)
        {
            if (message.Direction == Message.Directions.Request)
            {
                string str = String.Format("{0} {1}", rejectInfo ?? "", exc == null ? "" : exc.ToString());
                MessagingStatisticsGroup.OnRejectedMessage(message);
                Message reject = message.CreateRejectionResponse(rejectType, str);
                SendRejectionMessage(reject);
            }
            else
            {
                logger.Warn(ErrorCode.Messaging_Dispatcher_DiscardRejection,
                    "Discarding {0} rejection for message {1}. Exc = {2}", Enum.GetName(typeof(Message.Directions), message.Direction), message, exc.Message);
            }
        }

        internal void SendRejectionMessage(Message rejection)
        {
            if (rejection.Result == Message.ResponseTypes.Rejection)
            {
                Transport.SendMessage(rejection);
                rejection.ReleaseBodyAndHeaderBuffers();
            }
            else
            {
                throw new InvalidOperationException("Attempt to invoke Dispatcher.SendRejectionMessage() for a message that isn't a rejection.");
            }
        }

        private void ReceiveResponse(Message message, ActivationData targetActivation)
        {
            lock (targetActivation)
            {
                if (targetActivation.State == ActivationState.Invalid)
                {
                    logger.Warn(ErrorCode.Dispatcher_Receive_InvalidActivation, "Response received for invalid activation {0}", message);
                    MessagingProcessingStatisticsGroup.OnDispatcherMessageProcessedError(message, "Ivalid");
                    return;
                }
                MessagingProcessingStatisticsGroup.OnDispatcherMessageProcessedOK(message);
                if (Transport.TryDeliverToProxy(message))
                {
                    return;
                }
                else
                {
                GrainClient.InternalCurrent.ReceiveResponse(message);
            }
        }
        }

        // Check if it is OK to receive a message to its current target activation.
        // Accept all messages, unless we're shutting down and it's a request or a notification. 
        private bool SiloCanAcceptRequest(Message message, out ReasonDetail reason)
        {
            // todo: review - is this the right behavior for one-way messages?
            // todo: review - should this wait to accept task header? or eagerly acquire its info?
            message.AddTimestamp(Message.LifecycleTag.TaskIncoming);
            reason = null;
            if (catalog.SiloStatusOracle.CurrentStatus == SiloStatus.ShuttingDown)
            // && (message.Direction == Message.Directions.Request || message.Direction == Message.Directions.OneWay))
            {
                reason = ReasonDetail.ShuttingDown;
                return false;
            }
            return true;
        }

        /// <summary>
        /// Check if we can locally accept this message.
        /// Redirects if it can't be accepted.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="targetActivation"></param>
        private void ReceiveRequest(Message message, ActivationData targetActivation)
        {
            lock (targetActivation) // GK Deadlock
            {
                if (targetActivation.State == ActivationState.Invalid)
                {
                    ProcessRequestToInvalidActivation(message, targetActivation.ForwardingAddress, "process");
                }
                else if (!ActivationMayAcceptRequest(targetActivation, message))
                {
                    // Check for deadlock before Enqueueing.
                    if (config.Globals.PerformDeadlockDetection && !message.TargetGrain.IsSystemTarget)
                    {
                        try
                        {
                            CheckDeadlock(message);
                        }
                        catch (OrleansDeadlockException exc)
                        {
                            // Record that this message is no longer flowing through the system
                            MessagingProcessingStatisticsGroup.OnDispatcherMessageProcessedError(message, "Deadlock");
                            logger.Warn(ErrorCode.Dispatcher_DetectedDeadlock, "Detected Application Deadlock: {0}", exc.Message);
                            // We want to send DeadlockException back as an application exception, rather than as a system rejection.
                            SendResponse(message, OrleansResponse.ExceptionResponse(exc));
                            return;
                        }
                    }
                    EnqueueRequest(message, targetActivation);
                }
                else
                {
                    HandleIncomingRequest(message, targetActivation);
                }
            }
        }

        /// <summary>
        /// Determine if the activation is able to currently accept the given message
        /// - always accept responses
        /// For other messages, require that:
        /// - activation is properly initialized
        /// - the message would not cause a reentrancy  conflict
        /// - the message is in order
        /// </summary>
        /// <param name="targetActivation"></param>
        /// <param name="incoming"></param>
        /// <returns></returns>
        private bool ActivationMayAcceptRequest(ActivationData targetActivation, Message incoming)
        {
            if (!targetActivation.IsUsable) return false;
            if (targetActivation.Running == null) return true;
            if (!CanInterleave(targetActivation.Running, incoming)) return false;
            if (incoming.Direction == Message.Directions.Response) return true;
            return true;
        }

        /// <summary>
        /// Whether an incoming message can interleave 
        /// </summary>
        /// <param name="running"></param>
        /// <param name="incoming"></param>
        /// <returns></returns>
        public bool CanInterleave(Message running, Message incoming)
        {
            bool canInterleave = 
                (running.IsReadOnly && incoming.IsReadOnly) 
                || catalog.IsReentrantGrain(running.TargetActivation)
                || incoming.IsAlwaysInterleave;
#if DEBUG
            // This is a hot code path, so using #if to remove diags from Release version
            if (logger.IsVerbose2) logger.Verbose2(
                "CanInterleave={0} Incoming Message={1} Running = {2}", 
                canInterleave, incoming, running);
#endif
            return canInterleave;
        }

        // check if the current message will cause deadlock.
        // throw DeadlockException if yes.
        private void CheckDeadlock(Message message)
        {
            object obj = message.GetApplicationHeader(RequestContext.CallChainRequestContextHeader);
            if (obj == null) return; // first call in a chain

            IList prevChain = ((IList)obj);
            ActivationId nextId = message.TargetActivation;
            // check if the target activation already appears in the call chain.
            foreach (object invocationObj in prevChain)
            {
                ActivationId prevId = ((RequestInvocationHistory)invocationObj).ActivationId;
                if (prevId.Equals(nextId) && !catalog.IsReentrantGrain(nextId))
                {
                    List<RequestInvocationHistory> newChain = new List<RequestInvocationHistory>();
                    newChain.AddRange(prevChain.Cast<RequestInvocationHistory>());
                    newChain.Add(new RequestInvocationHistory(message));
                    throw new OrleansDeadlockException(newChain.Select(req =>
                                    new Tuple<GrainId, int, int>(req.GrainId, req.InterfaceId, req.MethodId)).ToList());
                }
            }
        }

        /// <summary>
        /// Handle an incoming message and queue/invoke appropriate handler
        /// </summary>
        /// <param name="message"></param>
        /// <param name="targetActivation"></param>
        public void HandleIncomingRequest(Message message, ActivationData targetActivation)
        {
            lock (targetActivation)
            {
                if (targetActivation.State == ActivationState.Invalid)
                {
                    ProcessRequestToInvalidActivation(message, targetActivation.ForwardingAddress, "handle");
                    return;
                }

#if false       // GK: We have already end checked overload when new request arrived to the silo. No need to check it again now.
                // Check for silo overload, and reject incoming requests if necessary
                var overloadException = targetActivation.CheckOverloaded(logger);
                if (overloadException != null)
                {
                    MessagingProcessingStatisticsGroup.OnDispatcherMessageProcessedError(message, "Overload");
                    RejectMessage(message, Message.RejectionTypes.FutureTransient, overloadException, "Target activation is overloaded " + targetActivation);
                    return;
                }
#endif

                // Now we can actually scheduler processing of this request
                targetActivation.RecordRunning(message);
                var context = new OrleansContext(targetActivation);
                message.AddTimestamp(Message.LifecycleTag.EnqueueWorkItem);
                MessagingProcessingStatisticsGroup.OnDispatcherMessageProcessedOK(message);
#if ALLOW_GRAPH_PARTITION_STRATEGY
                if (Constants.ALLOW_GRAPH_PARTITION_STRATEGY)
                {
                    GraphPartitionDirector.MessageBetweenGrainAndSilo(message.TargetAddress, message.SendingSilo, message.SendingAddress);
                }
#endif
                Scheduler.QueueWorkItem(new InvokeWorkItem(targetActivation, message, context), context);
            }
        }

        /// <summary>
        /// Enqueue message for local handling after transaction completes
        /// </summary>
        /// <param name="message"></param>
        /// <param name="targetActivation"></param>
        private void EnqueueRequest(Message message, ActivationData targetActivation)
        {
            var overloadException = targetActivation.CheckOverloaded(logger);
            if (overloadException != null)
            {
                MessagingProcessingStatisticsGroup.OnDispatcherMessageProcessedError(message, "Overload2");
                RejectMessage(message, Message.RejectionTypes.FutureTransient, overloadException, "Target activation is overloaded " + targetActivation);
                return;
            }
            message.AddTimestamp(Message.LifecycleTag.EnqueueWaiting);
            
            bool enqueuedOk = targetActivation.EnqueueMessage(message);
            if (!enqueuedOk)
            {
                ProcessRequestToInvalidActivation(message, targetActivation.ForwardingAddress, "enqueue");
            }

            // Dont count this as end of processing. The msg will come back after queueing via HandleIncomingRequest.

#if DEBUG
            // This is a hot code path, so using #if to remove diags from Release version
            // Note: Caller already holds lock on activation
            if (logger.IsVerbose2) logger.Verbose2(ErrorCode.Dispatcher_EnqueueMessage, 
                "EnqueueMessage for {0}: Running={1} Waiting={2}",
                message.TargetActivation, targetActivation.Running, targetActivation.PrintWaitingQueue());
#endif
        }

        internal void ProcessRequestToInvalidActivation(Message message, ActivationAddress forwardingAddress, string failedOperation, bool countError = true)
        {
            // Record that this message is no longer flowing through the system
            if (countError)
            {
                MessagingProcessingStatisticsGroup.OnDispatcherMessageProcessedError(message, failedOperation);
            }
            if (forwardingAddress != null)
            {
                // We have a forwarding address, so use to send to correct grain
                message.TargetAddress = forwardingAddress;
                Transport.SendMessage(message);
            }
            else
            {
                // otherwise reject the message
                RejectMessage(message, Message.RejectionTypes.Unrecoverable, null,
                        string.Format("Cannot {0} message for invalid target activation = {1}. Message = {2}", failedOperation, message.TargetAddress, message));
            }
        }

        #endregion

        #region Send path

        /// <summary>
        /// Send an outgoing message
        /// - may buffer for transaction completion / commit if it ends a transaction
        /// - choose target placement address, maintaining send order
        /// - add ordering info & maintain send order
        /// 
        /// </summary>
        /// <param name="message"></param>
        public async Task AsyncSendMessage(Message message)
        {
            try
            {
                await AddressMessage(message);
#if ALLOW_GRAPH_PARTITION_STRATEGY
                if (Constants.ALLOW_GRAPH_PARTITION_STRATEGY)
                {
                    GraphPartitionDirector.MessageBetweenGrainAndSilo(message.SendingAddress, message.TargetSilo, message.TargetAddress);
                }
#endif
                TransportMessage(message);
            }
            catch (Exception ex)
            {
                if (!(ex.GetBaseException() is KeyNotFoundException))
                {
                    logger.Error(ErrorCode.Dispatcher_SelectTarget_Failed,
                            String.Format("SelectTarget failed with {0}", ex.Message),
                            ex);
                }
                MessagingProcessingStatisticsGroup.OnDispatcherMessageProcessedError(message, "SelectTarget failed");
                RejectMessage(message, Message.RejectionTypes.Unrecoverable, ex);
            }
        }

        // [mlr] this is a compatibility method for portions of the code base that don't use
        // async/await yet, which is almost everything. there's no liability to discarding the
        // Task returned by AsyncSendMessage(), since 
        public void SendMessage(Message message)
        {
            AsyncSendMessage(message).Ignore();
        }

        /// <summary>
        /// Resolve target address for a message
        /// - use transaction info
        /// - check ordering info in message & sending activation
        /// - use sender's placement strategy
        /// </summary>
        /// <param name="message"></param>
        /// <returns>Resolve when message is addressed (modifies message fields)</returns>
        private async Task AddressMessage(Message message)
        {
            var targetAddress = message.TargetAddress;
            if (targetAddress.IsComplete) 
                return;

            // [mlr] placement strategy is determined by searching for a specification. first, we check for a strategy associated with the grain reference,
            // second, we check for a strategy associated with the target's interface. third, we check for a strategy associated with the activation sending the
            // message.
            var interfaceStrategy = targetAddress.Grain.IsGrain ? catalog.GetGrainPlacementStrategy(targetAddress.Grain) : null;
            var strategy = message.PlacementStrategy ?? interfaceStrategy; 
            var p = await PlacementDirector.SelectOrAddTarget(message.SendingAddress, message.TargetGrain, InsideGrainClient.Current.Catalog, strategy);
            if (p.IsNewPlacement && targetAddress.Grain.IsClient)
            {
                logger.Error(ErrorCode.Dispatcher_AddressMsg_UnregisteredClient, String.Format("AddressMessage could not find target for client pseudo-grain {0}", message));
                throw new KeyNotFoundException("Attempting to send a message to an unregistered client pseudo-grain");
            }
            message.SetTargetPlacement(p);
            // todo: remove - temporary for debugging placement distribution
            if (p.IsNewPlacement)
            {
                CounterStatistic.FindOrCreate(StatNames.STAT_DISPATCHER_NEW_PLACEMENT_TYPE2).Increment();
            }
            if (logger.IsVerbose2) logger.Verbose2(ErrorCode.Dispatcher_AddressMsg_SelectTarget, "AddressMessage Placement SelectTarget {0}", message);
       }

        internal void SendResponse(Message request, OrleansResponse response)
        {
            // create the response
            var message = request.CreateResponseMessage();
            message.BodyObject = response;

            if (message.TargetGrain.IsSystemTarget)
            {
                SendSystemTargetMessage(message);
            }
            else
            {
                //bool processedOk = (!response.ExceptionFlag || response.Exception == null);
                //MessagingProcessingStatisticsGroup.OnDispatcherMessageProcessed(processedOk);
                TransportMessage(message);
            }
        }

        internal void SendSystemTargetMessage(Message message)
        {
            message.Category = message.TargetGrain.Equals(Constants.MembershipOracleId)
                    ? Message.Categories.Ping : Message.Categories.System;

            if (message.TargetSilo == null)
            {
                message.TargetSilo = Transport.MyAddress;
            }
            if (message.TargetActivation == null)
            {
                message.TargetActivation = ActivationId.GetSystemActivation(message.TargetGrain, message.TargetSilo);
            }
            TransportMessage(message);
        }

        /// <summary>
        /// Directly send a message to the transport without processing
        /// </summary>
        /// <param name="message"></param>
        public void TransportMessage(Message message)
        {
            if (logger.IsVerbose2) logger.Verbose2(ErrorCode.Dispatcher_Send_AddressedMessage, "Addressed message {0}", message);
            Transport.SendMessage(message);
        }

        #endregion
        #region Execution

        /// <summary>
        /// Invoked when an activation has finished a transaction and may be ready for additional transactions
        /// </summary>
        /// <param name="activation">The activation that has just completed processing this message</param>
        /// <param name="message">The message that has just completed processing. 
        /// This will be <c>null</c> for the case of completion of Activate/Deactivate calls.</param>
        internal void OnActivationCompletedRequest(ActivationData activation, Message message)
        {
            lock (activation)
            {
#if DEBUG
                // This is a hot code path, so using #if to remove diags from Release version
                if (logger.IsVerbose2)
                {
                    logger.Verbose2(ErrorCode.Dispatcher_OnActivationCompletedRequest_Waiting,
                        "OnActivationCompletedRequest {0}: State={1} Waiting={2}",
                        activation.ActivationId, activation.State, activation.PrintWaitingQueue());
                }
#endif
                activation.ResetRunning(message);

                if (catalog.SiloStatusOracle.CurrentStatus == SiloStatus.ShuttingDown)
                    return;

                // ensure inactive callbacks get run even with transactions disabled
                if (activation.IsInactive)
                {
                    activation.RunOnInactive();
                }

                // Run message pump to see if there is a new request arrived to be processed
                RunMessagePump(activation);
            }
        }

        internal void OnActivateDeactivateCompleted(ActivationData activation)
        {
            lock (activation)
            {
                if (catalog.SiloStatusOracle.CurrentStatus == SiloStatus.ShuttingDown)
                    return;

                if (activation.State == ActivationState.Activating)
                {
                    activation.SetState(ActivationState.Inactive); // Activate calls on this activation are finished
                }
                else if (activation.State == ActivationState.Deactivating)
                {
                    activation.SetState(ActivationState.Invalid); // Deactivate calls on this activation are finished
                    catalog.OnFinishedGrainDeactivate(activation);
                }

                // ensure inactive callbacks get run even with transactions disabled
                if (activation.IsInactive)
                {
                    activation.RunOnInactive();
                }

                // Run message pump to see if there is a new request arrived to be processed
                RunMessagePump(activation);
            }
        }

        internal void RunMessagePump(ActivationData activation)
        {
            // Note: this method must be called while holding lock (activation)
#if DEBUG
            // This is a hot code path, so using #if to remove diags from Release version
            // Note: Caller already holds lock on activation
            if (logger.IsVerbose2)
            {
                logger.Verbose2(ErrorCode.Dispatcher_ActivationEndedTurn_Waiting,
                    "RunMessagePump {0}: State={1} Running={2} Waiting={3}",
                    activation.ActivationId, activation.State, activation.Running, activation.PrintWaitingQueue());
            }
#endif
            // don't run any messages until activation is ready
            if (activation.State != ActivationState.Inactive)
                return;

            bool runLoop;
            do
            {
                runLoop = false;
                Message nextMessage = activation.PeekNextWaitingMessage();
                if (nextMessage != null)
                {
                    if (ActivationMayAcceptRequest(activation, nextMessage))
                    {
                        activation.DequeueNextWaitingMessage();
                        //logger.Info("Pumping.");
                        // TODO: we might be over-writing an already running read only request.
                        HandleIncomingRequest(nextMessage, activation);
                        runLoop = true;
                    }
                }
            }
            while (runLoop);
        }

        private bool ShouldInjectError(Message message)
        {
            if (errorInjection && message.Direction == Message.Directions.Request)
            {
                double r = rand.NextDouble() * 100;
                if (r < errorInjectionRate)
                {
                    if (r < rejectionInjectionRate)
                    {
                        //if (logger.IsVerbose) logger.Verbose(ErrorCode.Dispatcher_InjectingRejection, "Injecting a rejection");
                        //MessagingProcessingStatisticsGroup.OnDispatcherMessageProcessedError(message);
                        //RejectMessage(message, Message.RejectionTypes.FutureTransient, null, "Injected rejection");
                        return true;
                    }

                    if (logger.IsVerbose) logger.Verbose(ErrorCode.Dispatcher_InjectingMessageLoss, "Injecting a message loss");
                    // else do nothing and intentionally drop message on the floor to inject a message loss
                    return true;
                }
            }
            return false;
        }

        #endregion
    }
}
