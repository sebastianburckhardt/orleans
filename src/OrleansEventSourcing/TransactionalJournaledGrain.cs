using Orleans.Concurrency;
using Orleans.MultiCluster;
using Orleans.LogConsistency;
using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.Storage;
using Orleans.Transactions;
using Orleans;

namespace Orleans.EventSourcing
{
    /// <summary>
    /// A base class for log-consistent grains using standard event-sourcing terminology.
    /// All operations are reentrancy-safe.
    /// <typeparam name="TGrainState">The type for the grain state, i.e. the aggregate view of the event log.</typeparam>
    /// </summary>
    public abstract class TransactionalJournaledGrain<TGrainState> : JournaledGrain<TGrainState, object>
        where TGrainState : class, new()
    { }

    /// <summary>
    /// A base class for log-consistent grains using standard event-sourcing terminology.
    /// All operations are reentrancy-safe.
    /// <typeparam name="TState">The type for the grain state, i.e. the aggregate view of the event log.</typeparam>
    /// <typeparam name="TEvent">The common base class for the events</typeparam>
    /// </summary>
    public abstract class TransactionalJournaledGrain<TState, TEvent> :
        LogConsistentGrainBase<TState>,
        ITransactionalGrain,
        ILogConsistentGrain,
        ILogConsistencyProtocolParticipant,
        ILogViewAdaptorHost<TransactionalJournaledGrain<TState, TEvent>.LState, TransactionalJournaledGrain<TState, TEvent>.LEvent>
        where TState : class, new()
        where TEvent : class
    {
        protected TransactionalJournaledGrain() { }

        #region lower layer data representation

        // the log view adaptor we use for persisting the state & metadata for this transactional grain
        // it uses lower-layer LState and LEvent types
        private ILogViewAdaptor<LState, LEvent> storageAdaptor;

        /// <summary>
        /// The lower-layer state. Contains both a stable checkpoint and a log of active transactions.
        /// </summary>
        [Serializable]
        public class LState
        {
            public TState StableState { get; set; }

            public GrainVersion StableVersion { get; set; }

            public List<LUpdateEvent> Active { get; set; }

            public LState()
            {
                StableState = new TState();
                Active = new List<LUpdateEvent>();
            }

            public GrainVersion LatestVersion { get {
                    return Active.LastOrDefault()?.Version ?? StableVersion;
                } }
        }

        /// <summary>
        /// The superclass of all events describing operations on the lower-layer
        /// </summary>
        [Serializable]
        public abstract class LEvent
        {
            public abstract void Update(LState state, TransactionalJournaledGrain<TState, TEvent> host);
        }

        [Serializable]
        public class LUpdateEvent : LEvent
        {
            public GrainVersion Version { get; set; }

            public TEvent Event { get; set; }

            public override void Update(LState state, TransactionalJournaledGrain<TState, TEvent> host)
            {
                state.Active.Add(this);
            }
        }

        [Serializable]
        public class LUndoEvent : LEvent
        {
            public int Count { get; set; }

            public override void Update(LState state, TransactionalJournaledGrain<TState, TEvent> host)
            {
                state.Active.RemoveRange(state.Active.Count - Count, Count);
            }
        }

        [Serializable]
        public class LStabilizeEvent : LEvent
        {
            public int Count { get; set; }

            public override void Update(LState state, TransactionalJournaledGrain<TState, TEvent> host)
            {
                for (int i = 0; i < Count; i++)
                {
                    var update = state.Active[i];
                    try
                    {
                        host.TransitionState(state.StableState, update.Event);
                    }
                    catch (Exception e)
                    {
                        host.services.CaughtUserCodeException("UpdateView", nameof(LStabilizeEvent.Update), e);
                    }
                    state.StableVersion = update.Version;
                }
                state.Active.RemoveRange(0, Count);
            }
        }

        #endregion

        #region user operations

        /// <summary>
        /// Raise an event.
        /// </summary>
        /// <param name="event">Event to raise</param>
        /// <returns></returns>
        protected void RaiseEvent(TEvent @event)
        {
            if (@event == null) throw new ArgumentNullException("event");

            var info = services.TransactionInfo;

            if (info.IsReadOnly)
                throw new OrleansReadOnlyViolatedException(info.TransactionId);

            Restore(); // remove updates by transactions that have already aborted

            var state = storageAdaptor.TentativeView;
            var latestversion = state.LatestVersion;

            // do not allow writes belonging to transactions older than the one that last wrote
            if (latestversion.TransactionId > info.TransactionId)
                throw new OrleansTransactionWaitDieException(info.TransactionId);

            // record write in transaction info
            RecordWrite(info, latestversion, state.StableVersion);
            
            // send the event to storage
            storageAdaptor.Submit(new LUpdateEvent()
            {
                Version = new GrainVersion()
                {
                    TransactionId = info.TransactionId,
                    WriteNumber = (info.TransactionId == latestversion.TransactionId) ? latestversion.WriteNumber + 1 : 0
                }
            });

            // update the cached state
            if (cachedStates.ContainsKey(info.TransactionId))
            {
                try
                {
                    TransitionState(cachedStates[info.TransactionId], @event);
                }
                catch (Exception e)
                {
                    services.CaughtUserCodeException("UpdateView", nameof(RaiseEvent), e);
                }
                
            }
        }

         /// <summary>
        /// Read the current state.
        /// </summary>
        protected TState State
        {
            get
            {
                Restore(); // remove updates by transactions that have already aborted

                var info = services.TransactionInfo;

                // if we have this transaction's state cached, we can return it immediately
                if (cachedStates.ContainsKey(info.TransactionId))
                {
                    return cachedStates[info.TransactionId];
                }

                var state = storageAdaptor.TentativeView;

                // if the asked-for version is older than the stable version, we cannot serve this read.
                if (state.StableVersion.TransactionId > info.TransactionId)
                    throw new OrleansTransactionVersionDeletedException(info.TransactionId);

                // deep-copy the stable state, then roll forward
                var copy = (TState)services.SerializationManager.DeepCopy(state.StableState);
                var copyversion = state.StableVersion;

                for (int i = 0; i < state.Active.Count; i++)
                {
                    var nextupdate = state.Active[i];

                    // if we are getting into updates that are ordered after this transaction, stop
                    if (nextupdate.Version.TransactionId > info.TransactionId)
                        break;

                    try
                    {
                        TransitionState(copy, nextupdate.Event);
                    }
                    catch (Exception e)
                    {
                        services.CaughtUserCodeException("UpdateView", nameof(State), e);
                    }
                    copyversion = nextupdate.Version;
                }

                // record the read version
                RecordRead(info, copyversion, state.StableVersion);

                // cache this state for quicker future lookup
                if (!info.IsReadOnly)
                    cachedStates[info.TransactionId] = copy;

                return copy;
            }
        }

        public ILogViewAdaptorFactory DefaultAdaptorFactory
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        private void RecordRead(TransactionInfo info, GrainVersion readVersion, GrainVersion stableVersion)
        {
            var thisRef = this.AsReference<ITransactionalGrain>();
            if (readVersion.TransactionId == info.TransactionId)
            {
                // Just reading our own write here.
                // Sanity check to see if there's a lost write.
                if (info.WriteSet.ContainsKey(thisRef) && info.WriteSet[thisRef] > readVersion.WriteNumber)
                {
                    // Context has record of more writes than we have, some writes must be lost.
                    throw new OrleansTransactionAbortedException(info.TransactionId, "Lost Write");
                }
            }
            else
            {
                if (info.ReadSet.ContainsKey(thisRef) && info.ReadSet[thisRef] != readVersion)
                {
                    // Uh-oh. Read two different versions of the grain.
                    throw new OrleansValidationFailedException(info.TransactionId);
                }

                info.ReadSet[thisRef] = readVersion;

                if (readVersion.TransactionId != info.TransactionId && readVersion.TransactionId > stableVersion.TransactionId)
                {
                    info.DependentTransactions.Add(readVersion.TransactionId);
                }
            }
        }

        private void RecordWrite(TransactionInfo info, GrainVersion latestVersion, GrainVersion stableVersion)
        {
            var thisRef = this.AsReference<ITransactionalGrain>();
            if (!info.WriteSet.ContainsKey(thisRef))
            {
                info.WriteSet.Add(thisRef, 0);
            }
            info.WriteSet[thisRef]++;

            if (latestVersion.TransactionId != info.TransactionId && latestVersion > stableVersion)
            {
                info.DependentTransactions.Add(latestVersion.TransactionId);
            }
        }

        #endregion

        #region transaction management

        public class LogRecord
        {
            // The id of the last version written by this transaction
            public GrainVersion Version { get; set; }

            // the last state of this transaction; is constructed lazily, i.e. mabye null if not used
            public TState State { get; set; }
        }

        // cached states for transactions.
        private readonly Dictionary<long, TState> cachedStates = new Dictionary<long, TState>();

        // bound for writing - do not accept updates from transactions whose id is lower than this
        private long writeLowerBound;

        // this is called on grain activation
        // after adaptor is initialized, but before user-defined OnActivate is executed
        private async Task Recover()
        {
            // read current contents of storage
            await storageAdaptor.Synchronize();

            Restore();
        }

        /// <summary>
        /// Check with the transaction agent and rollback all events belonging to aborted transactions.
        /// </summary>
        private void Restore()
        {
            var lstate = storageAdaptor.TentativeView;
            var agent = services.TransactionAgent;
            int undocount = 0;

            for (int pos = lstate.Active.Count - 1; pos >= 0; pos--)
            {
                if (agent.IsAborted(lstate.Active[pos].Version.TransactionId))
                {
                    undocount++;
                }
                else
                {
                    // if this write belongs to committed transaction, all earlier writes must 
                    // also belong to committed transactions
                    break; 
                }
            }

            if (undocount > 0)
                storageAdaptor.Submit(new LUndoEvent() { Count = undocount });
        }

        /// <inheritdoc/>
        public async Task<bool> Prepare(long transactionId, GrainVersion? writeVersion, GrainVersion? readVersion)
        {
            // we no longer need to cache a state for this transaction
            if (this.cachedStates.ContainsKey(transactionId))
                this.cachedStates.Remove(transactionId);

            var state = storageAdaptor.TentativeView;

            if (!ValidateWrite(state, writeVersion))
                return false;

            if (!ValidateRead(state, transactionId, readVersion))
                return false;        

            // wait for all the pending levents to reach persistent storage
            await storageAdaptor.ConfirmSubmittedEntries();

            return true;
        }

        private bool ValidateWrite(LState state, GrainVersion? version)
        {
            if (!version.HasValue)
                return true;

            // validate that the last write by this tx matches the version
            var lastWriteByThisTx = state.Active.LastOrDefault(updateEvent =>
                                    updateEvent.Version.TransactionId == version.Value.TransactionId);
            return (lastWriteByThisTx != null && lastWriteByThisTx.Version.Equals(version));       
        }

        private bool ValidateRead(LState state, long transactionId, GrainVersion? version)
        {
            if (!version.HasValue)
                return true;

            // validate that the last version written by any
            // transaction t (where t less or equal to transactionId) 
            // matches version

            var last = state.StableVersion;
            foreach(var updateEvent in state.Active)
            {
                if (updateEvent.Version < version)
                    last = updateEvent.Version;
                else
                    break;
            }

            return last.Equals(version);
        }

        /// <inheritdoc/>
        public Task Abort(long transactionId)
        {
            var state = storageAdaptor.TentativeView;

            // look for first update by the aborted transaction
            int pos = state.Active.Count - 1;
            while (pos >= 0 && state.Active[pos].Version.TransactionId >= transactionId)
            {
                pos--;
            }

            // if we found an entry, undo the entire suffix
            if (pos >= 0 && state.Active[pos].Version.TransactionId == transactionId)
            {
                storageAdaptor.Submit(new LUndoEvent() { Count = state.Active.Count - pos });
            }

            return TaskDone.Done; // no need to wait for persistence
        }

        /// <inheritdoc/>
        public Task Commit(long transactionId)
        {
            var state = storageAdaptor.TentativeView;

            // look for last update by the committed transaction
            int pos = 0;
            while (pos < state.Active.Count && state.Active[pos].Version.TransactionId <= transactionId)
            {
                pos--;
            }

            // if we found an entry, commit the entire prefix
            if (pos < state.Active.Count && state.Active[pos].Version.TransactionId == transactionId)
            {
                storageAdaptor.Submit(new LStabilizeEvent() { Count = pos + 1 });
            }

            return TaskDone.Done; // no need to wait for persistence
        }

        #endregion

        #region adaptor plumbing

        private ILogConsistencyProtocolServices services;

        /// <summary>
        /// Called right after grain is constructed, to install the adaptor.
        /// The log-consistency provider contains a factory method that constructs the adaptor with chosen types for this grain
        /// </summary>
        void ILogConsistentGrain.InstallAdaptor(ILogViewAdaptorFactory factory, object initialState, string graintypename, IStorageProvider storageProvider, ILogConsistencyProtocolServices services)
        {
            // call the log consistency provider to construct the adaptor, passing the type argument
            storageAdaptor = factory.MakeLogViewAdaptor<LState, LEvent>(this, new LState() { StableState = (TState) initialState }, graintypename, storageProvider, services);
        }

        /// <summary>
        /// called by adaptor to update LView given LEvent
        /// </summary>
        /// <param name="view">log view</param>
        /// <param name="entry">log entry</param>
        void ILogViewAdaptorHost<LState, LEvent>.UpdateView(LState view, LEvent entry)
        {
            entry.Update(view, this); // pass this, so stabilize has access to user-specified transition function
        }

        /// <summary>
        /// Notify log view adaptor of activation (called before user-level OnActivate)
        /// </summary>
        async Task ILogConsistencyProtocolParticipant.PreActivateProtocolParticipant()
        {
            await storageAdaptor.PreOnActivate();
            await Recover();
        }

        /// <summary>
        /// Notify log view adaptor of activation (called after user-level OnActivate)
        /// </summary>
        Task ILogConsistencyProtocolParticipant.PostActivateProtocolParticipant()
        {
            return storageAdaptor.PostOnActivate();
        }

        /// <summary>
        /// Notify log view adaptor of deactivation
        /// </summary>
        Task ILogConsistencyProtocolParticipant.DeactivateProtocolParticipant()
        {
            return storageAdaptor.PostOnDeactivate();
        }

        /// <summary>
        /// Receive a protocol message from other clusters, passed on to log view adaptor.
        /// </summary>
        [AlwaysInterleave]
        Task<ILogConsistencyProtocolMessage> ILogConsistencyProtocolParticipant.OnProtocolMessageReceived(ILogConsistencyProtocolMessage payload)
        {
            return storageAdaptor.OnProtocolMessageReceived(payload);
        }

        /// <summary>
        /// Receive a configuration change, pass on to log view adaptor.
        /// </summary>
        [AlwaysInterleave]
        Task ILogConsistencyProtocolParticipant.OnMultiClusterConfigurationChange(MultiCluster.MultiClusterConfiguration next)
        {
            return storageAdaptor.OnMultiClusterConfigurationChange(next);
        }

        /// <summary>
        /// called by adaptor on state change. 
        /// </summary>
        void ILogViewAdaptorHost<LState, LEvent>.OnViewChanged(bool tentative, bool confirmed)
        {
            // currently we ignore all notifications... 
            // we will possibly revisit when designing geo-replicated transaction algorithms
        }

        /// <summary>
        /// called by adaptor on connection issues. 
        /// </summary>
        void IConnectionIssueListener.OnConnectionIssue(ConnectionIssue connectionIssue)
        {
            OnConnectionIssue(connectionIssue);
        }

        /// <summary>
        /// called by adaptor when a connection issue is resolved. 
        /// </summary>
        void IConnectionIssueListener.OnConnectionIssueResolved(ConnectionIssue connectionIssue)
        {
            OnConnectionIssueResolved(connectionIssue);
        }

        #endregion

        #region methods that are optionally overridden by user


        /// <summary>
        /// Called when the underlying persistence or replication protocol is running into some sort of connection trouble.
        /// <para>Override this to monitor the health of the log-consistency protocol and/or
        /// to customize retry delays.
        /// Any exceptions thrown are caught and logged by the <see cref="ILogConsistencyProvider"/>.</para>
        /// </summary>
        /// <returns>The time to wait before retrying</returns>
        protected virtual void OnConnectionIssue(ConnectionIssue issue)
        {
        }

        /// <summary>
        /// Called when a previously reported connection issue has been resolved.
        /// <para>Override this to monitor the health of the log-consistency protocol. 
        /// Any exceptions thrown are caught and logged by the <see cref="ILogConsistencyProvider"/>.</para>
        /// </summary>
        protected virtual void OnConnectionIssueResolved(ConnectionIssue issue)
        {
        }

        /// <summary>
        /// Defines how to apply events to the state. Unless it is overridden in the subclass, it calls
        /// a dynamic "Apply" function on the state, with the event as a parameter.
        /// All exceptions thrown by this method are caught and logged by the log view provider.
        /// <para>Override this to customize how to transition the state for a given event.</para>
        /// </summary>
        /// <param name="state"></param>
        /// <param name="event"></param>
        protected virtual void TransitionState(TState state, TEvent @event)
        {
            dynamic s = state;
            dynamic e = @event;
            s.Apply(e);
        }

        #endregion
    }
}
