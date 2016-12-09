using System.Threading.Tasks;
using Orleans.Concurrency;
using Orleans.Runtime;
using Orleans.MultiCluster;
using System;
using System.Collections.Generic;
using Orleans.Core;
using Orleans.LogConsistency;
using Orleans.Storage;

namespace Orleans.EventSourcing
{

    /// <summary>
    /// Queued grain base class.
    /// <typeparam name="TState">The type for the state of this grain.</typeparam>
    /// <typeparam name="TDelta">The type for objects that represent updates to the state.</typeparam>
    /// </summary>
    public abstract class QueuedGrain<TState,TDelta> : 
        LogConsistentGrainBase<TState>,
        ILogConsistentGrain, 
        IProtocolParticipant,
        ILogViewAdaptorHost<TState, TDelta>
        where TState : class,new()
        where TDelta : class
    {
        protected QueuedGrain()
        { }

        /// <summary>
        /// Grain implementers do NOT have to expose this constructor but can choose to do so.
        /// This constructor is particularly useful for unit testing where test code can create a Grain and replace
        /// the IGrainIdentity and IGrainRuntime with test doubles (mocks/stubs).
        /// </summary>
        /// <param name="identity"></param>
        /// <param name="runtime"></param>
        protected QueuedGrain(IGrainIdentity identity, IGrainRuntime runtime) : base(identity, runtime)
        { }

        /// The object encapsulating the log view provider functionality and local state
        /// (similar to <see cref="GrainStateStorageBridge"/> for storage providers)
        internal ILogViewAdaptor<TState,TDelta> Adaptor { get; private set; }


        /// <summary>
        /// Called right after grain is constructed, to install the log view adaptor.
        /// The log view provider contains a factory method that constructs the adaptor with chosen types for this grain
        /// </summary>
        void ILogConsistentGrain.InstallAdaptor(ILogViewAdaptorFactory provider, object initialstate, string graintypename, IStorageProvider storageProvider, IProtocolServices services)
        {
            // call the log view provider to construct the adaptor, passing the type argument
            Adaptor = provider.MakeLogViewAdaptor<TState,TDelta>(this, (TState) initialstate, graintypename, storageProvider, services);            
        }
        ILogViewAdaptorFactory ILogConsistentGrain.DefaultAdaptorFactory
        {
            get { return new VersionedStateStorage.DefaultAdaptorFactory(); }
        }

        /// <summary>
        /// called by adaptor to update the view when entries are appended.
        /// </summary>
        /// <param name="view">log view</param>
        /// <param name="entry">log entry</param>
        void ILogViewAdaptorHost<TState, TDelta>.UpdateView(TState view, TDelta entry)
        {
            ApplyDeltaToState(view, entry);
        }

        /// <summary>
        /// called by adaptor on state change. 
        /// </summary>
        void ILogViewAdaptorHost<TState, TDelta>.OnViewChanged(bool TentativeStateChanged, bool ConfirmedStateChanged)
        {
            if (TentativeStateChanged)
                OnTentativeStateChanged();
            if (ConfirmedStateChanged)
                OnConfirmedStateChanged();
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

        /// <summary>
        /// Notify log view adaptor of activation
        /// </summary>
        public Task ActivateProtocolParticipant()
        {
            return Adaptor.Activate();
        }

        /// <summary>
        /// Notify log view adaptor of deactivation
        /// </summary>
        public Task DeactivateProtocolParticipant()
        {
            return Adaptor.Deactivate();
        }


        /// <summary>
        /// Receive a protocol message from other clusters, passed on to log view adaptor.
        /// </summary>
        [AlwaysInterleave]
        Task<IProtocolMessage> IProtocolParticipant.OnProtocolMessageReceived(IProtocolMessage payload)
        {
            return Adaptor.OnProtocolMessageReceived(payload);
        }

        /// <summary>
        /// Receive a configuration change, pass on to log view adaptor.
        /// </summary>
        [AlwaysInterleave]
        Task IProtocolParticipant.OnMultiClusterConfigurationChange(MultiCluster.MultiClusterConfiguration next)
        {
            return Adaptor.OnMultiClusterConfigurationChange(next);
        }

        #region callbacks from storage interface

        // these methods are protected because they get called from inside the class only

        /// <summary>
        /// Subclasses must implement this method to define how deltas are applied to states.
        /// </summary>
        /// <param name="state">The state to mutate</param>
        /// <param name="delta">The update object to apply</param>
        /// <returns></returns>
        protected abstract void ApplyDeltaToState(TState state, TDelta delta);

        /// <summary>
        /// Called after the tentative state may have changed due to local or remote events.
        /// <para>Override this to react to changes of the tentative state.</para>
        /// </summary>
        protected virtual void OnTentativeStateChanged()
        {
        }

        /// <summary>
        /// Called after the confirmed state may have changed (i.e. the confirmed version increased).
        /// <para>Override this to react to changes of the confirmed state.</para>
        /// </summary>
        protected virtual void OnConfirmedStateChanged()
        {
        }

        /// <summary>
        /// Called when the underlying persistence or replication protocol is running into some sort of connection trouble.
        /// <para>Subclasses can override, to monitor the health of the persistence or replication algorithm and/or
        /// to customize retry delays.
        /// Any exceptions thrown are caught and logged by the <see cref="ILogConsistencyProvider"/>.</para>
        /// </summary>
        /// <returns>The time to wait before retrying</returns>
        protected virtual void OnConnectionIssue(ConnectionIssue issue)
        {
        }

        /// <summary>
        /// Called when a previously reported connection issue has been resolved.
        /// <para>Subclasses can override, to monitor the health of the persistence or replication algorithm. 
        /// Any exceptions thrown will be caught and logged by the <see cref="ILogConsistencyProvider"/>.</para>
        /// </summary>
        protected virtual void OnConnectionIssueResolved(ConnectionIssue issue)
        {
        }

        #endregion

        #region storage interface exposed to user grain code

        // these methods are protected because the user should call them only from within the grain,
        // not directly from other grains. 

        /// <summary>
        /// Retrieve the current, latest global version of the state, and confirm all updates currently in the queue. 
        /// <returns>A task that can be waited on.</returns>
        /// </summary>
        public Task SynchronizeNowAsync()
        {
            return Adaptor.SynchronizeNowAsync();
        }

        /// <summary>
        /// Enqueues an update.
        /// The update becomes visible in (TentativeState) immediately,
        /// and in (ConfirmedState) after it is confirmed, which happens automatically in the background.
        /// <param name="update">An object representing the update</param>
        /// </summary>
        public void EnqueueUpdate(TDelta update)
        {
            Adaptor.Submit(update);
        }


        /// <summary>
        /// Enqueues multiple updates, as an atomic sequence.
        /// The updates becomes visible in (TentativeState) immediately,
        /// and in (ConfirmedState) after they are confirmed, which happens automatically in the background.
        /// <param name="updates">A sequence of objects representing the updates</param>
        /// </summary>
        public void EnqueueUpdateSequence(IEnumerable<TDelta> updates)
        {
            Adaptor.SubmitRange(updates);
        }


        /// <summary>
        /// Perform a conditional update. The update fails if other updates modify the state first.
        /// <param name="update">An object representing the update</param>
        /// <returns>true if the update was successful, and false if the update failed due to conflicts.</returns>
        /// </summary>
        public Task<bool> TryConditionalUpdateAsync(TDelta update)
        {
           return Adaptor.TryAppend(update);
        }

        /// <summary>
        /// Perform a sequence of updates, conditionally and atomically. Fails if other updates modify the state first.
        /// <param name="updates">A sequence of objects representing the updates</param>
        /// <returns>true if the update was successful, and false if the update failed due to conflicts.</returns>
        /// </summary>
        public Task<bool> TryConditionalUpdateSequenceAsync(IEnumerable<TDelta> updates)
        {
            return Adaptor.TryAppendRange(updates);
        }

        /// <summary>
        /// Returns the current queue of unconfirmed updates.
        /// </summary>
        public IEnumerable<TDelta> UnconfirmedUpdates
        { 
           get { return Adaptor.UnconfirmedSuffix; } 
        }

        /// <summary>
        /// Waits until all updates currently in the queue are confirmed.
        /// </summary>
        /// <returns>A task that can be waited on.</returns>
        public Task ConfirmUpdates()
        {
            return Adaptor.ConfirmSubmittedEntriesAsync();
        }


        /// <summary>
        /// The tentative state of this grain (read-only).
        /// This state is equivalent to (ConfirmedState) with all the updates in (UnconfirmedUpdates) applied on top.
        /// </summary>
        public TState TentativeState
        {
            get { return Adaptor.TentativeView; }
        }

        /// <summary>
        /// The last confirmed snapshot of the global state (read-only).
        /// Does not include the effect of the updates in (UnconfirmedUpdates).
        /// </summary>
        public TState ConfirmedState
        {
            get { return Adaptor.ConfirmedView; }
        }

        /// <summary>
        /// The version of the last confirmed snapshot of the global state.
        /// </summary>
        public int ConfirmedVersion
        {
            get { return Adaptor.ConfirmedVersion; }
        }

        /// <inheritdoc cref="ILogConsistencyDiagnostics.UnresolvedConnectionIssues"/>
        protected IEnumerable<ConnectionIssue> UnresolvedConnectionIssues
        {
            get
            {
                return Adaptor.UnresolvedConnectionIssues;
            }
        }

        /// <inheritdoc cref="ILogConsistencyDiagnostics.EnableStatsCollection"/>
        public void EnableStatsCollection()
        {
            Adaptor.EnableStatsCollection();
        }

        /// <inheritdoc cref="ILogConsistencyDiagnostics.DisableStatsCollection"/>
        public void DisableStatsCollection()
        {
            Adaptor.DisableStatsCollection();
        }

        /// <inheritdoc cref="ILogConsistencyDiagnostics.GetStats"/>
        public LogConsistencyStatistics GetStats()
        {
            return Adaptor.GetStats();
        }

        #endregion


    }


}
