using System.Threading.Tasks;
using Orleans.Concurrency;
using Orleans.MultiCluster;
using System;
using System.Collections.Generic;
using Orleans.LogConsistency;
using Orleans.Core;
using Orleans.Storage;
using Orleans.Runtime;

namespace Orleans.EventSourcing
{

    /// <summary>
    /// Log View grain.
    /// <typeparam name="TView">The type for the log view, i.e. state of this grain.</typeparam>
    /// <typeparam name="TLogEntry">The type for log entries.</typeparam>
    /// </summary>
    public abstract class LogViewGrain<TView, TLogEntry> :
        LogConsistentGrainBase<TView>,
        ILogConsistentGrain,  
        IProtocolParticipant,
        ILogViewAdaptorHost<TView, TLogEntry>
        where TView : class, new()
        where TLogEntry : class
    {
        protected LogViewGrain()
        { }

        /// <summary>
        /// Grain implementers do NOT have to expose this constructor but can choose to do so.
        /// This constructor is particularly useful for unit testing where test code can create a Grain and replace
        /// the IGrainIdentity and IGrainRuntime with test doubles (mocks/stubs).
        /// </summary>
        /// <param name="identity"></param>
        /// <param name="runtime"></param>
        protected LogViewGrain(IGrainIdentity identity, IGrainRuntime runtime) : base(identity, runtime)
        { }

        /// <summary>
        /// The object encapsulating the log view provider functionality and local state
        /// (similar to <see cref="GrainStateStorageBridge"/> for storage providers)
        /// </summary>
        internal ILogViewAdaptor<TView, TLogEntry> Adaptor { get; private set; }

        /// <summary>
        /// Called right after grain is constructed, to install the log view adaptor.
        /// The log view provider contains a factory method that constructs the adaptor with chosen types for this grain
        /// </summary>
        void ILogConsistentGrain.InstallAdaptor(ILogViewAdaptorFactory factory, object initialstate, string graintypename, IStorageProvider storageProvider, IProtocolServices services)
        {
            // call the log consistency provider to construct the adaptor, passing the type argument
            Adaptor = factory.MakeLogViewAdaptor<TView, TLogEntry>(this, (TView)initialstate, graintypename, storageProvider, services);
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
        void ILogViewAdaptorHost<TView, TLogEntry>.UpdateView(TView view, TLogEntry entry)
        {
            UpdateView(view, entry);
        }

        /// <summary>
        /// called by adaptor on state change. 
        /// </summary>
        void ILogViewAdaptorHost<TView, TLogEntry>.OnViewChanged(bool tentativeViewChanged, bool confirmedViewChanged)
        {
            if (tentativeViewChanged)
                OnTentativeViewChanged();
            if (confirmedViewChanged)
                OnConfirmedViewChanged();
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
        /// Subclasses must implement this method to define how the view is updated when entries are appended. 
        /// Any exceptions thrown are caught and logged by the <see cref="ILogConsistencyProvider"/>.
        /// </summary>
        /// <param name="view">The view to mutate</param>
        /// <param name="entry">The entry to apply</param>
        /// <returns></returns>
        protected abstract void UpdateView(TView view, TLogEntry entry);

        /// <summary>
        /// Called after the tentative view may have changed due to entries being appended.
        /// <para>Subclasses can implement this to react to changes of the tentative view. 
        /// Any exceptions thrown are caught and logged by the <see cref="ILogConsistencyProvider"/>.</para>
        /// </summary>
        protected virtual void OnTentativeViewChanged()
        {
        }

        /// <summary>
        /// Called after the confirmed view may have changed (i.e. the confirmed version increased).
        /// <para>Subclasses can implement this to react to changes of the confirmed view.
        /// Any exceptions thrown are caught and logged by the <see cref="ILogConsistencyProvider"/>.</para>
        /// </summary>
        protected virtual void OnConfirmedViewChanged()
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
        // These methods match what is defined in ILogViewStorageInterface{TLogView,TLogEntry}.
        // Unfortunately, we cannot simply inherit that interface because that only works for public members.

        /// <inheritdoc cref="ILogViewRead{TLogView,TLogEntry}.TentativeView"/>
        protected TView TentativeView
        {
            get { return Adaptor.TentativeView; }
        }

        /// <inheritdoc cref="ILogViewRead{TLogView,TLogEntry}.ConfirmedView"/>
        protected TView ConfirmedView
        {
            get { return Adaptor.ConfirmedView; }
        }

        /// <inheritdoc cref="ILogViewRead{TLogView,TLogEntry}.ConfirmedVersion"/>
        protected int ConfirmedVersion
        {
            get { return Adaptor.ConfirmedVersion; }
        }

        /// <inheritdoc cref="ILogViewRead{TLogView,TLogEntry}.UnconfirmedSuffix"/>
        protected IEnumerable<TLogEntry> UnconfirmedSuffix
        {
            get { return Adaptor.UnconfirmedSuffix; }
        }

        /// <inheritdoc cref="ILogViewUpdate{TLogEntry}.Submit(TLogEntry)"/>
        protected void Submit(TLogEntry entry)
        {
            Adaptor.Submit(entry);
        }

        /// <inheritdoc cref="ILogViewUpdate{TLogEntry}.SubmitRange(IEnumerable{TLogEntry})"/>
        protected void SubmitRange(IEnumerable<TLogEntry> entries)
        {
            Adaptor.SubmitRange(entries);
        }

        /// <inheritdoc cref="ILogViewUpdate{TLogEntry}.TryAppend(TLogEntry)"/>
        protected Task<bool> TryAppend(TLogEntry entry)
        {
            return Adaptor.TryAppend(entry);
        }

        /// <inheritdoc cref="ILogViewUpdate{TLogEntry}.TryAppendRange(IEnumerable{TLogEntry})"/>
        protected Task<bool> TryAppendRange(IEnumerable<TLogEntry> entries)
        {
            return Adaptor.TryAppendRange(entries);
        }

        /// <inheritdoc cref="ILogViewUpdate{TLogEntry}.ConfirmSubmittedEntriesAsync"/>
        protected Task ConfirmSubmittedEntriesAsync()
        {
            return Adaptor.ConfirmSubmittedEntriesAsync();
        }

        /// <inheritdoc cref="ILogViewUpdate{TLogEntry}.SynchronizeNowAsync"/>
        protected Task SynchronizeNowAsync()
        {
            return Adaptor.SynchronizeNowAsync();
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
        protected void EnableStatsCollection()
        {
            Adaptor.EnableStatsCollection();
        }

        /// <inheritdoc cref="ILogConsistencyDiagnostics.DisableStatsCollection"/>
        protected void DisableStatsCollection()
        {
            Adaptor.DisableStatsCollection();
        }

        /// <inheritdoc cref="ILogConsistencyDiagnostics.GetStats"/>
        protected LogConsistencyStatistics GetStats()
        {
            return Adaptor.GetStats();
        }

 
        #endregion
    }
}
