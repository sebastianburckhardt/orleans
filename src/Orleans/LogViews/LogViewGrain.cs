using System.Threading.Tasks;
using Orleans.Concurrency;
using Orleans.MultiCluster;
using System;
using System.Collections.Generic;
using Orleans.LogViews;

namespace Orleans
{

    /// <summary>
    /// Log view grain.
    /// <typeparam name="TView">The type for the log view, i.e. state of this grain.</typeparam>
    /// <typeparam name="TLogEntry">The type for log entries.</typeparam>
    /// </summary>
    public abstract class LogViewGrain<TView, TLogEntry> :
        LogViewGrainBase<TView>,
        ILogViewGrain,  
        IProtocolParticipant,
        ILogViewHost<TView, TLogEntry>
        where TView : class, new()
        where TLogEntry : class
    {
        protected LogViewGrain()
        { }

        /// <summary>
        /// The object encapsulating the log view provider functionality and local state
        /// (similar to <see cref="StorageBridge"> for storage providers)
        /// </summary>
        internal ILogViewAdaptor<TView, TLogEntry> Adaptor { get; private set; }

        /// <summary>
        /// Called right after grain is constructed, to install the log view adaptor.
        /// The log view provider contains a factory method that constructs the adaptor with chosen types for this grain
        /// </summary>
        void ILogViewGrain.InstallAdaptor(ILogViewProvider provider, object initialstate, string graintypename, IProtocolServices services)
        {
            // call the log view provider to construct the adaptor, passing the type argument
            Adaptor = provider.MakeLogViewAdaptor<TView, TLogEntry>(this, (TView)initialstate, graintypename, services);
        }

        /// <summary>
        /// called by adaptor to update the view when entries are appended.
        /// </summary>
        /// <param name="view">log view</param>
        /// <param name="entry">log entry</param>
        void ILogViewHost<TView, TLogEntry>.UpdateView(TView view, TLogEntry entry)
        {
            UpdateView(view, entry);
        }

        /// <summary>
        /// called by adaptor to retrieve the identity of this grain, for tracing purposes.
        /// </summary>
        string ILogViewHost<TView, TLogEntry>.IdentityString
        {
            get { return Identity.IdentityString; }
        }

        /// <summary>
        /// called by adaptor on state change. 
        /// </summary>
        void ILogViewHost<TView, TLogEntry>.OnViewChanged(bool tentativeViewChanged, bool confirmedViewChanged)
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

        #region methods implemented by subclasses

        /// <summary>
        /// Subclasses must implement this method to define how the view is updated when entries are appended. 
        /// Any exceptions thrown are caught and logged by the <see cref="ILogViewProvider"/>.
        /// </summary>
        /// <param name="view">The view to mutate</param>
        /// <param name="entry">The entry to apply</param>
        /// <returns></returns>
        protected abstract void UpdateView(TView view, TLogEntry entry);

        /// <summary>
        /// Called after the tentative view may have changed due to entries being appended.
        /// <para>Subclasses can implement this to react to changes of the tentative view. 
        /// Any exceptions thrown are caught and logged by the <see cref="ILogViewProvider"/>.</para>
        /// </summary>
        protected virtual void OnTentativeViewChanged()
        {
        }

        /// <summary>
        /// Called after the confirmed view may have changed (i.e. the confirmed version increased).
        /// <para>Subclasses can implement this to react to changes of the confirmed view.
        /// Any exceptions thrown are caught and logged by the <see cref="ILogViewProvider"/>.</para>
        /// </summary>
        protected virtual void OnConfirmedViewChanged()
        {
        }

        /// <summary>
        /// Called when the underlying persistence or replication protocol is running into some sort of connection trouble.
        /// <para>Subclasses can override, to monitor the health of the persistence or replication algorithm and/or
        /// to customize retry delays.
        /// Any exceptions thrown are caught and logged by the <see cref="ILogViewProvider"/>.</para>
        /// </summary>
        /// <returns>The time to wait before retrying</returns>
        protected virtual void OnConnectionIssue(ConnectionIssue issue)
        {
        }

        /// <summary>
        /// Called when a previously reported connection issue has been resolved.
        /// <para>Subclasses can override, to monitor the health of the persistence or replication algorithm. 
        /// Any exceptions thrown will be caught and logged by the <see cref="ILogViewProvider"/>.</para>
        /// </summary>
        protected virtual void OnConnectionIssueResolved(ConnectionIssue issue)
        {
        }

        #endregion

        #region methods called by subclasses

        /// <summary>
        /// Local, tentative view of the log (reflecting both confirmed and unconfirmed entries)
        /// </summary>
        protected TView TentativeView
        {
            get { return Adaptor.TentativeView; }
        }

        /// <summary>
        /// Confirmed view of the log (reflecting only confirmed entries)
        /// </summary>
        protected TView ConfirmedView
        {
            get { return Adaptor.ConfirmedView; }
        }

        /// <summary>
        /// The length of the confirmed prefix of the log
        /// </summary>
        protected int ConfirmedVersion
        {
            get { return Adaptor.ConfirmedVersion; }
        }

        /// <summary>
        /// A list of the submitted entries that do not yet appear in the confirmed prefix.
        /// </summary>
        protected IEnumerable<TLogEntry> UnconfirmedSuffix
        {
            get { return Adaptor.UnconfirmedSuffix; }
        }


        /// <summary>
        /// Submit a single log entry to be appended to the global log,
        /// either at the current or at any later position.
        /// </summary>
        protected void Submit(TLogEntry entry)
        {
            Adaptor.Submit(entry);
        }

        /// <summary>
        /// Submit a range of log entries to be appended atomically to the global log,
        /// either at the current or at any later position.
        /// </summary>
        protected void SubmitRange(IEnumerable<TLogEntry> entries)
        {
            Adaptor.SubmitRange(entries);
        }

        /// <summary>
        /// Try to append a single log entry at the current position of the log.
        /// </summary>
        /// <returns>true if the entry was appended successfully, or false 
        /// if there was a concurrency conflict (i.e. some other entries were previously appended).
        /// </returns>
        protected Task<bool> TryAppend(TLogEntry entry)
        {
            return Adaptor.TryAppend(entry);
        }

        /// <summary>
        /// Try to append a range of log entries atomically at the current position of the log.
        /// </summary>
        /// <returns>true if the entries were appended successfully, or false 
        /// if there was a concurrency conflict (i.e. some other entries were previously appended).
        /// </returns>
        protected Task<bool> TryAppendRange(IEnumerable<TLogEntry> entries)
        {
            return Adaptor.TryAppendRange(entries);
        }

        /// <summary>
        /// Confirm all submitted entries.
        ///<para>Waits until all previously submitted entries appear in the confirmed prefix of the log.</para>
        /// </summary>
        protected Task ConfirmSubmittedEntriesAsync()
        {
            return Adaptor.ConfirmSubmittedEntriesAsync();
        }

        /// <summary>
        /// Confirm all submitted entries and get the latest log view.
        ///<para>Waits until all previously submitted entries appear in the confirmed prefix of the log, and forces a refresh of the confirmed prefix.</para>
        /// </summary>
        /// <returns></returns>
        protected Task SynchronizeNowAsync()
        {
            return Adaptor.SynchronizeNowAsync();
        }


        /// <summary>
        /// Returns the currently unresolved connection issues.
        /// </summary>
        protected IEnumerable<ConnectionIssue> UnresolvedConnectionIssues
        {
            get
            {
                return Adaptor.UnresolvedConnectionIssues;
            }
        }



        /// <summary>
        /// Enable statistics collection in the log view provider.
        /// </summary>
        protected void EnableStatsCollection()
        {
            Adaptor.EnableStatsCollection();
        }

        /// <summary>
        /// Disable statistics collection in the log view provider.
        /// </summary>
        protected void DisableStatsCollection()
        {
            Adaptor.DisableStatsCollection();
        }

        /// <summary>
        /// access internal statistics about the log view provider.
        /// </summary>
        /// <returns>an object containing statistics.</returns>
        protected LogViewStatistics GetStats()
        {
            return Adaptor.GetStats();
        }

 
        #endregion
    }
}
