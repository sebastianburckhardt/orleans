﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans.LogViews;
using System.Diagnostics;
using Orleans.Concurrency;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.MultiCluster;
using Orleans.GrainDirectory;

namespace Orleans.Runtime.LogViews
{
    /// <summary>
    /// A general template for constructing log view adaptors that are based on
    /// a sequentially read and written primary. We use this to construct 
    /// a variety of different log view providers, all following the same basic pattern 
    /// (read and write latest view from/to primary, and send notifications after writing).
    ///<para>
    /// Note that the log itself is transient, i.e. not actually saved to storage - only the latest view and some 
    /// metadata (the log position, and write flags) is stored in the primary. 
    /// It is safe to interleave calls to this adaptor (using grain scheduler only, of course).
    /// </para>
    ///<para>
    /// Subclasses override ReadAsync and WriteAsync to read from / write to primary.
    /// Calls to the primary are serialized, i.e. never interleave.
    /// </para>
    /// </summary>
    /// <typeparam name="TLogView">The user-defined view of the log</typeparam>
    /// <typeparam name="TLogEntry">The type of the log entries</typeparam>
    /// <typeparam name="TSubmissionEntry">The type of submission entries stored in pending queue</typeparam>
    public abstract class PrimaryBasedLogViewAdaptor<TLogView, TLogEntry, TSubmissionEntry> : ILogViewAdaptor<TLogView, TLogEntry>
    where TLogView : class, new()
        where TLogEntry : class
        where TSubmissionEntry : SubmissionEntry<TLogEntry>
    {

        #region interface to subclasses that implement specific providers


        /// <summary>
        /// Set confirmed view the initial value (a view of the empty log)
        /// </summary>
        protected abstract void InitializeConfirmedView(TLogView initialstate);

        /// <summary>
        /// Read cached global state.
        /// </summary>
        protected abstract TLogView LastConfirmedView();

        /// <summary>
        /// Read version of cached global state.
        /// </summary>
        protected abstract int GetConfirmedVersion();

        /// <summary>
        /// Read the latest primary state. Must block/retry until successful.
        /// </summary>
        /// <returns></returns>
        protected abstract Task ReadAsync();

        /// <summary>
        /// Apply pending entries to the primary. Must block/retry until successful. 
        /// </summary>
        protected abstract Task<int> WriteAsync();

        /// <summary>
        /// Create a submission entry for the submitted log entry. 
        /// Using a type parameter so we can add protocol-specific info to this class.
        /// </summary>
        /// <returns></returns>
        protected abstract TSubmissionEntry MakeSubmissionEntry(TLogEntry entry);

        /// <summary>
        /// Whether this cluster supports submitting updates
        /// </summary>
        protected virtual bool SupportSubmissions {  get { return true;  } }

        /// <summary>
        /// Handle protocol messages.
        /// </summary>
        protected virtual Task<IProtocolMessage> OnMessageReceived(IProtocolMessage payload)
        {
            // subclasses that define custom protocol messages must override this
            throw new NotImplementedException();
        }

        /// <summary>
        /// Handle notification messages. Override this to handle notification subtypes.
        /// </summary>
        protected virtual void OnNotificationReceived(INotificationMessage payload)
        {        
            var msg = payload as VersionNotificationMessage; 
            if (msg != null)
            {
                if (msg.Version > lastVersionNotified)
                    lastVersionNotified = msg.Version;
                return;
            }

            var batchmsg = payload as BatchedNotificationMessage;
            if (batchmsg != null)
            {
                foreach (var bm in batchmsg.Notifications)
                    OnNotificationReceived(bm);
                return;
            }

            // subclass should have handled this in override
            throw new ProtocolTransportException(string.Format("message type {0} not handled by OnNotificationReceived", payload.GetType().FullName));
        }

        private int lastVersionNotified;

        /// <summary>
        /// Process stored notifications during worker cycle. Override to handle notification subtypes.
        /// </summary>
        protected virtual void ProcessNotifications()
        {
            if (lastVersionNotified > this.GetConfirmedVersion())
            {
                Services.Verbose("force refresh because of version notification v{0}", lastVersionNotified);
                needRefresh = true;
            }
        }

        /// <summary>
        /// Merge two notification messages, for batching. Override to handle notification subtypes.
        /// </summary>
        protected virtual INotificationMessage Merge(INotificationMessage earliermessage, INotificationMessage latermessage)
        {
            return new VersionNotificationMessage()
            {
                Version = latermessage.Version
            };
        }

        /// <summary>
        /// Called when configuration of the multicluster is changing.
        /// </summary>
        protected virtual Task OnConfigurationChange(MultiClusterConfiguration next)
        {
            Configuration = next;
            return TaskDone.Done;
        }

        /// <summary>
        /// The grain that is using this adaptor
        /// </summary>
        protected ILogViewHost<TLogView, TLogEntry> Host { get; private set; }

        protected IProtocolServices Services { get; private set; }

        protected MultiClusterConfiguration Configuration { get; set; }

        protected ILogViewProvider Provider;

        /// <summary>
        /// Tracks notifications sent. Created lazily since many copies will never need to send notifications.
        /// </summary>
        private NotificationTracker notificationTracker;


        private const int max_notification_batch_size = 10000;


        protected PrimaryBasedLogViewAdaptor(ILogViewHost<TLogView, TLogEntry> host, ILogViewProvider provider,
            TLogView initialstate, IProtocolServices services)
        {
            Debug.Assert(host != null && services != null && initialstate != null);
            this.Host = host;
            this.Services = services;
            this.Provider = provider;
            InitializeConfirmedView(initialstate);
            worker = new BatchWorkerFromDelegate(() => Work());
        }

        public virtual async Task Activate()
        {
            Services.Verbose2("Activation Started");

            if (Silo.CurrentSilo.GlobalConfig.HasMultiClusterNetwork)
            {
                // subscribe this grain to configuration change events
                Silo.CurrentSilo.LocalMultiClusterOracle.SubscribeToMultiClusterConfigurationEvents(Services.GrainReference);
            }

            // initial load happens async
            KickOffInitialRead().Ignore();

            var latestconf = Services.MultiClusterConfiguration;
            if (latestconf != null)
                await OnMultiClusterConfigurationChange(latestconf);

            Services.Verbose2("Activation Complete");
        }

        private async Task KickOffInitialRead()
        {
            needInitialRead = true;
            // kick off notification for initial read cycle with a bit of delay
            // so that we don't do this several times if user does strong sync
            await Task.Delay(10);
            Services.Verbose2("Notify (initial read)");
            worker.Notify();
        }

        public virtual async Task Deactivate()
        {
            Services.Verbose2("Deactivation Started");

            while (!worker.IsIdle())
            {
                await worker.WaitForCurrentWorkToBeServiced();
            }

            if (Silo.CurrentSilo.GlobalConfig.HasMultiClusterNetwork)
            {
                // unsubscribe this grain from configuration change events
                Silo.CurrentSilo.LocalMultiClusterOracle.UnSubscribeFromMultiClusterConfigurationEvents(Services.GrainReference);
            }

            Services.Verbose2("Deactivation Complete");
        }



        #endregion

        // the currently submitted, unconfirmed entries. 
        private readonly List<TSubmissionEntry> pending = new List<TSubmissionEntry>();


        /// called at beginning of WriteAsync to the current tentative state
        protected TLogView CopyTentativeState()
        {
            var state = TentativeView;
            tentativeStateInternal = null; // to avoid aliasing
            return state;
        }
        /// called at beginning of WriteAsync to the current batch of updates
        protected TSubmissionEntry[] GetCurrentBatchOfUpdates()
        {
            return pending.ToArray(); // must use a copy
        }
        /// called at beginning of WriteAsync to get current number of pending updates
        protected int GetNumberPendingUpdates()
        {
            return pending.Count;
        }

        /// <summary>
        ///  Tentative State. Represents Stable State + effects of pending updates.
        ///  Computed lazily (null if not in use)
        /// </summary>
        private TLogView tentativeStateInternal;

        /// <summary>
        /// A flag that indicates to the worker that the client wants to refresh the state
        /// </summary>
        private bool needRefresh;

        /// <summary>
        /// A flag that indicates that we have not read global state at all yet, and should do so
        /// </summary>
        private bool needInitialRead;

        /// <summary>
        /// Background worker which asynchronously sends operations to the leader
        /// </summary>
        private BatchWorker worker;




        /// statistics gathering. Is null unless stats collection is turned on.
        protected LogViewStatistics stats = null;


        /// For use by protocols. Determines if this cluster is part of the configured multicluster.
        protected bool IsMyClusterJoined()
        {
            return (Configuration != null && Configuration.Clusters.Contains(Services.MyClusterId));
        }

        /// <summary>
        /// Block until this cluster is joined to the multicluster.
        /// </summary>
        protected async Task EnsureClusterJoinedAsync()
        {
            while (!IsMyClusterJoined())
            {
                Services.Verbose("Waiting for join");
                await Task.Delay(5000);
            }
        }
        /// <summary>
        /// Wait until this cluster has received a configuration that is at least as new as timestamp
        /// </summary>
        protected async Task GetCaughtUpWithConfigurationAsync(DateTime adminTimestamp)
        {
            while (Configuration == null || Configuration.AdminTimestamp < adminTimestamp)
            {
                Services.Verbose("Waiting for config {0}", adminTimestamp);

                await Task.Delay(5000);
            }
        }



        #region Interface

        /// <inheritdoc />
        public void Submit(TLogEntry logEntry)
        {
            if (!SupportSubmissions)
                throw new InvalidOperationException("provider does not support submissions on cluster " + Services.MyClusterId);

            if (stats != null) stats.EventCounters["SubmitCalled"]++;

            Services.Verbose2("Submit");

            SubmitInternal(DateTime.UtcNow, logEntry);

            worker.Notify();
        }

        /// <inheritdoc />
        public void SubmitRange(IEnumerable<TLogEntry> logEntries)
        {
            if (!SupportSubmissions)
                throw new InvalidOperationException("Provider does not support submissions on cluster " + Services.MyClusterId);

            if (stats != null) stats.EventCounters["SubmitRangeCalled"]++;

            Services.Verbose2("SubmitRange");

            var time = DateTime.UtcNow;

            foreach (var e in logEntries)
                SubmitInternal(time, e);

            worker.Notify();
        }

        /// <inheritdoc />
        public Task<bool> TryAppend(TLogEntry logEntry)
        {
            if (!SupportSubmissions)
                throw new InvalidOperationException("Provider does not support submissions on cluster " + Services.MyClusterId);

            if (stats != null) stats.EventCounters["TryAppendCalled"]++;

            Services.Verbose2("TryAppend");

            var promise = new TaskCompletionSource<bool>();

            SubmitInternal(DateTime.UtcNow, logEntry, GetConfirmedVersion() + pending.Count, promise);

            worker.Notify();

            return promise.Task;
        }

        /// <inheritdoc />
        public Task<bool> TryAppendRange(IEnumerable<TLogEntry> logEntries)
        {
            if (!SupportSubmissions)
                throw new InvalidOperationException("Provider does not support submissions on cluster " + Services.MyClusterId);

            if (stats != null) stats.EventCounters["TryAppendRangeCalled"]++;

            Services.Verbose2("TryAppendRange");

            var promise = new TaskCompletionSource<bool>();
            var time = DateTime.UtcNow;
            var pos = GetConfirmedVersion() + pending.Count;

            bool first = true;
            foreach (var e in logEntries)
            {
                SubmitInternal(time, e, pos++, first ? promise : null);
                first = false;
            }

            worker.Notify();

            return promise.Task;
        }


        private const int unconditional = -1;

        private void SubmitInternal(DateTime time, TLogEntry logentry, int conditionalPosition = unconditional, TaskCompletionSource<bool> resultPromise = null)
        {
            // create a submission entry
            var submissionentry = this.MakeSubmissionEntry(logentry);
            submissionentry.SubmissionTime = time;
            submissionentry.ResultPromise = resultPromise;
            submissionentry.ConditionalPosition = conditionalPosition;

            // add submission to queue
            pending.Add(submissionentry);

            // if we have a tentative state in use, update it
            if (this.tentativeStateInternal != null)
            {
                try
                {
                    Host.UpdateView(this.tentativeStateInternal, logentry);
                }
                catch (Exception e)
                {
                    Services.CaughtViewUpdateException("PrimaryBasedLogViewAdaptor.SubmitInternal", e);
                }
            }

            Host.OnViewChanged(true, false);
        }

        /// <inheritdoc />
        public TLogView TentativeView
        {
            get
            {
                if (stats != null)
                    stats.EventCounters["TentativeViewCalled"]++;

                if (tentativeStateInternal == null)
                    CalculateTentativeState();

                return tentativeStateInternal;
            }
        }

        /// <inheritdoc />
        public TLogView ConfirmedView
        {
            get
            {
                if (stats != null)
                    stats.EventCounters["ConfirmedViewCalled"]++;

                return LastConfirmedView();
            }
        }

        /// <inheritdoc />
        public int ConfirmedVersion
        {
            get
            {
                if (stats != null)
                    stats.EventCounters["ConfirmedVersionCalled"]++;

                return GetConfirmedVersion();
            }
        }

        /// <summary>
        /// Called from network
        /// </summary>
        /// <param name="payLoad"></param>
        /// <returns></returns>
        public async Task<IProtocolMessage> OnProtocolMessageReceived(IProtocolMessage payLoad)
        {
            var notificationMessage = payLoad as INotificationMessage;

            if (notificationMessage != null)
            {
                Services.Verbose("NotificationReceived v{0}", notificationMessage.Version);

                OnNotificationReceived(notificationMessage);

                // poke worker so it will process the notifications
                worker.Notify();

                return null;
            }
            else
            {
                //it's a protocol message
                return await OnMessageReceived(payLoad);
            }
        }


        /// <summary>
        /// Called by MultiClusterOracle when there is a configuration change.
        /// </summary>
        /// <returns></returns>
        public async Task OnMultiClusterConfigurationChange(MultiCluster.MultiClusterConfiguration newConfig)
        {
            Debug.Assert(newConfig != null);

            var oldConfig = Configuration;

            // process only if newer than what we already have
            if (!MultiClusterConfiguration.OlderThan(oldConfig, newConfig))
                return;

            Services.Verbose("Processing Configuration {0}", newConfig);

            await this.OnConfigurationChange(newConfig); // updates Configuration and does any work required

            var added = oldConfig == null ? newConfig.Clusters : newConfig.Clusters.Except(oldConfig.Clusters);

            // if the multi-cluster is operated correctly, this grain should not be active before we are joined to the multicluster
            // but if we detect that anyway here, enforce a refresh to reduce risk of missed notifications
            if (!needInitialRead && added.Contains(Services.MyClusterId))
            {
                needRefresh = true;
                Services.Verbose("Refresh Because of Join");
                worker.Notify();
            }

        }



        #endregion

        /// <summary>
        /// method is virtual so subclasses can add their own events
        /// </summary>
        public virtual void EnableStatsCollection()
        {

            stats = new LogViewStatistics()
            {
                EventCounters = new Dictionary<string, long>(),
                StabilizationLatenciesInMsecs = new List<int>()
            };

            stats.EventCounters.Add("TentativeViewCalled", 0);
            stats.EventCounters.Add("ConfirmedViewCalled", 0);
            stats.EventCounters.Add("ConfirmedVersionCalled", 0);
            stats.EventCounters.Add("SubmitCalled", 0);
            stats.EventCounters.Add("SubmitRangeCalled", 0);
            stats.EventCounters.Add("TryAppendCalled", 0);
            stats.EventCounters.Add("TryAppendRangeCalled", 0);
            stats.EventCounters.Add("ConfirmSubmittedEntriesCalled", 0);
            stats.EventCounters.Add("SynchronizeNowCalled", 0);

            stats.EventCounters.Add("WritebackEvents", 0);

            stats.StabilizationLatenciesInMsecs = new List<int>();
        }

        /// <summary>
        /// Disable stats collection
        /// </summary>
        public void DisableStatsCollection()
        {
            stats = null;
        }

        /// <summary>
        /// Get states
        /// </summary>
        /// <returns></returns>
        public LogViewStatistics GetStats()
        {
            return stats;
        }


        private void CalculateTentativeState()
        {
            // copy the master
            this.tentativeStateInternal = (TLogView)SerializationManager.DeepCopy(LastConfirmedView());

            // Now apply all operations in pending 
            foreach (var u in this.pending)
                try
                {
                    Host.UpdateView(this.tentativeStateInternal, u.Entry);
                }
                catch (Exception e)
                {
                    Services.CaughtViewUpdateException("PrimaryBasedLogViewAdaptor.CalculateTentativeState", e);
                }
        }


        /// <summary>
        /// batch worker performs reads from and writes to global state.
        /// only one work cycle is active at any time.
        /// </summary>
        internal async Task Work()
        {
            Services.Verbose("<1 ProcessNotifications");

            var version = GetConfirmedVersion();

            ProcessNotifications();

            Services.Verbose("<2 NotifyViewChanges");

            NotifyViewChanges(ref version);

            bool haveToWrite = (pending.Count != 0);

            bool haveToRead = needInitialRead || (needRefresh && !haveToWrite);

            Services.Verbose("<3 Storage htr={0} htw={1}", haveToRead, haveToWrite);

            try
            {
                if (haveToRead)
                {
                    needRefresh = needInitialRead = false; // retrieving fresh version

                    await ReadAsync();

                    NotifyViewChanges(ref version);
                }

                if (haveToWrite)
                {
                    needRefresh = needInitialRead = false; // retrieving fresh version

                    await UpdatePrimary();

                    if (stats != null) stats.EventCounters["WritebackEvents"]++;
                }

            }
            catch (Exception e)
            {
                // this should never happen - we are supposed to catch and store exceptions 
                // in the correct place (LastPrimaryException or notification trackers)
                Services.ProtocolError("WorkerCycle threw exception " + e, true);

            }

            Services.Verbose("<4 Done");
        }


        /// <summary>
        /// This function stores the operations in the pending queue as a batch to the primary.
        /// Retries until some batch commits or there are no updates left.
        /// </summary>
        internal async Task UpdatePrimary()
        {
            int version = GetConfirmedVersion();

            while (true)
            {
                try
                {
                    // find stale conditional updates, remove them, and notify waiters
                    RemoveStaleConditionalUpdates();

                    if (pending.Count == 0)
                        return; // no updates to write.

                    // try to write the updates as a batch
                    var writeResult = await WriteAsync();

                    NotifyViewChanges(ref version, writeResult);

                    // if the batch write failed due to conflicts, retry.
                    if (writeResult == 0)
                        continue;

                    Host.OnViewChanged(false, true);

                    // notify waiting promises of the success of conditional updates
                    NotifyPromises(writeResult, true);

                    // record stabilization time, for statistics
                    if (stats != null)
                    {
                        var timeNow = DateTime.UtcNow;
                        for (int i = 0; i < writeResult; i++)
                        {
                            var latency = timeNow - pending[i].SubmissionTime;
                            stats.StabilizationLatenciesInMsecs.Add(latency.Milliseconds);
                        }
                    }

                    // remove completed updates from queue
                    pending.RemoveRange(0, writeResult);

                    return;
                }
                catch (Exception e)
                {
                    LastPrimaryException = e; // store last exception for inspection by user code

                    // retry again
                    continue;
                }
            }
        }

        private void NotifyViewChanges(ref int version, int numWritten = 0)
        {
            var v = GetConfirmedVersion();
            bool tentativeChanged = (v != version + numWritten);
            bool confirmedChanged = (v != version);
            if (tentativeChanged || confirmedChanged)
            {
                tentativeStateInternal = null; // conservative.
                Host.OnViewChanged(tentativeChanged, confirmedChanged);
                version = v;
            }
        }

        /// <summary>
        /// returns last observed communication exception, or null if communication was successful.
        /// Exceptions are either observed while communicating with the primary, or while trying to 
        /// notify other clusters. The latter type of exception is reported only if there is not a
        /// exception of the former type.
        /// </summary>
        public Exception LastException
        {
            get
            {
                if (LastPrimaryException != null)
                    return LastPrimaryException;
                if (notificationTracker != null)
                    return notificationTracker.LastException;
                return null;
            }
        }

        /// <summary>
        /// Store the last exception that occurred while communicating with the primary.
        /// Is null if the last communication was successful.
        /// </summary>
        protected Exception LastPrimaryException;

        /// <inheritdoc />
        public async Task SynchronizeNowAsync()
        {
            if (stats != null)
                stats.EventCounters["SynchronizeNowCalled"]++;

            Services.Verbose("SynchronizeNowStart");

            needRefresh = true;
            await worker.NotifyAndWaitForWorkToBeServiced();

            Services.Verbose("SynchronizeNowComplete");
        }

        public IEnumerable<TLogEntry> UnconfirmedSuffix
        {
            get
            {
                return pending.Select(te => te.Entry);
            }
        }

        /// <inheritdoc />
        public async Task ConfirmSubmittedEntriesAsync()
        {
            if (stats != null)
                stats.EventCounters["ConfirmSubmittedEntriesCalled"]++;

            Services.Verbose("ConfirmSubmittedEntriesStart");

            if (pending.Count != 0)
                await worker.WaitForCurrentWorkToBeServiced();

            Services.Verbose("ConfirmSubmittedEntriesEnd");
        }

        /// <summary>
        /// send failure notifications
        /// </summary>
        protected void NotifyPromises(int count, bool success)
        {
            for (int i = 0; i < count; i++)
            {
                var promise = pending[i].ResultPromise;
                if (promise != null)
                    promise.SetResult(success);
            }
        }

        /// <summary>
        /// go through updates and remove all the conditional updates that have already failed
        /// </summary>
        protected void RemoveStaleConditionalUpdates()
        {
            int version = GetConfirmedVersion();
            bool foundFailedConditionalUpdates = false;

            for (int pos = 0; pos < pending.Count; pos++)
            {
                var submissionEntry = pending[pos];
                if (submissionEntry.ConditionalPosition != unconditional
                    && (foundFailedConditionalUpdates ||
                           submissionEntry.ConditionalPosition != (version + pos)))
                {
                    foundFailedConditionalUpdates = true;
                    if (submissionEntry.ResultPromise != null)
                        submissionEntry.ResultPromise.SetResult(false);
                }
                pos++;
            }

            if (foundFailedConditionalUpdates)
            {
                pending.RemoveAll(e => e.ConditionalPosition != unconditional);
                tentativeStateInternal = null;
                Host.OnViewChanged(true, false);
            }
        }

        protected void BroadcastNotification(INotificationMessage msg, string exclude = null)
        {
            // if there is only one cluster, or if we are global single instance, don't send notifications.
            if (Services.MultiClusterConfiguration.Clusters.Count == 1
                || Services.RegistrationStrategy != ClusterLocalRegistration.Singleton)
                return;

            // create notification tracker if we haven't already
            if (notificationTracker == null)
                notificationTracker = new NotificationTracker(this.Services, Configuration, max_notification_batch_size);

            notificationTracker.BroadcastNotification(msg, exclude);
        }
    }

    /// <summary>
    /// Base class for submission entries stored in pending queue. 
    /// </summary>
    /// <typeparam name="E"></typeparam>
    public class SubmissionEntry<E>
    {
        public E Entry;
        public DateTime SubmissionTime;
        public TaskCompletionSource<bool> ResultPromise;
        public int ConditionalPosition;
    }


}
