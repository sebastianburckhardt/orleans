using System;
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
    public abstract class PrimaryBasedLogViewAdaptor<TLogView,TLogEntry,TSubmissionEntry> : ILogViewAdaptor<TLogView,TLogEntry> 
    where TLogView : class,new() 
        where TLogEntry:class
        where TSubmissionEntry: SubmissionEntry<TLogEntry>
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
        /// Handle protocol messages.
        /// </summary>
        /// <param name="payload"></param>
        /// <returns></returns>
        protected virtual Task<IProtocolMessage> OnMessageReceived(IProtocolMessage payload)
        {
            return Task.FromResult<IProtocolMessage>(null);
        }

        /// <summary>
        /// Handle notification messages. Override to handle notification subtypes.
        /// </summary>
        /// <param name="payload"></param>
        /// <returns></returns>
        protected virtual void OnNotificationReceived(NotificationMessage payload)
        {
            var msg = (VersionNotificationMessage) payload; // override to handle additional types
            if (msg.Version > lastVersionNotified)
                lastVersionNotified = msg.Version;
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
                need_refresh = true;
            }
        }

        /// <summary>
        /// Merge two notification messages, for batching. Override to handle notification subtypes.
        /// </summary>
        protected virtual NotificationMessage Merge(NotificationMessage earliermessage, NotificationMessage latermessage)
        {
            return new VersionNotificationMessage() {
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
        private NotificationTracker notificationtracker;

      
        protected PrimaryBasedLogViewAdaptor(ILogViewHost<TLogView,TLogEntry> host, ILogViewProvider provider,
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
            need_initial_read = true;
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
            TentativeStateInternal = null; // to avoid aliasing
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
        private TLogView TentativeStateInternal;
     
        /// <summary>
        /// A flag that indicates to the worker that the client wants to refresh the state
        /// </summary>
        private bool need_refresh;

        /// <summary>
        /// A flag that indicates that we have not read global state at all yet, and should do so
        /// </summary>
        private bool need_initial_read;

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

        public void Submit(TLogEntry logentry)
        {
            if (stats != null) stats.EventCounters["SubmitCalled"]++;

            Services.Verbose2("Submit");

            SubmitInternal(DateTime.UtcNow, logentry);

            worker.Notify();
        }

        public void SubmitRange(IEnumerable<TLogEntry> logentries)
        {
            if (stats != null) stats.EventCounters["SubmitRangeCalled"]++;

            Services.Verbose2("SubmitRange");

            var time = DateTime.UtcNow;

            foreach (var e in logentries)
                SubmitInternal(time, e);

            worker.Notify();
        }

        public Task<bool> TryAppend(TLogEntry logentry)
        {
            if (stats != null) stats.EventCounters["TryAppendCalled"]++;

            Services.Verbose2("TryAppend");

            var promise = new TaskCompletionSource<bool>();

            SubmitInternal(DateTime.UtcNow, logentry, GetConfirmedVersion() + pending.Count, promise);

            worker.Notify();

            return promise.Task;
        }

        public Task<bool> TryAppendRange(IEnumerable<TLogEntry> logentries)
        {
            if (stats != null) stats.EventCounters["TryAppendRangeCalled"]++;

            Services.Verbose2("TryAppendRange");

            var promise = new TaskCompletionSource<bool>();
            var time = DateTime.UtcNow;
            var pos = GetConfirmedVersion() + pending.Count;

            bool first = true;
            foreach (var e in logentries)
            {
                SubmitInternal(time, e, pos++, first ? promise : null);
                first = false;
            }

            worker.Notify();
            
            return promise.Task;
        }


        private const int Unconditional = -1;

        private void SubmitInternal(DateTime time, TLogEntry logentry, int conditionalPosition = Unconditional, TaskCompletionSource<bool> resultPromise = null)
         {
             // create a submission entry
             var submissionentry = this.MakeSubmissionEntry(logentry);
             submissionentry.SubmissionTime = time;
             submissionentry.ResultPromise = resultPromise;
             submissionentry.ConditionalPosition = conditionalPosition;

             // add submission to queue
             pending.Add(submissionentry);

             // if we have a tentative state in use, update it
             if (this.TentativeStateInternal != null)
             {
                 try
                 {
                     Host.UpdateView(this.TentativeStateInternal, logentry);
                 }
                 catch (Exception e)
                 {
                     Services.CaughtViewUpdateException("PrimaryBasedLogViewAdaptor.SubmitInternal", e);
                 }
             }

             Host.OnViewChanged(true, false);
         }

        public TLogView TentativeView
        {
            get
            {
                if (stats != null)
                    stats.EventCounters["TentativeViewCalled"]++;

                if (TentativeStateInternal == null)
                    CalculateTentativeState();

                return TentativeStateInternal;
            }
        }

    
        public TLogView ConfirmedView
        {
            get
            {
                if (stats != null)
                    stats.EventCounters["ConfirmedViewCalled"]++;

                return LastConfirmedView();
            }
        }

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
        /// <param name="payload"></param>
        /// <returns></returns>
        public async Task<IProtocolMessage> OnProtocolMessageReceived(IProtocolMessage payload)
        {
            var notificationmessage = payload as NotificationMessage;

            if (notificationmessage != null)
            {
                Services.Verbose("NotificationReceived v{0}", notificationmessage.Version);

                OnNotificationReceived(notificationmessage);

                // poke worker so it will process the notifications
                worker.Notify();

                return null;
            }
            else
            {
                //it's a protocol message
                return await OnMessageReceived(payload);
            }
        }


        /// <summary>
        /// Called by MultiClusterOracle when there is a configuration change.
        /// </summary>
        /// <returns></returns>
        public async Task OnMultiClusterConfigurationChange(MultiCluster.MultiClusterConfiguration newconf)
        {
            Debug.Assert(newconf != null);

            var oldconf = Configuration;

            // process only if newer than what we already have
            if (!MultiClusterConfiguration.OlderThan(oldconf, newconf))
                return;

            Services.Verbose("Processing Configuration {0}", newconf);

            await this.OnConfigurationChange(newconf); // updates Configuration and does any work required

            var added = oldconf == null ? newconf.Clusters : newconf.Clusters.Except(oldconf.Clusters);

            // if the multi-cluster is operated correctly, this grain should not be active before we are joined to the multicluster
            // but if we detect that anyway here, enforce a refresh to reduce risk of missed notifications
            if (!need_initial_read && added.Contains(Services.MyClusterId))
            {
                need_refresh = true;
                Services.Verbose("Refresh Because of Join");
                worker.Notify();
            }

        }
    


        #endregion

  
        
        // method is virtual so subclasses can add their own events
        public virtual void EnableStatsCollection() {

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

        public void DisableStatsCollection()
        {
            stats = null;
        }

        public LogViewStatistics GetStats()
        {
            return stats;
        }


        private void CalculateTentativeState()
        {
            // copy the master
            this.TentativeStateInternal = (TLogView) SerializationManager.DeepCopy(LastConfirmedView());

            // Now apply all operations in pending 
            foreach (var u in this.pending)
                try
                {
                     Host.UpdateView(this.TentativeStateInternal, u.Entry);
                }
                catch(Exception e)
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
            var version = GetConfirmedVersion();

            ProcessNotifications();

            NotifyViewChanges(ref version);

            bool have_to_write = (pending.Count != 0);

            bool have_to_read = need_initial_read || (need_refresh && !have_to_write);

            Services.Verbose("WorkerCycle Start htr={0} htw={1}", have_to_read, have_to_write);

            if (have_to_read)
            {
                need_refresh = need_initial_read = false; // retrieving fresh version

                await ReadAsync();

                NotifyViewChanges(ref version);
            }

            if (have_to_write)
            {
                need_refresh = need_initial_read = false; // retrieving fresh version

                await UpdatePrimary();

                if (stats != null) stats.EventCounters["WritebackEvents"]++;
            }

            Services.Verbose("WorkerCycle Done");
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
                    var writeresult = await WriteAsync();

                    NotifyViewChanges(ref version, writeresult);

                    // if the batch write failed due to conflicts, retry.
                    if (writeresult == 0)
                        continue;

                    Host.OnViewChanged(false, true);

                    // notify waiting promises of the success of conditional updates
                    NotifyPromises(writeresult, true);

                    // record stabilization time, for statistics
                    if (stats != null)
                    {
                        var timeNow = DateTime.UtcNow;
                        for (int i = 0; i < writeresult; i++)
                        {
                            var latency = timeNow - pending[i].SubmissionTime;
                            stats.StabilizationLatenciesInMsecs.Add(latency.Milliseconds);
                        }
                    }

                    // remove completed updates from queue
                    pending.RemoveRange(0, writeresult);

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

        private void NotifyViewChanges(ref int version, int numwritten = 0)
        {
            var v = GetConfirmedVersion();
            bool tentativechanged = (v != version + numwritten);
            bool confirmedchanged = (v != version);
            if (tentativechanged || confirmedchanged)
            {
                TentativeStateInternal = null; // conservative.
                Host.OnViewChanged(tentativechanged, confirmedchanged);
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
                if (notificationtracker != null)
                    return notificationtracker.LastException;
                return null;
            }
        }

        /// <summary>
        /// Store the last exception that occurred while communicating with the primary.
        /// Is null if the last communication was successful.
        /// </summary>
        protected Exception LastPrimaryException;

        /// <summary>
        /// Wait for all local updates to finish, and retrieve latest global state. 
        /// May require global coordination.
        /// </summary>
        /// <returns></returns>
        public async Task SynchronizeNowAsync()
        {
            if (stats != null)
                stats.EventCounters["SynchronizeNowCalled"]++;

            Services.Verbose("SynchronizeNowStart");

            need_refresh = true;
            await worker.NotifyAndWait();

            Services.Verbose("SynchronizeNowComplete");
        }

        public IEnumerable<TLogEntry> UnconfirmedSuffix
        {
            get 
            {
                 return pending.Select(te => te.Entry);
            }
        }

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
            bool foundfailedconditionalupdates = false;

            for (int pos = 0; pos < pending.Count; pos++)
            {
                var submissionentry = pending[pos];
                if (submissionentry.ConditionalPosition != Unconditional
                    && (foundfailedconditionalupdates || 
                           submissionentry.ConditionalPosition != (version + pos)))
                {
                    foundfailedconditionalupdates = true;
                    if (submissionentry.ResultPromise != null)
                        submissionentry.ResultPromise.SetResult(false);
                }
                pos++;
            }

            if (foundfailedconditionalupdates)
            {
                pending.RemoveAll(e => e.ConditionalPosition != Unconditional);
                TentativeStateInternal = null;
                Host.OnViewChanged(true, false);
            }
        }

        protected void BroadcastNotification(NotificationMessage msg, string exclude = null)
        {
            // if there is only one cluster, or if we are global single instance, don't send notifications.
            if (Services.MultiClusterConfiguration.Clusters.Count == 1
                || Services.RegistrationStrategy != ClusterLocalRegistration.Singleton)
                return;

            // create notification tracker if we haven't already
            if (notificationtracker == null)
                notificationtracker = new NotificationTracker(this.Services, Configuration, Merge);

            notificationtracker.BroadcastNotification(msg, exclude);
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
