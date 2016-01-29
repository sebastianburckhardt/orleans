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

namespace Orleans.Runtime.LogViews
{
    /// <summary>
    /// A general template for constructing log view adaptors based on
    /// a sequentially read and written primary.
    ///<para>
    /// The log itself is transient, i.e. not actually saved to storage - only the latest view and some 
    /// metadata (the log position, and write flags) is stored in the primary. 
    /// It is safe to interleave calls to this adaptor (on a cooperative scheduler).
    /// </para>
    ///<para>
    /// Suclasses override ReadAsync and WriteAsync to read from / write to primary.
    /// Calls to the primary are serialized, i.e. never interleave.
    /// </para>
    /// </summary>
    /// <typeparam name="TLogView">The user-defined view of the log</typeparam>
    /// <typeparam name="TLogEntry">The type of the log entries</typeparam>
    /// 
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
            // we have no interest in messages. return null.
            return Task.FromResult<IProtocolMessage>(null);
        }
        

        /// <summary>
        /// Handle notification messages.
        /// </summary>
        /// <param name="payload"></param>
        /// <returns></returns>
        protected virtual void OnNotificationReceived(NotificationMessage payload)
        {
            // record latest version we are told
            CreateNotificationTrackerIfNeeded();
            if (notificationtracker.lastversionreceived < payload.Version)
                 notificationtracker.lastversionreceived = payload.Version;
        }

        /// <summary>
        /// Process stored notifications during worker cycle.
        /// </summary>
        /// <param name="payload"></param>
        /// <returns></returns>
        protected virtual void ProcessNotifications()
        {
            if (notificationtracker != null && notificationtracker.lastversionreceived > GetConfirmedVersion())
                need_refresh = true;
        }

        /// <summary>
        /// Called when configuration of the multicluster is changing.
        /// </summary>
        /// <param name="previous"></param>
        /// <param name="current"></param>
        /// <returns></returns>
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

        protected NotificationTracker notificationtracker;

        protected class NotificationTracker
        {
            public int lastversionreceived;
            public Dictionary<string, NotificationStatus> sendstatus;
        }

        protected PrimaryBasedLogViewAdaptor(ILogViewHost<TLogView,TLogEntry> host, ILogViewProvider provider,
            TLogView initialstate, IProtocolServices services)
        {
            Debug.Assert(host != null && services != null && initialstate != null);
            this.Host = host;
            this.Services = services;
            this.Provider = provider;
            InitializeConfirmedView(initialstate);
            worker = new BackgroundWorker(() => Work());
            Services.Verbose2("Constructed {0}", Host.IdentityString);
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

            await worker.WaitForQuiescence();
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

 
        // called at beginning of WriteAsync to the current tentative state
        protected TLogView CopyTentativeState()
        {
            var state = TentativeView;
            TentativeStateInternal = null; // to avoid aliasing
            return state;
        }
        // called at beginning of WriteAsync to the current batch of updates
        protected TSubmissionEntry[] GetCurrentBatchOfUpdates()
        {
            return pending.ToArray(); // must use a copy
        }
        // called at beginning of WriteAsync to get current number of pending updates
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
        private BackgroundWorker worker;


     

        // statistics gathering. Is null unless stats collection is turned on.
        protected LogViewStatistics stats = null;


        // For use by protocols. Determines if this cluster is part of the configured multicluster.
        protected bool IsMyClusterJoined()
        {
            return (Configuration != null && Configuration.Clusters.Contains(Services.MyClusterId));
        }

        protected async Task EnsureClusterJoinedAsync()
        {
            //TODO use notification instead of polling
            while (!IsMyClusterJoined())
            {
                Services.Verbose("Waiting for join");
                await Task.Delay(5000);
            }
        }
        protected async Task GetCaughtUpWithConfigurationAsync(DateTime adminTimestamp)
        {
            //TODO use notification instead of polling
            while (Configuration == null || Configuration.AdminTimestamp < adminTimestamp)
            {
                Services.Verbose("Waiting for config {0}", adminTimestamp);

                await Task.Delay(5000);
            }
        }


    
        #region Interface

        public void Submit(TLogEntry logentry)
        {
            if (stats != null) stats.eventCounters["SubmitCalled"]++;

            Services.Verbose2("Submit");

            SubmitInternal(DateTime.UtcNow, logentry);

            worker.Notify();
        }

        public void SubmitRange(IEnumerable<TLogEntry> logentries)
        {
            if (stats != null) stats.eventCounters["SubmitRangeCalled"]++;

            Services.Verbose2("SubmitRange");

            var time = DateTime.UtcNow;

            foreach (var e in logentries)
                SubmitInternal(time, e);

            worker.Notify();
        }

        public Task<bool> TryAppend(TLogEntry logentry)
        {
            if (stats != null) stats.eventCounters["TryAppendCalled"]++;

            Services.Verbose2("TryAppend");

            var promise = new TaskCompletionSource<bool>();

            SubmitInternal(DateTime.UtcNow, logentry, GetConfirmedVersion() + pending.Count, promise);

            worker.Notify();

            return promise.Task;
        }

        public Task<bool> TryAppendRange(IEnumerable<TLogEntry> logentries)
        {
            if (stats != null) stats.eventCounters["TryAppendRangeCalled"]++;

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

        protected const int Unconditional = -1;

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
                     Host.TransitionView(this.TentativeStateInternal, logentry);
                 }
                 catch (Exception e)
                 {
                     Services.CaughtTransitionException("PrimaryBasedLogViewAdaptor.SubmitInternal", e);
                 }
             }

             Host.OnViewChanged(true, false);
         }

        public TLogView TentativeView
        {
            get
            {
                if (stats != null)
                    stats.eventCounters["TentativeViewCalled"]++;

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
                    stats.eventCounters["ConfirmedViewCalled"]++;

                return LastConfirmedView();
            }
        }

        public int ConfirmedVersion
        {
            get
            {
                if (stats != null)
                    stats.eventCounters["ConfirmedVersionCalled"]++;

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

                worker.Notify();
                return null;
            }
            else
            {
                //Provider payload mesasge
                return await OnMessageReceived(payload);
            }
        }


           /// <summary>
        /// Called by MultiClusterOracle when there is a configuration change.
        /// </summary>
        /// <returns></returns>
        public async Task OnMultiClusterConfigurationChange(MultiCluster.MultiClusterConfiguration next)
        {
            Debug.Assert(next != null);

            var oldconf = Configuration;

            if (MultiClusterConfiguration.OlderThan(oldconf, next))
            {
                Services.Verbose("Processing Configuration {0}", next);

                await this.OnConfigurationChange(next); // updates Configuration and does any work required

                // remove from notification tracker
                if (notificationtracker != null)
                {
                    var removed = notificationtracker.sendstatus.Keys.Except(next.Clusters);
                    foreach (var x in removed)
                    {
                        Services.Verbose("No longer sending notifications to {0}", x);
                        notificationtracker.sendstatus.Remove(x);
                    }
                }

                var added = oldconf == null ? Configuration.Clusters : Configuration.Clusters.Except(oldconf.Clusters);

                // add to notification tracker
                if (notificationtracker != null)
                    foreach (var x in added)
                        if (x != Services.MyClusterId)
                        {
                            Services.Verbose("Now sending notifications to {0}", x);
                            notificationtracker.sendstatus.Add(x, new NotificationStatus());
                        }

                // if the multi-cluster is operated correctly, this grain should not be active before we are joined to the multicluster
                // but if we detect that anyway here, enforce a refresh to reduce risk of missed notifications
                if (!need_initial_read && added.Contains(Services.MyClusterId))
                {
                    need_refresh = true;
                    Services.Verbose("Refresh Because of Join");
                    worker.Notify();
                }
            }
        }
    


        #endregion

  
        
        // method is virtual so subclasses can add their own events
        public virtual void EnableStatsCollection() {

            stats = new LogViewStatistics()
            {
                eventCounters = new Dictionary<string, long>(),
                stabilizationLatenciesInMsecs = new List<int>()
            };
 
            stats.eventCounters.Add("TentativeViewCalled", 0);
            stats.eventCounters.Add("ConfirmedViewCalled", 0);
            stats.eventCounters.Add("ConfirmedVersionCalled", 0);
            stats.eventCounters.Add("SubmitCalled", 0);
            stats.eventCounters.Add("SubmitRangeCalled", 0);
            stats.eventCounters.Add("TryAppendCalled", 0);
            stats.eventCounters.Add("TryAppendRangeCalled", 0);
            stats.eventCounters.Add("ConfirmSubmittedEntriesCalled", 0);
            stats.eventCounters.Add("SynchronizeNowCalled", 0);

            stats.eventCounters.Add("WritebackEvents", 0);

            stats.stabilizationLatenciesInMsecs = new List<int>();
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
                     Host.TransitionView(this.TentativeStateInternal, u.Entry);
                }
                catch(Exception e)
                {
                    Services.CaughtTransitionException("PrimaryBasedLogViewAdaptor.CalculateTentativeState", e);
                }
        }


        /// <summary>
        /// Background worker performs reads from and writes to global state.
        /// </summary>
        /// <returns></returns>
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

                if (stats != null) stats.eventCounters["WritebackEvents"]++;
            }

            if (notificationtracker != null)
                RetryFailedMessages();


            Services.Verbose("WorkerCycle Done");
        }


        /// <summary>
        /// This function stores the operations in the pending queue as a batch to the primary.
        /// Retries until some batch commits or there are no updates left.
        /// </summary>
        /// <typeparam name="ResultType"></typeparam>
        /// <param name="update"></param>
        /// <returns></returns>
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
                            stats.stabilizationLatenciesInMsecs.Add(latency.Milliseconds);
                        }
                    }

                    // remove completed updates from queue
                    pending.RemoveRange(0, writeresult);

                    return;
                }
                catch (Exception e)
                {
                    LastExceptionInternal = e;

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


        public Exception LastException
        {
            get
            {
                if (LastExceptionInternal != null)
                    return LastExceptionInternal;
                if (notificationtracker != null)
                    notificationtracker.sendstatus.Values.OrderBy(ns => ns.LastFailure).Select(ns => ns.LastException).LastOrDefault();
                return null;
            }
        }

        protected Exception LastExceptionInternal;



        /// <summary>
        /// Wait for all local updates to finish, and retrieve latest global state. 
        /// May require global coordination.
        /// </summary>
        /// <returns></returns>
        public async Task SynchronizeNowAsync()
        {
            if (stats != null)
                stats.eventCounters["SynchronizeNowCalled"]++;

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
                stats.eventCounters["ConfirmSubmittedEntriesCalled"]++;

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
            int pos = 0;
            int version = GetConfirmedVersion();
            bool removedsome = false;

            while (pos < pending.Count)
            {
                var submissionentry = pending[pos];
                if (submissionentry.ConditionalPosition != Unconditional
                    && submissionentry.ConditionalPosition != (version + pos))
                {
                    pending.RemoveAt(pos); // expect this rarely to be perf issue since conditional updates are usually not batched
                    removedsome = true;
                    if (submissionentry.ResultPromise != null)
                        submissionentry.ResultPromise.SetResult(false);
                }
                else
                    pos++;
            }

            if (removedsome)
            {
                TentativeStateInternal = null;
                Host.OnViewChanged(true, false);
            }
        }

       


        #region Notification Messages

        protected class NotificationStatus
        {
            public Exception LastException;
            public DateTime LastFailure;
            public NotificationMessage FailedMessage;
            public int NumFailures;
            
            public TimeSpan RetryDelay()
            {
               if (NumFailures < 3) return TimeSpan.FromMilliseconds(1);
               if (NumFailures < 1000) return TimeSpan.FromSeconds(30);
               return TimeSpan.FromMinutes(1);
            }
        }

        protected void BroadcastNotification(NotificationMessage msg, string exclude = null)
        {

            CreateNotificationTrackerIfNeeded();

            foreach (var kvp in notificationtracker.sendstatus)
                SendNotificationMessage(kvp.Key, kvp.Value, msg).Ignore();  // exceptions are recorded in NotificationStatus

        }


        private async Task SendNotificationMessage(string destinationcluster, NotificationStatus ns, NotificationMessage message)
        {
            try
            {
                ns.FailedMessage = null;
                await Services.SendMessage(message, destinationcluster);
                ns.LastException = null;
                ns.NumFailures = 0;
                Services.Verbose("Sent notification to cluster {0}: {1}", destinationcluster, message);
            }
            catch (Exception e)
            {
                ns.FailedMessage = message; // keep it for resending
                ns.LastException = e;
                ns.LastFailure = DateTime.UtcNow;
                ns.NumFailures++;
                Services.Info("Could not send notification to cluster {0}: {1}", destinationcluster, e);
            }

            if (ns.FailedMessage != null)
            {
                // prod worker when it is time to retry
                await Task.Delay(ns.RetryDelay());
                worker.Notify();
            }
        }

        private void RetryFailedMessages()
        {
            foreach (var kvp in notificationtracker.sendstatus)
            {
                if (kvp.Value.FailedMessage != null
                    && (DateTime.UtcNow - kvp.Value.LastFailure) > kvp.Value.RetryDelay())
                    SendNotificationMessage(kvp.Key, kvp.Value, kvp.Value.FailedMessage).Ignore();
            }
        }


        protected void CreateNotificationTrackerIfNeeded()
        {
            if (notificationtracker == null)
            {
                notificationtracker = new NotificationTracker();

                notificationtracker.sendstatus = new Dictionary<string, NotificationStatus>();

                foreach (var x in Configuration.Clusters)
                    if (x != Services.MyClusterId)
                    {
                        Services.Verbose("Now sending notifications to {0}", x);
                        notificationtracker.sendstatus.Add(x, new NotificationStatus());
                    }
            }
        }
     

        #endregion



    }


    [Serializable]
    public class NotificationMessage: IProtocolMessage
    {
        // contains last global version
        public int Version {get; set;}

        // log view providers can subclass this to add more information
        // for example, the log entries that were appended
    }


    // does not need to be serialized - used only locally inside the LogViewAdaptors
    public class SubmissionEntry<E>
    {
        public E Entry;
        public DateTime SubmissionTime;
        public TaskCompletionSource<bool> ResultPromise;
        public int ConditionalPosition;
    }

}
