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
    public abstract class PrimaryBasedLogViewAdaptor<TLogView,TLogEntry,TTaggedEntry> : ILogViewAdaptor<TLogView,TLogEntry> 
        where TLogView : class,new() 
        where TLogEntry:class
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
        //protected abstract int LastConfirmedVersion();  //TODO

        /// <summary>
        /// Read the latest primary state. Must block/retry until successful.
        /// </summary>
        /// <returns></returns>
        protected abstract Task ReadAsync();

        /// <summary>
        /// Apply pending entries to the primary. Must block/retry until successful. 
        /// </summary>
        /// <param name="updates"></param>
        /// <returns>If non-null, this message is broadcast to all clusters</returns>
        protected abstract Task<WriteResult> WriteAsync();

        protected struct WriteResult
        {
            public int NumUpdatesWritten;
            public NotificationMessage NotificationMessage;
        }

        /// <summary>
        /// If required by protocol, tag local update, e.g. with unique identifier
        /// </summary>
        /// <returns></returns>
        protected abstract TTaggedEntry TagEntry(TLogEntry entry);

        /// <summary>
        /// Get the entry out from the tagged entry
        /// </summary>
        /// <param name="taggedentry"></param>
        /// <returns></returns>
        protected abstract TLogEntry UntagEntry(TTaggedEntry taggedentry);
    

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
        /// Handle notification messages.
        /// </summary>
        /// <param name="payload"></param>
        /// <returns></returns>
        protected virtual void OnNotificationReceived(NotificationMessage payload)
        {
            // default mechanism is to simply refresh everything
            need_refresh = true;
        }

        /// <summary>
        /// Process stored notifications during worker cycle.
        /// </summary>
        /// <param name="payload"></param>
        /// <returns></returns>
        protected virtual void ProcessNotifications()
        {
             // do nothing by default - need_refresh takes care of it
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


        protected List<IViewListener> listeners = new List<IViewListener>();

        protected ILogViewProvider Provider;

        protected Dictionary<string, NotificationStatus> notificationtracker;

        protected PrimaryBasedLogViewAdaptor(ILogViewHost<TLogView,TLogEntry> host, ILogViewProvider provider,
            TLogView initialstate, IProtocolServices services)
        {
            Debug.Assert(host != null && services != null && initialstate != null);
            this.Host = host;
            this.Services = services;
            this.Provider = provider;
            InitializeConfirmedView(initialstate);
            worker = new BackgroundWorker(() => Work());
            Provider.Log.Verbose2("{0} Constructed {1}", Services.GrainReference, Host.IdentityString);
        }

        public virtual async Task Activate()
        {
            Provider.Log.Verbose2("{0} Activation Started", Services.GrainReference);

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

            Provider.Log.Verbose2("{0} Activation Complete", Services.GrainReference);
        }

        private async Task KickOffInitialRead()
        {
            need_initial_read = true;
            // kick off notification for initial read cycle with a bit of delay
            // so that we don't do this several times if user does strong sync
            await Task.Delay(10);
            Provider.Log.Verbose2("{0} Notify (initial read)", Services.GrainReference);
            worker.Notify();
        }

        public virtual async Task Deactivate()
        {
            Provider.Log.Verbose2("{0} Deactivation Started", Services.GrainReference);

            listeners.Clear();
            await worker.WaitForQuiescence();
            if (Silo.CurrentSilo.GlobalConfig.HasMultiClusterNetwork)
            {
                // unsubscribe this grain from configuration change events
                Silo.CurrentSilo.LocalMultiClusterOracle.UnSubscribeFromMultiClusterConfigurationEvents(Services.GrainReference);
            }

            Provider.Log.Verbose2("{0} Deactivation Complete", Services.GrainReference);
        }

        protected void LogTransitionException(Exception e)
        {
            Provider.Log.Warn((int)ErrorCode.LogView_TransitionException, "{0} Exception in View Transition: {1}", Services.GrainReference, e);
        }

        #endregion

        // the currently submitted, unconfirmed entries. 
        private readonly List<TimedEntry> pending = new List<TimedEntry>();

        struct TimedEntry
        {
            public TTaggedEntry taggedEntry;
            public DateTime entryTime;
        }

        protected TLogView CopyTentativeState()
        {
            var state = TentativeView;
            TentativeStateInternal = null; // to avoid aliasing
            return state;
        }
        protected List<TTaggedEntry> CopyListOfUpdates()
        {
            return pending.Select(uh => uh.taggedEntry).ToList(); // must use a copy
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
                Provider.Log.Verbose("{0} Waiting for join", Services.GrainReference);
                await Task.Delay(5000);
            }
        }
        protected async Task GetCaughtUpWithConfigurationAsync(DateTime adminTimestamp)
        {
            //TODO use notification instead of polling
            while (Configuration == null || Configuration.AdminTimestamp < adminTimestamp)
            {
                Provider.Log.Verbose("{0} Waiting for config {1}", Services.GrainReference, adminTimestamp);

                await Task.Delay(5000);
            }
        }


    
        #region Interface

    

        public void Submit(TLogEntry logentry)
        {
            // add metadata to update if needed by protocol
            var taggedupdate = this.TagEntry(logentry);

            // add update to queue
            pending.Add(new TimedEntry()
                {
                    taggedEntry = taggedupdate,
                    entryTime = DateTime.UtcNow
                });

            // if we have a tentative state in use, update it
            if (this.TentativeStateInternal != null)
            {
                try
                {
                    Host.TransitionView(this.TentativeStateInternal,UntagEntry(taggedupdate));
                }
                catch(Exception e)
                {
                    LogTransitionException(e);
                }
            }

            if (stats != null) stats.eventCounters["SubmitCalled"]++;

            Provider.Log.Verbose2("{0} Submit", Services.GrainReference);

            worker.Notify();
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


        protected void ConfirmedStateChanged()
        {
            // invalidate tentative state - it is lazily recomputed
            TentativeStateInternal = null;

            // set flag to notify listeners
            ConfirmedStateHasChanged = true;
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
                Provider.Log.Verbose("{0} NotificationReceived {1}", Services.GrainReference, notificationmessage);

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
                Provider.Log.Verbose("{0} ({1}) Processing Configuration {2}", Services.GrainReference, Services.MyClusterId, next);

                await this.OnConfigurationChange(next); // updates Configuration and does any work required

                // remove from notification tracker
                if (notificationtracker != null)
                {
                    var removed = notificationtracker.Keys.Except(next.Clusters);
                    foreach (var x in removed)
                    {
                        Provider.Log.Verbose("{0} No longer sending notifications to {1}", Services.GrainReference, x);
                        notificationtracker.Remove(x);
                    }
                }

                var added = oldconf == null ? Configuration.Clusters : Configuration.Clusters.Except(oldconf.Clusters);

                // add to notification tracker
                if (notificationtracker != null)
                    foreach (var x in added)
                        if (x != Services.MyClusterId)
                        {
                            Provider.Log.Verbose("{0} Now sending notifications to {1}", Services.GrainReference, x);
                            notificationtracker.Add(x, new NotificationStatus());
                        }

                // if the multi-cluster is operated correctly, this grain should not be active before we are joined to the multicluster
                // but if we detect that anyway here, enforce a refresh to reduce risk of missed notifications
                if (!need_initial_read && added.Contains(Services.MyClusterId))
                {
                    need_refresh = true;
                    Provider.Log.Verbose("{0} Refresh Because of Join", Services.GrainReference);
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
            stats.eventCounters.Add("SubmitCalled", 0);            
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

        private bool ConfirmedStateHasChanged;


        private void CalculateTentativeState()
        {
            // copy the master
            this.TentativeStateInternal = (TLogView) SerializationManager.DeepCopy(LastConfirmedView());

            // Now apply all operations in pending 
            foreach (var u in this.pending)
                try
                {
                     Host.TransitionView(this.TentativeStateInternal,UntagEntry(u.taggedEntry));
                }
                catch(Exception e)
                {
                    Provider.Log.Warn((int)ErrorCode.LogView_TentativeTransitionException, "{0} Exception in View Transition on Tentative State: {1}", Services.GrainReference, e);
                }
        }

        /// <summary>
        /// Background worker performs reads from and writes to global state.
        /// </summary>
        /// <returns></returns>
        internal async Task Work()
        {

            bool have_to_write = (pending.Count != 0);

            bool have_to_read = need_initial_read || (need_refresh && !have_to_write);

            Provider.Log.Verbose("{2} WorkerCycle Start htr={0} htw={1}", have_to_read, have_to_write, Services.GrainReference);

            if (have_to_read)
            {
                need_refresh = need_initial_read = false; // retrieving fresh version

                await ReadAsync();

                LastExceptionInternal = null; // we were successful.
            }

            ProcessNotifications();

            if (have_to_write)
            {
                need_refresh = need_initial_read = false; // retrieving fresh version

                int numUpdates = await UpdatePrimary();

                pending.RemoveRange(0, numUpdates);

                if (stats != null) stats.eventCounters["WritebackEvents"]++;
            }

            if (have_to_read || have_to_write)
            {
                LastExceptionInternal = null; // we were successful.
            }

            if (notificationtracker != null)
                RetryFailedMessages();

            // notify local listeners
            if (ConfirmedStateHasChanged)
            {
                ConfirmedStateHasChanged = false;
                foreach (var l in listeners)
                    l.OnViewChanged();
            }

            Provider.Log.Verbose("{0} WorkerCycle Done", Services.GrainReference);
        }


        /// <summary>
        /// This function repeatedly tries to stabilise operations.
        /// It will block until the operations have been succesfully stabilised
        /// </summary>
        /// <typeparam name="ResultType"></typeparam>
        /// <param name="update"></param>
        /// <returns></returns>
        internal async Task<int> UpdatePrimary()
        {
            while (true)
            {
                try
                {
                    var writeresult = await WriteAsync();

                    if (stats != null)
                    {
                        var timeNow = DateTime.UtcNow;
                        for (int i = 0; i < writeresult.NumUpdatesWritten; i++)
                        {
                            var latency = timeNow - pending[i].entryTime;
                            stats.stabilizationLatenciesInMsecs.Add(latency.Milliseconds);
                        }
                    }

                   
                    if (writeresult.NotificationMessage != null)
                    {
                        if (notificationtracker == null)
                            CreateNotificationTracker();

                        foreach (var kvp in notificationtracker)
                            SendNotificationMessage(kvp.Key, kvp.Value, writeresult.NotificationMessage).Ignore();  // exceptions are recorded in NotificationStatus
                    }

                    // numUpdates here denotes the number of operations that were taken from the pending queue
                    return writeresult.NumUpdatesWritten;
                }
                catch (Exception e)
                {
                    // should never get here... subclass is supposed to retry on exceptions
                    // because only that one knows how to retry the right way
                    LastExceptionInternal = e;
                    // if we get here anyway, we retry again
                    continue;
                }
            }
        }


        public Exception LastException
        {
            get
            {
                if (LastExceptionInternal != null)
                    return LastExceptionInternal;
                if (notificationtracker != null)
                    notificationtracker.Values.OrderBy(ns => ns.LastFailure).Select(ns => ns.LastException).LastOrDefault();
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

            Provider.Log.Verbose("{0} SynchronizeNowStart", Services.GrainReference);

            need_refresh = true;
            await worker.NotifyAndWait();

            Provider.Log.Verbose("{0} SynchronizeNowComplete", Services.GrainReference);
        }

        public IEnumerable<TLogEntry> UnconfirmedSuffix
        {
            get 
            {
                return null;
                //TODO 
                //extract original update objects from the pending queue
                ///return pending.Select(uh => {
                //    var o = uh.taggedEntry;
                //    var t = o as ITaggedUpdate<TLogView>;
                //    if (t != null)
                //        return t.OriginalUpdate;
               //     else
               //         return o;
                //});
            }
        }

        public async Task ConfirmSubmittedEntriesAsync()
        {
            if (stats != null)
                stats.eventCounters["ConfirmSubmittedEntriesCalled"]++;

            Provider.Log.Verbose("{0} ConfirmSubmittedEntriesStart", Services.GrainReference);

            if (pending.Count != 0)
                await worker.WaitForCurrentWorkToBeServiced();

            Provider.Log.Verbose("{0} ConfirmSubmittedEntriesEnd", Services.GrainReference);
        }
    

        public bool SubscribeViewListener(IViewListener listener)
        {
            if (listeners.Contains(listener))
            {
                return false;
            }
            else
            {
                listeners.Add(listener);
            }
            return true;
        }

        public bool UnSubscribeViewListener(IViewListener listener)
        {
            return listeners.Remove(listener);
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


        protected async Task SendNotificationMessage(string destinationcluster, NotificationStatus ns, NotificationMessage message)
        {
            try
            {
                ns.FailedMessage = null;
                await Services.SendMessage(message, destinationcluster);
                ns.LastException = null;
                ns.NumFailures = 0;
                Provider.Log.Verbose("{0} Sent notification to cluster {1}: {2}", Services.GrainReference, destinationcluster, message);
            }
            catch (Exception e)
            {
                ns.FailedMessage = message; // keep it for resending
                ns.LastException = e;
                ns.LastFailure = DateTime.UtcNow;
                ns.NumFailures++;
                Provider.Log.Info("{0} Could not send notification to cluster {1}: {2}", Services.GrainReference, destinationcluster, e);
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
            foreach (var kvp in notificationtracker)
            {
                if (kvp.Value.FailedMessage != null
                    && (DateTime.UtcNow - kvp.Value.LastFailure) > kvp.Value.RetryDelay())
                    SendNotificationMessage(kvp.Key, kvp.Value, kvp.Value.FailedMessage).Ignore();
            }
        }


        private void CreateNotificationTracker()
        {
            notificationtracker = new Dictionary<string,NotificationStatus>();
               foreach (var x in Configuration.Clusters)
                        if (x != Services.MyClusterId)
                        {
                            Provider.Log.Verbose("{0} Now sending notifications to {1}", Services.GrainReference, x);
                            notificationtracker.Add(x, new NotificationStatus());
                        }
        }
     

        #endregion



    }


    [Serializable]
    public class NotificationMessage: IProtocolMessage
    {
        // contains no info
        
        // log view providers can subclass this to add more information
    }
    
}
