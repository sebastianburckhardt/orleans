using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans.Replication;
using System.Diagnostics;
using Orleans.Concurrency;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.MultiCluster;

namespace Orleans.Runtime.Replication
{
    /// <summary>
    /// A general template for constructing replication adaptors based on
    /// a sequentially read and written primary.
    /// Suclasses override ReadAsync and WriteAsync to read from / write to primary.
    /// </summary>
    /// <typeparam name="TGrainState">The user-defined grain state</typeparam>
    /// <typeparam name="TUpdate">The implementation-defined update object</typeparam>
    /// 
    public abstract class QueuedGrainAdaptorBase<TGrainState,TUpdate> :
        IQueuedGrainAdaptor<TGrainState> where TGrainState : GrainState, new()  where TUpdate : IUpdateOperation<TGrainState>
    {
        #region interface to subclasses that implement specific providers

 
        /// <summary>
        /// Set cached global state to initial value.
        /// </summary>
        protected abstract void InitializeCachedGlobalState(TGrainState initialstate);

        /// <summary>
        /// Read cached global state.
        /// </summary>
        protected abstract TGrainState LastConfirmedGlobalState();

        /// <summary>
        /// Read the latest primary state. Must block/retry until successful.
        /// </summary>
        /// <returns></returns>
        protected abstract Task ReadAsync();

        /// <summary>
        /// Write updates. Must block/retry until successful. 
        /// </summary>
        /// <param name="updates"></param>
        /// <returns>If non-null, this message is broadcast to all replicas</returns>
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
        protected virtual TUpdate TagUpdate(IUpdateOperation<TGrainState> update)
        {
            // by default, we don't tag.
            return (TUpdate) update;
        }

        /// <summary>
        /// Handle replication protocol messages.
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
        protected QueuedGrain<TGrainState> Host { get; private set; }

        protected IReplicationProtocolServices Services { get; private set; }

        protected MultiClusterConfiguration Configuration { get; set; }


        protected List<IConfirmedStateListener> listeners = new List<IConfirmedStateListener>();

        protected IReplicationProvider Provider;

        protected Dictionary<string, NotificationStatus> notificationtracker;

        protected QueuedGrainAdaptorBase(QueuedGrain<TGrainState> host, IReplicationProvider provider,
            TGrainState initialstate, IReplicationProtocolServices services)
        {
            Debug.Assert(host != null && services != null && initialstate != null);
            this.Host = host;
            this.Services = services;
            this.Provider = provider;
            InitializeCachedGlobalState(initialstate);
            worker = new BackgroundWorker(() => Work());
            Provider.Log.Verbose2("{0} Constructed {1}", Services.GrainReference, host.IdentityString);
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



        #endregion

        // the currently pending updates. 
        private readonly List<UpdateHolder> pending = new List<UpdateHolder>();

        struct UpdateHolder
        {
            public TUpdate updateObject;
            public DateTime entryTime;

            public UpdateHolder(TUpdate update)
            {
                this.updateObject = update;
                this.entryTime = DateTime.UtcNow;
            }
        }

        protected TGrainState CopyTentativeState()
        {
            var state = TentativeState;
            TentativeStateInternal = null; // to avoid aliasing
            return state;
        }
        protected List<TUpdate> CopyListOfUpdates()
        {
            return pending.Select(uh => uh.updateObject).ToList(); // must use a copy
        }
      
     

        /// <summary>
        ///  Tentative State. Represents Stable State + effects of pending updates.
        ///  Computed lazily (null if not in use)
        /// </summary>
        private TGrainState TentativeStateInternal;
     
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
        protected QueuedGrainStatistics stats = null;


        // For use by replication protocols. Determines if this cluster is part of the configured multicluster.
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

    

        public void EnqueueUpdate(IUpdateOperation<TGrainState> updateoperation)
        {
            //Trace.TraceInformation("UpdateLocalAsync");

            // add metadata to update if needed by replication protocol
            var taggedupdate = this.TagUpdate(updateoperation);

            // add update to queue
            pending.Add(new UpdateHolder(taggedupdate));

            // if we have a tentative state in use, update it
            if (this.TentativeStateInternal != null)
            {
                try
                {
                    taggedupdate.Update(this.TentativeStateInternal);
                }
                catch
                {
                    //TODO trace
                }
            }

            if (stats != null) stats.eventCounters["EnqueueUpdateCalled"]++;

            Provider.Log.Verbose2("{0} EnqueueUpdate", Services.GrainReference);

            worker.Notify();
        }



        public TGrainState TentativeState
        {
            get
            {
                if (stats != null)
                    stats.eventCounters["TentativeStateCalled"]++;

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

    
        public TGrainState ConfirmedState
        {
            get
            {
                if (stats != null)
                    stats.eventCounters["ConfirmedStateCalled"]++;

                return LastConfirmedGlobalState();
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
                Provider.Log.Verbose("{0} Processing Configuration {1}", Services.GrainReference, next);

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

            stats = new QueuedGrainStatistics()
            {
                eventCounters = new Dictionary<string, long>(),
                stabilizationLatenciesInMsecs = new List<int>()
            };
 
            stats.eventCounters.Add("TentativeStateCalled", 0);
            stats.eventCounters.Add("ConfirmedStateCalled", 0);
            stats.eventCounters.Add("EnqueueUpdateCalled", 0);            
            stats.eventCounters.Add("CurrentQueueHasDrainedCalled", 0);
            stats.eventCounters.Add("SynchronizeNowAsyncCalled", 0);

            stats.eventCounters.Add("WritebackEvents", 0);

            stats.stabilizationLatenciesInMsecs = new List<int>();
        }

        public void DisableStatsCollection()
        {
            stats = null;
        }

        public QueuedGrainStatistics GetStats()
        {
            return stats;
        }

        private bool ConfirmedStateHasChanged;


        private void CalculateTentativeState()
        {
            // copy the master
            this.TentativeStateInternal = (TGrainState)LastConfirmedGlobalState().DeepCopy();

            // Now apply all operations in pending 
            foreach (var u in this.pending)
                try
                {
                    u.updateObject.Update(this.TentativeStateInternal);
                }
                catch
                {
                    //TODO trace
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
                    l.OnConfirmedStateChanged();
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
                    // should never get here... replication provider is supposed to retry on exceptions
                    // because only the replication provider knows how to retry the right way
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
                stats.eventCounters["SynchronizeNowAsyncCalled"]++;

            Provider.Log.Verbose("{0} SynchronizeNowAsyncStart", Services.GrainReference);

            need_refresh = true;
            await worker.NotifyAndWait();

            Provider.Log.Verbose("{0} SynchronizeNowAsyncComplete", Services.GrainReference);
        }

        public IEnumerable<IUpdateOperation<TGrainState>> UnconfirmedUpdates
        {
            get 
            { 
                // extract original update objects from the pending queue
                return pending.Select(uh => {
                    var o = uh.updateObject;
                    var t = o as ITaggedUpdate<TGrainState>;
                    if (t != null)
                        return t.OriginalUpdate;
                    else
                        return o;
                });
            }
        }

        public async Task CurrentQueueHasDrained()
        {
            if (stats != null)
                stats.eventCounters["CurrentQueueHasDrainedCalled"]++;

            Provider.Log.Verbose("{0} CurrentQueueHasDrainedStart", Services.GrainReference);

            if (pending.Count != 0)
                await worker.WaitForCurrentWorkToBeServiced();

            Provider.Log.Verbose("{0} CurrentQueueHasDrainedEnd", Services.GrainReference);
        }
    

        public bool SubscribeConfirmedStateListener(IConfirmedStateListener listener)
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

        public bool UnSubscribeConfirmedStateListener(IConfirmedStateListener listener)
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

    public interface ITaggedUpdate<T> : IUpdateOperation<T> where T : GrainState, new()
    {
        IUpdateOperation<T> OriginalUpdate { get; }
    }



    [Serializable]
    public class NotificationMessage: IProtocolMessage
    {
        // contains no info
        
        // replication providers can subclass this to add more information
    }
    
}
