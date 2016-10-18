using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;
using Orleans.LogViews;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Runtime.LogViews
{
    /// <summary>
    /// A log view adaptor that wraps around a traditional storage adaptor, and uses batching and e-tags
    /// to append entries.
    ///<para>
    /// The log itself is transient, i.e. not actually saved to storage - only the latest view and some 
    /// metadata (the log position, and write flags) are stored. 
    /// </para>
    /// </summary>
    /// <typeparam name="TLogView">Type of log view</typeparam>
    /// <typeparam name="TLogEntry">Type of log entry</typeparam>
    public class StorageProviderLogViewAdaptor<TLogView,TLogEntry> : PrimaryBasedLogViewAdaptor<TLogView, TLogEntry, SubmissionEntry<TLogEntry>> where TLogView : class,new() where TLogEntry : class
    {
        /// <summary>
        /// Initialize a StorageProviderLogViewAdaptor class
        /// </summary>
        public StorageProviderLogViewAdaptor(ILogViewHost<TLogView, TLogEntry> host, TLogView initialState, ILogViewProvider repProvider, IStorageProvider globalStorageProvider, string grainTypeName, IProtocolServices services)
            : base(host, repProvider, initialState, services)
        {
            this.globalStorageProvider = globalStorageProvider;
            this.grainTypeName = grainTypeName;
        }


        private const int maxEntriesInNotifications = 200;
        private const int slowpollinterval = 10000;


        IStorageProvider globalStorageProvider;
        string grainTypeName;        // stores the confirmed state including metadata
        GrainStateWithMetaDataAndETag<TLogView> GlobalStateCache;

        protected override TLogView LastConfirmedView()
        {
            return GlobalStateCache.StateAndMetaData.State;
        }

        protected override int GetConfirmedVersion()
        {
           return GlobalStateCache.StateAndMetaData.GlobalVersion;
        }

        protected override void InitializeConfirmedView(TLogView initialstate)
        {
            GlobalStateCache = new GrainStateWithMetaDataAndETag<TLogView>(initialstate);
        }

        // no special tagging is required, thus we create a plain submission entry
        protected override SubmissionEntry<TLogEntry> MakeSubmissionEntry(TLogEntry entry)
        {
            return new SubmissionEntry<TLogEntry>() { Entry = entry } ;
        }
      

        protected override async Task ReadAsync()
        {
            enter_operation("ReadAsync");

            int backoff_msec = -1;

            while (true)
            {
                if (backoff_msec > 0)
                    await Task.Delay(backoff_msec);

                try
                {
                    // for manual testing
                    //await Task.Delay(5000);

                    await globalStorageProvider.ReadStateAsync(grainTypeName, Services.GrainReference, GlobalStateCache);

                    LastPrimaryException = null; // successful, so we clear stored exception
                    
                    Services.Verbose("read success {0}", GlobalStateCache);

                    break; // successful
                }
                catch (Exception e)
                {
                    LastPrimaryException = e;
                }

                Services.Verbose("read failed");

                IncreaseBackoff(ref backoff_msec);
            }

            exit_operation("ReadAsync");
        }



        Random random = null;

        /// <summary>
        /// Increase backoff
        /// </summary>
        public void IncreaseBackoff(ref int backoff)
        {
            // after first fail do not backoff yet... keep it at zero
            if (backoff == -1) {  
                backoff = 0; 
                return;
            }

            if (random == null)
                random = new Random();

            // grows exponentially up to slowpoll interval
            if (backoff < slowpollinterval)
                backoff = (int)((backoff + random.Next(5, 15)) * 1.5);

            // during slowpoll, slightly randomize
            if (backoff > slowpollinterval)
                   backoff = slowpollinterval + random.Next(1, 200);
        }

        protected override async Task<int> WriteAsync()
        {
            enter_operation("WriteAsync");

            int backoffMsec = -1;

            var state = CopyTentativeState();
            var updates = GetCurrentBatchOfUpdates();
            bool batchsuccessfullywritten = false;

            var nextglobalstate = new GrainStateWithMetaDataAndETag<TLogView>(state);
            nextglobalstate.StateAndMetaData.WriteVector = GlobalStateCache.StateAndMetaData.WriteVector;
            nextglobalstate.StateAndMetaData.GlobalVersion = GlobalStateCache.StateAndMetaData.GlobalVersion + updates.Length;
            nextglobalstate.ETag = GlobalStateCache.ETag;

            var writebit = nextglobalstate.StateAndMetaData.ToggleBit(Services.MyClusterId);

            try
            {
                // for manual testing
                //await Task.Delay(5000);

                await globalStorageProvider.WriteStateAsync(grainTypeName, Services.GrainReference, nextglobalstate);

                LastPrimaryException = null; // successful

                batchsuccessfullywritten = true;

                GlobalStateCache = nextglobalstate;

                Services.Verbose("write ({0} updates) success {1}", updates.Length, GlobalStateCache);

            }
            catch (Exception e)
            {
                LastPrimaryException = e;
            }

            if (!batchsuccessfullywritten)
            {
                IncreaseBackoff(ref backoffMsec);

                Services.Verbose("write apparently failed {0}", nextglobalstate);

                while (true) // be stubborn until we can read what is there
                {

                    if (backoffMsec > 0)
                    {
                        Services.Verbose("backoff {0}", backoffMsec);

                        await Task.Delay(backoffMsec);
                    }

                    try
                    {
                        await globalStorageProvider.ReadStateAsync(grainTypeName, Services.GrainReference, GlobalStateCache);

                        LastPrimaryException = null; // successful, so we clear stored exception

                        Services.Verbose("read success {0}", GlobalStateCache);

                        break;
                    }
                    catch (Exception e)
                    {
                        LastPrimaryException = e;
                    }

                    Services.Verbose("read failed");

                    IncreaseBackoff(ref backoffMsec);
                }

                // check if last apparently failed write was in fact successful

                if (writebit == GlobalStateCache.StateAndMetaData.ContainsBit(Services.MyClusterId))
                {
                    GlobalStateCache = nextglobalstate;

                    Services.Verbose("last write ({0} updates) was actually a success {1}", updates.Length, GlobalStateCache);

                    batchsuccessfullywritten = true;
                }
            }


            // broadcast notifications to all other clusters
            if (batchsuccessfullywritten)
                BroadcastNotification(new UpdateNotificationMessage()
                   {
                       Version = GlobalStateCache.StateAndMetaData.GlobalVersion,
                       Updates = updates.Select(se => se.Entry).ToList(),
                       Origin = Services.MyClusterId,
                       ETag = GlobalStateCache.ETag
                   });

            exit_operation("WriteAsync");

            if (!batchsuccessfullywritten)
                return 0;

            return updates.Length;
        }
    
        
        [Serializable]
        protected class UpdateNotificationMessage : INotificationMessage 
        {
            public int Version { get; set; }

            public string Origin { get; set; }

            public List<TLogEntry> Updates { get; set; }

            public string ETag { get; set; }

            public override string ToString()
            {
                return string.Format("v{0} ({1} updates by {2}) etag={2}", Version, Updates.Count, Origin, ETag);
            }
         }

        protected override INotificationMessage Merge(INotificationMessage earlierMessage, INotificationMessage laterMessage)
        {
            var earlier = earlierMessage as UpdateNotificationMessage;
            var later = laterMessage as UpdateNotificationMessage;

            if (earlier != null
                && later != null
                && earlier.Origin == later.Origin
                && earlier.Version + later.Updates.Count == later.Version
                && earlier.Updates.Count + later.Updates.Count < maxEntriesInNotifications)

                return new UpdateNotificationMessage()
                {
                    Version = later.Version,
                    Origin = later.Origin,
                    Updates = earlier.Updates.Concat(later.Updates).ToList(),
                    ETag = later.ETag
                };

            else
                return base.Merge(earlierMessage, laterMessage); // keep only the version number
        }

        private SortedList<long, UpdateNotificationMessage> notifications = new SortedList<long,UpdateNotificationMessage>();

        protected override void OnNotificationReceived(INotificationMessage payload)
        {
            var um = payload as UpdateNotificationMessage;
            if (um != null)
                notifications.Add(um.Version - um.Updates.Count, um);
            else
                base.OnNotificationReceived(payload);
        }

        protected override void ProcessNotifications()
        {
            // discard notifications that are behind our already confirmed state
            while (notifications.Count > 0 && notifications.ElementAt(0).Key < GlobalStateCache.StateAndMetaData.GlobalVersion)
            {
                Services.Verbose("discarding notification {0}", notifications.ElementAt(0).Value);
                notifications.RemoveAt(0);
            }

            // process notifications that reflect next global version
            while (notifications.Count > 0 && notifications.ElementAt(0).Key == GlobalStateCache.StateAndMetaData.GlobalVersion)
            {
                var updateNotification = notifications.ElementAt(0).Value;
                notifications.RemoveAt(0);

                // Apply all operations in pending 
                foreach (var u in updateNotification.Updates)
                    try
                    {
                        Host.UpdateView(GlobalStateCache.StateAndMetaData.State, u);
                    }
                    catch (Exception e)
                    {
                        Services.CaughtViewUpdateException("ProcessNotifications", e);
                    }

                GlobalStateCache.StateAndMetaData.GlobalVersion = updateNotification.Version;

                GlobalStateCache.StateAndMetaData.ToggleBit(updateNotification.Origin);

                GlobalStateCache.ETag = updateNotification.ETag;         

                Services.Verbose("notification success ({0} updates) {1}", updateNotification.Updates.Count, GlobalStateCache);
            }

            Services.Verbose2("unprocessed notifications in queue: {0}", notifications.Count);

            base.ProcessNotifications();
         
        }


        #region non-reentrancy assertions

#if DEBUG
        bool operation_in_progress;
#endif

        [Conditional("DEBUG")]
        private void enter_operation(string name)
        {
#if DEBUG
            Services.Verbose2("/-- enter {0}", name);
            Debug.Assert(!operation_in_progress);
            operation_in_progress = true;
#endif
        }

        [Conditional("DEBUG")]
        private void exit_operation(string name)
        {
#if DEBUG
            Services.Verbose2("\\-- exit {0}", name);
            Debug.Assert(operation_in_progress);
            operation_in_progress = false;
#endif
        }

       

        #endregion
    }
}
