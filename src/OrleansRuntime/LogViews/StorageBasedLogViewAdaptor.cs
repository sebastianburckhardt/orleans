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
    /// A log view adaptor that wraps around a traditional storage adaptor
    ///<para>
    /// The log itself is transient, i.e. not actually saved to storage - only the latest view and some 
    /// metadata (the log position, and write flags) are stored. 
    /// </para>
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class StorageBasedLogViewAdaptor<T,E> : PrimaryBasedLogViewAdaptor<T,E,E> where T : LogViewType<E>, new() where E: class
    {
        public StorageBasedLogViewAdaptor(ILogViewAdaptorHost host, T initialstate, ILogViewProvider repprovider, IStorageProvider globalstorageprovider, string graintypename, IProtocolServices services)
            : base(host, repprovider, initialstate, services)
        {
            this.globalstorageprovider = globalstorageprovider;
            this.graintypename = graintypename;
        }

        IStorageProvider globalstorageprovider;
        string graintypename;

        // stores the confirmed state including metadata
        GrainStateWithMetaData<T> GlobalStateCache;

        protected override T LastConfirmedView()
        {
            return GlobalStateCache.GrainState;
        }

        protected override void InitializeConfirmedView(T initialstate)
        {
            GlobalStateCache = new GrainStateWithMetaData<T>(initialstate);
        }

        // no tagging is required, thus the following two are identity functions
        protected override E TagEntry(E entry)
        {
            return entry;
        }
        protected override E UntagEntry(E taggedupdate)
        {
            return taggedupdate;
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

                    await globalstorageprovider.ReadStateAsync(graintypename, Services.GrainReference, GlobalStateCache);

                    ConfirmedStateChanged(); // confirmed state has changed

                    Provider.Log.Verbose("{0} read success {1}", Services.GrainReference, GlobalStateCache);

                    break; // successful
                }
                catch (Exception e)
                {
                    LastExceptionInternal = e;
                }

                Provider.Log.Verbose("{0} read failed", Services.GrainReference);

                increasebackoff(ref backoff_msec);
            }

            exit_operation("ReadAsync");
        }


        public const int slowpollinterval = 10000;

        Random random = null;

        public void increasebackoff(ref int backoff)
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


        protected override async Task<WriteResult> WriteAsync()
        {
            enter_operation("WriteAsync");

            int backoff_msec = -1;

            T state;
            List<E> updates;

            while (true)
            {
                state = CopyTentativeState();
                updates = CopyListOfUpdates();

                var nextglobalstate = new GrainStateWithMetaData<T>()
                {
                   GlobalVersion = GlobalStateCache.GlobalVersion + 1,
                   GrainState = state,
                   WriteVector = GlobalStateCache.WriteVector,
                   Etag = GlobalStateCache.Etag
                };

                var writebit = nextglobalstate.ToggleBit(Services.MyClusterId);

                try
                {
                    // for manual testing
                    //await Task.Delay(5000);

                    await globalstorageprovider.WriteStateAsync(graintypename, Services.GrainReference, nextglobalstate);
                    
                    GlobalStateCache = nextglobalstate;

                    ConfirmedStateChanged(); // confirmed state has changed

                    Provider.Log.Verbose("{0} write ({1} updates) success {2}", Services.GrainReference, updates.Count, GlobalStateCache);

                    break; // successful
                }
                catch (Exception e) 
                {
                    LastExceptionInternal = e;
                }

                increasebackoff(ref backoff_msec);

                Provider.Log.Verbose("{0} write apparently failed {1}", Services.GrainReference, nextglobalstate);

                while(true)
                {
                    if (backoff_msec > 0)
                    {
                        Provider.Log.Verbose("{0} backoff {1}", Services.GrainReference, backoff_msec);

                        await Task.Delay(backoff_msec);
                    }

                    try
                    {
                        await globalstorageprovider.ReadStateAsync(graintypename, Services.GrainReference, GlobalStateCache);

                        ConfirmedStateChanged(); // confirmed state has changed

                        Provider.Log.Verbose("{0} read success {1}", Services.GrainReference, GlobalStateCache);
                        
                        break; // successful
                    }
                    catch (Exception e) 
                    {
                        LastExceptionInternal = e;
                    }

                    Provider.Log.Verbose("{0} read failed", Services.GrainReference);

                    increasebackoff(ref backoff_msec);
                }            

                // check if last apparently failed write was in fact successful

                if (writebit == GlobalStateCache.ContainsBit(Services.MyClusterId))
                {
                    GlobalStateCache = nextglobalstate;

                    ConfirmedStateChanged(); // confirmed state has changed

                    Provider.Log.Verbose("{0} last write ({1} updates) was actually a success {2}", Services.GrainReference, updates.Count, GlobalStateCache);

                    break;
                }
            }

            exit_operation("WriteAsync");

            return new WriteResult()
            {
                NumUpdatesWritten = updates.Count,
                NotificationMessage = new UpdateNotificationMessage()
                {
                    GlobalVersion = GlobalStateCache.GlobalVersion,
                    Updates = updates,
                    Origin = Services.MyClusterId,
                    Etag = GlobalStateCache.Etag
                }
            };

        }
        
        [Serializable]
        protected class UpdateNotificationMessage : NotificationMessage 
        {
            public long GlobalVersion { get; set; }

            public string Origin { get; set; }

            public List<E> Updates { get; set; }

            public string Etag { get; set; }

            public override string ToString()
            {
                return string.Format("v{0} ({1} updates by {2}) etag={2}", GlobalVersion, Updates.Count, Origin, Etag);
            }

            public void ApplyToGlobalState(GrainStateWithMetaData<T> globalstate)
            {
                Debug.Assert(GlobalVersion == globalstate.GlobalVersion + 1);

                // Apply all operations in pending 
                foreach (var u in Updates)
                    globalstate.GrainState.TransitionView(u);

                globalstate.GlobalVersion++;

                globalstate.ToggleBit(Origin);

                globalstate.Etag = Etag;
            }
        }

        private SortedList<long, UpdateNotificationMessage> notifications = new SortedList<long,UpdateNotificationMessage>();

        protected override void OnNotificationReceived(NotificationMessage payload)
        {
           var um = (UpdateNotificationMessage) payload;
           notifications.Add(um.GlobalVersion, um);
        }

        protected override void ProcessNotifications()
        {
            enter_operation("ProcessNotifications");

            // discard notifications that are behind our already confirmed state
            while (notifications.Count > 0 && notifications.ElementAt(0).Key <= GlobalStateCache.GlobalVersion)
            {
                Provider.Log.Verbose("{0} discarding notification {1}", Services.GrainReference, notifications.ElementAt(0).Value.Updates.Count);
                notifications.RemoveAt(0);
            }

            // process notifications that reflect next global version
            while (notifications.Count > 0 && notifications.ElementAt(0).Key == GlobalStateCache.GlobalVersion + 1)
            {
                var updatenotification = notifications.ElementAt(0).Value;
                notifications.RemoveAt(0);

                updatenotification.ApplyToGlobalState(GlobalStateCache);

                ConfirmedStateChanged(); // confirmed state has changed

                Provider.Log.Verbose("{0} notification success ({0} updates) {1}", Services.GrainReference, updatenotification.Updates.Count, GlobalStateCache);
            }

            Provider.Log.Verbose2("{0} unprocessed notifications in queue: {1}", Services.GrainReference, notifications.Count);
         
            exit_operation("ProcessNotifications");
        }


        #region non-reentrancy assertions

#if DEBUG
        bool operation_in_progress;
#endif

        [Conditional("DEBUG")]
        private void enter_operation(string name)
        {
#if DEBUG
            Provider.Log.Verbose2("{0} /-- enter {1}", Services.GrainReference, name);
            Debug.Assert(!operation_in_progress);
            operation_in_progress = true;
#endif
        }

        [Conditional("DEBUG")]
        private void exit_operation(string name)
        {
#if DEBUG
            Provider.Log.Verbose2("{0} \\-- exit {1}", Services.GrainReference, name);
            Debug.Assert(operation_in_progress);
            operation_in_progress = false;
#endif
        }

       

        #endregion
    }
}
