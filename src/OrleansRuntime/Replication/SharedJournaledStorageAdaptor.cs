using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;
using Orleans.EventSourcing;
using Orleans.Replication;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Runtime.Replication
{
  
    /// <summary>
    /// An queued grain adaptor that wraps around a traditional storage adaptor
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class SharedJournaledStorageAdaptor<T> : QueuedGrainAdaptorBase<T, IUpdateOperation<T>> where T : GrainState, new()
    {
        public SharedJournaledStorageAdaptor(IReplicationAdaptorHost host, T initialstate, IReplicationProvider repprovider, IJournaledStorageProvider globalStorageProvider, string grainTypeName, IReplicationProtocolServices services)
            : base(host, repprovider, initialstate, services)
        {
            this.globalStorageProvider = globalStorageProvider;
            this.grainTypeName = grainTypeName;
        }

        IJournaledStorageProvider globalStorageProvider;
        string grainTypeName;

        // stores the confirmed state including metadata
        GrainStateWithMetaData<T> GlobalStateCache;

        protected override T LastConfirmedGlobalState()
        {
            return GlobalStateCache.GrainState;
        }

        protected override void InitializeCachedGlobalState(T initialstate)
        {
            GlobalStateCache = new GrainStateWithMetaData<T>(initialstate);
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
                    var streamName = StreamName.GetName(grainTypeName, Services.GrainReference, GlobalStateCache.GrainState as ICustomStreamName);
                    await globalStorageProvider.ReadState(streamName, GlobalStateCache);

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
            List<IUpdateOperation<T>> updates;

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

                    var streamName = StreamName.GetName(grainTypeName, Services.GrainReference, GlobalStateCache.GrainState as ICustomStreamName);
                    await globalStorageProvider.WriteState(streamName, nextglobalstate);
                    
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
                        var streamName = StreamName.GetName(grainTypeName, Services.GrainReference, GlobalStateCache.GrainState as ICustomStreamName);
                        await globalStorageProvider.ReadState(streamName, GlobalStateCache);

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

            public List<IUpdateOperation<T>> Updates { get; set; }

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
                    try
                    {
                        u.Update(globalstate.GrainState);
                    }
                    catch
                    {
                        //TODO trace
                    }

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
