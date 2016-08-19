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
using Orleans.Runtime.LogViews;

namespace Orleans.Providers.LogViews
{

    /// <summary>
    /// A log view adaptor that wraps around a user-provided storage interface
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class CustomStorageAdaptor<T, E> : PrimaryBasedLogViewAdaptor<T, E, SubmissionEntry<E>>
        where T : class,new()
        where E : class
    {
        public CustomStorageAdaptor(ILogViewHost<T, E> host, T initialstate, 
            ILogViewProvider repprovider, IProtocolServices services)
            : base(host, repprovider, initialstate, services)
        {
            if (! (host is ICustomStorageInterface<T,E>))
                throw new BadProviderConfigException("Must implement ICustomStorageInterface<T,E> for CustomStorageLogView provider");
        }

        private const int slowpollinterval = 10000;

        private T cached;
        private int version;

        protected override T LastConfirmedView()
        {
            return cached;
        }

        protected override int GetConfirmedVersion()
        {
           return version;
        }

        protected override void InitializeConfirmedView(T initialstate)
        {
            cached = initialstate;
            version = 0;
        }

        // no special tagging is required, thus we create a plain submission entry
        protected override SubmissionEntry<E> MakeSubmissionEntry(E entry)
        {
            return new SubmissionEntry<E>() { Entry = entry } ;
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
                   var result = await ((ICustomStorageInterface<T,E>) Host).ReadStateFromStorageAsync();
                   version = result.Key;
                   cached = result.Value;

                   LastPrimaryException = null; // successful, so we clear stored prior exceptions
                    
                   Services.Verbose("read success v{0}", version);

                   break; // successful
                }
                catch (Exception e)
                {
                    LastPrimaryException = e; // store last exception for inspection by user code
                }

                Services.Verbose("read failed {0}", LastPrimaryException != null ? (LastPrimaryException.GetType().Name + LastPrimaryException.Message) : "");

                increasebackoff(ref backoff_msec);
            }

            exit_operation("ReadAsync");
        }



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

        protected override async Task<int> WriteAsync()
        {
            enter_operation("WriteAsync");

            int backoff_msec = -1;

            var updates = GetCurrentBatchOfUpdates().Select(submissionentry => submissionentry.Entry).ToList();
            bool writesuccessful = false;
            bool transitionssuccessful = false;

            try
            {
                writesuccessful = await ((ICustomStorageInterface<T,E>) Host).ApplyUpdatesToStorageAsync(updates, version);

                LastPrimaryException = null; // successful, so we clear stored exception

                if (writesuccessful)
                {
                    Services.Verbose("write ({0} updates) success v{1}", updates.Count, version + updates.Count);

                    // now we update the cached state by applying the same updates
                    // in case we encounter any exceptions we will re-read the whole state from storage
                    try
                    {
                        foreach (var u in updates)
                        {
                            version++;
                            Host.UpdateView(this.cached, u);
                        }

                        transitionssuccessful = true;
                    }
                    catch (Exception e)
                    {
                        Services.CaughtViewUpdateException("CustomStorageLogViewAdaptor.WriteAsync", e);
                    }
                }
            }
            catch (Exception e)
            {
                LastPrimaryException = e; // store exception for inspection by user code
            }

            if (!writesuccessful || !transitionssuccessful)    {
                Services.Verbose("{0} failed {1}", writesuccessful ? "transitions" : "write", LastPrimaryException != null ? (LastPrimaryException.GetType().Name + LastPrimaryException.Message) : "");

                increasebackoff(ref backoff_msec);

                while (true) // be stubborn until we can re-read the state from storage
                {
                    if (backoff_msec > 0)
                    {
                        Services.Verbose("backoff {0}", backoff_msec);

                        await Task.Delay(backoff_msec);
                    }

                    try
                    {
                        var result = await ((ICustomStorageInterface<T, E>)Host).ReadStateFromStorageAsync();
                        version = result.Key;
                        cached = result.Value;

                        LastPrimaryException = null; // successful

                        Services.Verbose("read success v{0}", version);

                        break;
                    }
                    catch (Exception e)
                    {
                        LastPrimaryException = e;
                    }

                    Services.Verbose("read failed {0}", LastPrimaryException != null ? (LastPrimaryException.GetType().Name + LastPrimaryException.Message) : "");

                    increasebackoff(ref backoff_msec);
                }
            }

            // broadcast notifications to all other clusters
            // TODO: send state instead of updates, if smaller
            if (writesuccessful)
                BroadcastNotification(new UpdateNotificationMessage()
                   {
                       Version = version,
                       Updates = updates,
                   });

            exit_operation("WriteAsync");

            return writesuccessful ? updates.Count : 0;
        }


        [Serializable]
        protected class UpdateNotificationMessage : INotificationMessage
        {
            public int Version { get; set; }
            public List<E> Updates { get; set; }

            public override string ToString()
            {
                return string.Format("v{0} ({1} updates)", Version, Updates.Count);
            }
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
            enter_operation("ProcessNotifications");

            // discard notifications that are behind our already confirmed state
            while (notifications.Count > 0 && notifications.ElementAt(0).Key < version)
            {
                Services.Verbose("discarding notification {0}", notifications.ElementAt(0).Value);
                notifications.RemoveAt(0);
            }

            // process notifications that reflect next global version
            while (notifications.Count > 0 && notifications.ElementAt(0).Key == version)
            {
                var updatenotification = notifications.ElementAt(0).Value;
                notifications.RemoveAt(0);

                // Apply all operations in pending 
                foreach (var u in updatenotification.Updates)
                    try
                    {
                        Host.UpdateView(cached, u);
                    }
                    catch (Exception e)
                    {
                        Services.CaughtViewUpdateException("ProcessNotifications", e);
                    }

                version = updatenotification.Version;

                Services.Verbose("notification success ({0} updates) v{1}", updatenotification.Updates.Count, version);
            }

            Services.Verbose2("unprocessed notifications in queue: {0}", notifications.Count);

            base.ProcessNotifications();
        
            exit_operation("ProcessNotifications");
        }

        [Conditional("DEBUG")]
        private void enter_operation(string name)
        {
            Services.Verbose2("/-- enter {0}", name);
        }

        [Conditional("DEBUG")]
        private void exit_operation(string name)
        {
            Services.Verbose2("\\-- exit {0}", name);
        }

    }
}
