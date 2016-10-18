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
using Orleans.MultiCluster;

namespace Orleans.Providers.LogViews
{

    /// <summary>
    /// A log view adaptor that uses the user-provided storage interface <see cref="ICustomStorageInterface{T,E}"/>. 
    /// This interface must be implemented by any grain that uses this log view adaptor.
    /// </summary>
    /// <typeparam name="TLogView">log view type</typeparam>
    /// <typeparam name="TLogEntry">log entry type</typeparam>
    public class CustomStorageAdaptor<TLogView, TLogEntry> : PrimaryBasedLogViewAdaptor<TLogView, TLogEntry, SubmissionEntry<TLogEntry>>
        where TLogView : class, new()
        where TLogEntry : class
    {
        /// <summary>
        /// Initialize a new instance of CustomStorageAdaptor class
        /// </summary>
        public CustomStorageAdaptor(ILogViewHost<TLogView, TLogEntry> host, TLogView initialState,
            ILogViewProvider repProvider, IProtocolServices services, string primaryCluster)
            : base(host, repProvider, initialState, services)
        {
            if (!(host is ICustomStorageInterface<TLogView, TLogEntry>))
                throw new BadProviderConfigException("Must implement ICustomStorageInterface<TLogView,TLogEntry> for CustomStorageLogView provider");
            this.primaryCluster = primaryCluster;
        }

        private string primaryCluster;

        private const int slowpollinterval = 10000;

        private TLogView cached;
        private int version;

        protected override TLogView LastConfirmedView()
        {
            return cached;
        }

        protected override int GetConfirmedVersion()
        {
            return version;
        }

        protected override void InitializeConfirmedView(TLogView initialstate)
        {
            cached = initialstate;
            version = 0;
        }

        protected override bool SupportSubmissions
        {
            get
            {
                return MayAccessStorage();
            }
        }

        private bool MayAccessStorage()
        {
            return (!Services.MultiClusterEnabled)
                   || string.IsNullOrEmpty(primaryCluster)
                   || primaryCluster == Services.MyClusterId;
        }

        // no special tagging is required, thus we create a plain submission entry
        protected override SubmissionEntry<TLogEntry> MakeSubmissionEntry(TLogEntry entry)
        {
            return new SubmissionEntry<TLogEntry>() { Entry = entry };
        }

        [Serializable]
        private class ReadRequest : IProtocolMessage
        {
            public int KnownVersion { get; set; }
        }
        [Serializable]
        private class ReadResponse<TLogView> : IProtocolMessage
        {
            public int Version { get; set; }

            public TLogView Value { get; set; }
        }

        protected override Task<IProtocolMessage> OnMessageReceived(IProtocolMessage payload)
        {
            var request = (ReadRequest) payload;

            if (! MayAccessStorage())
                throw new ProtocolTransportException("message destined for primary cluster ended up elsewhere (inconsistent configurations?)");

            var response = new ReadResponse<TLogView>() { Version = version };

            // optimization: include value only if version is newer
            if (version > request.KnownVersion)
                response.Value = cached;

            return Task.FromResult<IProtocolMessage>(response);
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

                    if (MayAccessStorage())
                    {
                        // read from storage
                        var result = await ((ICustomStorageInterface<TLogView, TLogEntry>)Host).ReadStateFromStorageAsync();
                        version = result.Key;
                        cached = result.Value;
                    }
                    else
                    {
                        // read from primary cluster
                        var request = new ReadRequest() { KnownVersion = version };
                        if (!Services.MultiClusterConfiguration.Clusters.Contains(primaryCluster))
                            throw new ProtocolTransportException("the specified primary cluster is not in the multicluster configuration");
                        var response =(ReadResponse<TLogView>) await Services.SendMessage(request, primaryCluster);
                        if (response.Version > request.KnownVersion)
                        {
                            version = response.Version;
                            cached = response.Value;
                        }              
                    }

                    LastPrimaryException = null; // successful, so we clear stored prior exceptions

                    Services.Verbose("read success v{0}", version);

                    break; // successful
                }
                catch (Exception e)
                {
                    LastPrimaryException = e; // store last exception for inspection by user code
                }

                Services.Verbose("read failed {0}", LastPrimaryException != null ? (LastPrimaryException.GetType().Name + LastPrimaryException.Message) : "");

                Increasebackoff(ref backoff_msec);
            }

            exit_operation("ReadAsync");
        }



        Random random = null;

        /// <summary>
        /// Increase backoff
        /// </summary>
        public void Increasebackoff(ref int backoff)
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
                writesuccessful = await ((ICustomStorageInterface<TLogView,TLogEntry>) Host).ApplyUpdatesToStorageAsync(updates, version);

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

                Increasebackoff(ref backoff_msec);

                while (true) // be stubborn until we can re-read the state from storage
                {
                    if (backoff_msec > 0)
                    {
                        Services.Verbose("backoff {0}", backoff_msec);

                        await Task.Delay(backoff_msec);
                    }

                    try
                    {
                        var result = await ((ICustomStorageInterface<TLogView, TLogEntry>)Host).ReadStateFromStorageAsync();
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

                    Increasebackoff(ref backoff_msec);
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
            public List<TLogEntry> Updates { get; set; }

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
