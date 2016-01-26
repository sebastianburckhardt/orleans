using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans.LogViews;
using Orleans.MultiCluster;
using Orleans.Runtime.LogViews;

namespace Orleans.Providers.EventStores
{
    /// <summary>
    /// A log view adaptor that is based on an event store
    /// </summary>
    /// <typeparam name="T">The type of the log view, or grain state.</typeparam>
    /// <typeparam name="E">The type of the events (usually just object)</typeparam>
    class EventStoreLogViewAdaptor<T,E> : PrimaryBasedLogViewAdaptor<T,E,SubmissionEntry<E>> 
        where T: class,new() where E: class
    {

        public EventStoreLogViewAdaptor(ILogViewHost<T,E> host, ILogViewProvider provider,
            T initialstate, IProtocolServices services, string streamname)
            : base(host,provider,initialstate,services)
        {
            if (!(provider is IEventStore))
                throw new ArgumentException("host");
            if (string.IsNullOrEmpty(streamname))
                throw new ArgumentException("streamname");

            this.streamname = streamname;
        }


        private IEventStore EventStore { get { return (IEventStore) Provider; } }
        private string streamname;

        private T ConfirmedStateInternal;
        private int ConfirmedVersionInternal;

        protected override T LastConfirmedView()
        {
            return ConfirmedStateInternal;
        }

        protected override int GetConfirmedVersion()
        {
            return ConfirmedVersionInternal;
        }

        protected override void InitializeConfirmedView(T initialstate)
        {
            ConfirmedStateInternal = initialstate;
            ConfirmedVersionInternal = 0;
        }

        // no special tagging of events is required here, we create a plain submission entry
        protected override SubmissionEntry<E> MakeSubmissionEntry(E entry)
        {
            return new SubmissionEntry<E>() { Entry = entry };
        }
    
        // Read the latest primary state, retrying until successful
        protected override async Task ReadAsync()
        {
            await ReadAsyncInternal(null);
        }

        // Read the latest primary state, retrying until successful
        // while doing so, look for a particular guid appearing in the stream
        private async Task<bool> ReadAsyncInternal(Guid? lookfor)
        {
            bool guid_was_seen = false;
            while (true)
            {
                try
                {
                    var eventstream = await EventStore.LoadStreamFromVersion(streamname, ConfirmedVersionInternal);

                    LastExceptionInternal = null;

                    if (eventstream.Version != ConfirmedVersionInternal)
                        Services.ProtocolError("event store returned wrong version", true);

                    // construct state from stream of events
                    foreach (var item in eventstream.Events)
                    {
                        E @event;
                        
                        var taggedevent = item as TaggedEvent;
                        if (taggedevent != null)
                        {
                            @event = (E)taggedevent.Event;
                            if (lookfor.HasValue && lookfor.Value.Equals(taggedevent.Guid))
                                guid_was_seen = true;
                        }
                        else
                        {
                            @event = (E) item;
                        }

                        try
                        {
                            Host.TransitionView(ConfirmedStateInternal, @event);
                        }
                        catch (Exception e)
                        {
                            Services.CaughtTransitionException("ProcessNotifications", e); // logged by log view provider
                        }

                        ConfirmedVersionInternal++;
                        ConfirmedStateChanged();
                    }

                    return guid_was_seen; // exit the loop, we successfully read all events
                }
                catch (Exception e)
                {
                    Services.CaughtException("ReadAsync", e); // logged by log view provider
                    LastExceptionInternal = e;
                }
            }
        }

        private IEnumerable<object> TagLast(SubmissionEntry<E>[] batch, Guid guid)
        {
            for (int i = 0; i < batch.Length - 1; i++)
              yield return (object) batch[i];

            yield return new TaggedEvent(batch[batch.Length-1], guid);
        }

        // write entries in pending queue as a batch. 
        // Retry until outcome has been conclusively determined.
        // Return 0 if no updates were written, or the size of the whole batch if updates were written.
        protected override async Task<int> WriteAsync()
        {
            var updates = GetCurrentBatchOfUpdates();

            bool isConditional = updates.Any(se => se.ConditionalPosition != Unconditional);

            var guid = Guid.NewGuid(); // used for duplicate filtering on exception path

            bool success;

            try
            {
                await EventStore.AppendToStream(
                       streamname,
                       isConditional ? (int?)ConfirmedVersionInternal : null,
                       TagLast(updates, guid)
                    );

                LastExceptionInternal = null;
                success = true;
            }
            catch (OptimisticConcurrencyException)
            {
                success = false;
            }
            catch (Exception e)
            {
                Services.CaughtException("WriteAsync", e); // logged by log view provider
                LastExceptionInternal = e;
                success = false;
            }

            if (!success && LastExceptionInternal != null)
            {
                // we do not know for sure whether the write really failed. We need to read to find out.
                success = await ReadAsyncInternal(guid);
            }

            // let other clusters know that they should fetch the new events
            // we may optimize this in the future by sending the events directly
            if (success)
                BroadcastNotification(new NotificationMessage());

            return success ? updates.Length : 0;
        }
 


     


    }
}
