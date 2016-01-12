using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans.EventSourcing;
using Orleans.LogViews;
using Orleans.MultiCluster;

namespace Orleans.Runtime.LogViews
{
    /// <summary>
    /// A log view adaptor that is based on an event store
    /// </summary>
    /// <typeparam name="T">The type of the log view, or grain state.</typeparam>
    class EventStoreBasedLogViewAdaptor<T> : PrimaryBasedLogViewAdaptor<T,object,TaggedEntry> where T: LogViewType<object>,new()
    {

        public EventStoreBasedLogViewAdaptor(ILogViewAdaptorHost host, ILogViewProvider provider,
            T initialstate, IProtocolServices services, IEventStore eventstore, string streamname)
            : base(host,provider,initialstate,services)
        {
            this.eventstore = eventstore;
            this.streamname = streamname;
        }


        private IEventStore eventstore;
        private string streamname;

        private T ConfirmedStateInternal;

        /// <summary>
        /// Set confirmed view the initial value (a view of the empty log)
        /// </summary>
        protected override void InitializeConfirmedView(T initialstate)
        {
            ConfirmedStateInternal = initialstate;
        }

        /// <summary>
        /// Read cached global state.
        /// </summary>
        protected override T LastConfirmedView()
        {
            return ConfirmedStateInternal;
        }

        /// <summary>
        /// Read version of cached global state.
        /// </summary>
        //protected abstract int LastConfirmedVersion();  //TODO

        /// <summary>
        /// Read the latest primary state. Must block/retry until successful.
        /// </summary>
        /// <returns></returns>
        protected override Task ReadAsync()
        {
            // construct state from stream of events

            // TODO

            return TaskDone.Done;
        }

        /// <summary>
        /// Apply pending entries to the primary. Must block/retry until successful. 
        /// </summary>
        /// <param name="updates"></param>
        /// <returns>If non-null, this message is broadcast to all clusters</returns>
        protected override Task<WriteResult> WriteAsync()
        {


            // TODO

            return Task.FromResult<WriteResult>(new WriteResult()
            {
                NotificationMessage = null,
                NumUpdatesWritten = 0
            });
        }
 

        /// <summary>
        /// If required by protocol, tag local update, e.g. with unique identifier
        /// </summary>
        /// <returns></returns>
        protected override TaggedEntry TagEntry(object entry)
        {
            return new TaggedEntry()
            {
                Entry = entry,
                Guid = new Guid()                
            };
        }

        /// <summary>
        /// Get the entry out from the tagged entry
        /// </summary>
        /// <param name="taggedentry"></param>
        /// <returns></returns>
        protected override object UntagEntry(TaggedEntry taggedentry)
        {
            return taggedentry.Entry;
        }


     


    }
}
