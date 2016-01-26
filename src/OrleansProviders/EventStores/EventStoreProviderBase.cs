using Orleans.LogViews;
using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Providers.EventStores
{
    /// <summary>
    /// A template for building event store providers.
    /// Subclasses can extend and override the IEventStore interface
    /// </summary>
    public abstract class EventStoreProviderBase : IEventStore, ILogViewProvider
    {
        public string Name { get; private set; }

        public Logger Log { get; private set; }

        private static int counter;
        private int id;
        protected virtual string GetLoggerName()
        {
            return string.Format("LogViews.{0}.{1}", GetType().Name, id);
        }

        public Task Init(string name, IProviderRuntime providerRuntime, IProviderConfiguration config)
        {
            Name = name;
            id = Interlocked.Increment(ref counter);

            Log = providerRuntime.GetLogger(GetLoggerName());
            Log.Info("Init (Severity={0})", Log.SeverityLevel);

            return Init(config);
        }


        public Task Close()
        {
            return TaskDone.Done; // TODO
        }

        public ILogViewAdaptor<TView, TEntry> MakeLogViewAdaptor<TView, TEntry>(ILogViewHost<TView, TEntry> hostgrain, TView initialstate, string graintypename, IProtocolServices services)
            where TView : class, new()
            where TEntry : class
        {
            var streamname = DetermineStreamName(graintypename, services);
            return new EventStoreLogViewAdaptor<TView, TEntry>(hostgrain, this, initialstate, services, streamname);
        }


        // override this to use a different naming scheme
        protected virtual string DetermineStreamName(string graintypename, IProtocolServices services)
        {

            return string.Format("{0}-{1}", graintypename, services.GrainReference.ToKeyString());

        }


        #region IEventStore

        public abstract Task Init(IProviderConfiguration config);

        public abstract Task<IEventStream> LoadStream(string streamName);

        public abstract Task<IEventStream> LoadStreamFromVersion(string streamName, int version);

        public abstract Task AppendToStream(string streamName, int? expectedVersion, IEnumerable<object> events);

        public abstract Task DeleteStream(string streamName, int? expectedVersion);

        #endregion
    }
}
 