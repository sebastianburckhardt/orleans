using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;
using Orleans.Replication;
using Orleans.Runtime.Replication;
using Orleans.Runtime;
using System.Threading;

namespace Orleans.Providers.Replication
{
    /// <summary>
    /// A replication provider that doesn't actually replicate, but just stores the global state locally.
    /// Meant to be used for testing only.
    /// </summary>
    public class DummyProvider : IReplicationProvider
    {
        public string Name { get; private set; }

        public Logger Log { get; private set; }

        private static int counter;
        private int id;

        protected virtual string GetLoggerName()
        {
            return string.Format("Replication.{0}.{1}", GetType().Name, id);
        }

        public Task Init(string name, IProviderRuntime providerRuntime, IProviderConfiguration config)
        {
            Name = name;
            id = Interlocked.Increment(ref counter);
            Log = providerRuntime.GetLogger(GetLoggerName());
            Log.Info("Init (Severity={0})", Log.SeverityLevel);

            // nothing to do for this dummy provider
            // in general, may read configuration and throw BadReplicationProviderConfigException
            return TaskDone.Done;
        }

        public Task Close()
        {
            return TaskDone.Done;
        }

        public IQueuedGrainAdaptor<T> MakeReplicationAdaptor<T>(QueuedGrain<T> hostgrain, T initialstate, string graintypename, IReplicationProtocolServices services) where T : GrainState, new()
        {
            return new DummyReplicationAdaptor<T>(hostgrain, this, initialstate, services);
        }
    

        public void SetupDependedOnStorageProviders(Func<string,Storage.IStorageProvider> providermanagerlookup)
        {
 	        // not needed for this provider
        }
}

    public class DummyReplicationAdaptor<T> : QueuedGrainAdaptorBase<T,IUpdateOperation<T>> where T : GrainState, new()
    {
        // in this dummy replication protocol, the "global" state is just locally stored
        T PseudoGlobalState;

        public DummyReplicationAdaptor(QueuedGrain<T> host, IReplicationProvider provider, T initialstate, IReplicationProtocolServices services)
            : base(host, provider, initialstate, services)
        {
            PseudoGlobalState = initialstate;
        }


        T CachedState;

        protected override T LastConfirmedGlobalState()
        {
            return CachedState;
        }
        protected override void InitializeCachedGlobalState(T initialstate)
        {
            CachedState = initialstate;
        }

     

       
        protected override async Task ReadAsync()
        {
            Trace.TraceInformation("ReadAsync");
            //await Task.Delay(5000);
            await Task.Delay(1);
            CachedState = PseudoGlobalState;
        }

        /// <summary>
        /// Write updates. Must block until successful.
        /// </summary>
        /// <param name="updates"></param>
        /// <returns></returns>
        protected override async Task<WriteResult> WriteAsync()
        {
            Trace.TraceInformation("WriteAsync");

            List<IUpdateOperation<T>> updates = CopyListOfUpdates();

            foreach (var u in updates)
                try
                {
                    u.Update(PseudoGlobalState);
                }
                catch
                {
                    //TODO trace
                }
           // await Task.Delay(5000);
            await Task.Delay(1);

            return new WriteResult()
            {
                NumUpdatesWritten = updates.Count,
                NotificationMessage = new NotificationMessage()
            };
        }

    
    }
}
