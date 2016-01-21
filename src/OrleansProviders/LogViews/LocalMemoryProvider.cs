using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;
using Orleans.LogViews;
using Orleans.Runtime.LogViews;
using Orleans.Runtime;
using System.Threading;

namespace Orleans.Providers.LogViews
{
    /// <summary>
    /// A simple log view provider that keeps the latest view in local memory.
    /// Does not synchronize between clusters.
    /// </summary>
    public class LocalMemoryProvider : ILogViewProvider
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

            return TaskDone.Done;
        }

        public Task Close()
        {
            return TaskDone.Done;
        }

        public ILogViewAdaptor<TView, TEntry> MakeLogViewAdaptor<TView, TEntry>(ILogViewHost<TView,TEntry> hostgrain, TView initialstate, string graintypename, IProtocolServices services)
            where TView : class, new()
            where TEntry : class
        {
            return new MemoryLogViewAdaptor<TView, TEntry>(hostgrain, this, initialstate, services);
        }
    }

    public class MemoryLogViewAdaptor<TView, TEntry> : PrimaryBasedLogViewAdaptor<TView, TEntry, TEntry>
        where TView : class, new()
        where TEntry : class
    {
        // the latest log view is simply stored here, in memory
        TView GlobalSnapshot;
        int GlobalVersion;

        public MemoryLogViewAdaptor(ILogViewHost<TView, TEntry> host, ILogViewProvider provider, TView initialstate, IProtocolServices services)
            : base(host, provider, initialstate, services)
        {
            GlobalSnapshot = initialstate;
            GlobalVersion = 0;
        }

        TView LocalSnapshot;
        int LocalVersion;

        // no tagging is required, thus the following two are identity functions
        protected override TEntry TagEntry(TEntry entry)
        {
            return entry;
        }
        protected override TEntry UntagEntry(TEntry taggedentry)
        {
            return taggedentry;
        }

        protected override TView LastConfirmedView()
        {
            return LocalSnapshot;
        }
        protected override int GetConfirmedVersion()
        {
            return LocalVersion; 
        }
        protected override void InitializeConfirmedView(TView initialstate)
        {
            LocalSnapshot = initialstate;
            LocalVersion = 0;
        }
       
        protected override async Task ReadAsync()
        {
            Trace.TraceInformation("ReadAsync");
            //await Task.Delay(5000);
            await Task.Delay(1);
            LocalSnapshot = GlobalSnapshot;
            LocalVersion = GlobalVersion;
        }

        /// <summary>
        /// Write updates. Must block until successful.
        /// </summary>
        /// <param name="updates"></param>
        /// <returns></returns>
        protected override async Task<WriteResult> WriteAsync()
        {
            Trace.TraceInformation("WriteAsync");

            var updates = CopyListOfUpdates();

            foreach (var u in updates)
            {
                try
                {
                    Host.TransitionView(GlobalSnapshot, u);
                }
                catch (Exception e)
                {
                    Services.CaughtTransitionException("MemoryLogViewAdaptor.WriteAsync", e);
                }

                GlobalVersion++;
            }

           // await Task.Delay(5000);
            await Task.Delay(1);

            LocalSnapshot = GlobalSnapshot;
            LocalVersion = GlobalVersion;

            return new WriteResult()
            {
                NumUpdatesWritten = updates.Count,
                NotificationMessage = null
            };
        }

    
    }
}
