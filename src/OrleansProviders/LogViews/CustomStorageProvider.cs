using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;
using Orleans.LogViews;
using Orleans.Runtime;
using Orleans.Runtime.LogViews;
using Orleans.Storage;
using System.Threading;

namespace Orleans.Providers.LogViews
{
    /// <summary>
    /// A log view provider that relies on a user-supplied storage interface
    /// (for grains that implement ICustomPrimaryStorage)
    /// </summary>
    public class CustomStorageProvider : ILogViewProvider
    {
        public string Name { get; private set; }

        public Logger Log { get; private set; }

        private static int counter;
        private int id;

        public string PrimaryCluster { get; private set; }

        protected virtual string GetLoggerName()
        {
            return string.Format("LogViews.{0}.{1}", GetType().Name, id);
        }

        public Task Init(string name, IProviderRuntime providerRuntime, IProviderConfiguration config)
        {
            Name = name;
            id = Interlocked.Increment(ref counter);
            PrimaryCluster = config.GetProperty("PrimaryCluster", "");

            if (string.IsNullOrEmpty(PrimaryCluster))
                throw new BadProviderConfigException("Missing attribute in CustomStorageLogView provider: PrimaryCluster");

            Log = providerRuntime.GetLogger(GetLoggerName());
            Log.Info("Init (Severity={0}) PrimaryCluster={1}", Log.SeverityLevel, PrimaryCluster);

            return TaskDone.Done;
        }

        public Task Close()
        {
            return TaskDone.Done;
        }

        public ILogViewAdaptor<TView, TEntry> MakeLogViewAdaptor<TView, TEntry>(ILogViewHost<TView, TEntry> hostgrain, TView initialstate, string graintypename, IProtocolServices services)
            where TView : class, new()
            where TEntry : class
        {
            return new CustomStorageAdaptor<TView, TEntry>(hostgrain, initialstate, this, services, PrimaryCluster);
        }
    }

}