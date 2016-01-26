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
    /// A log view provider that stores the log in an event store
    /// </para>
    /// </summary>
    public class EventStoreProvider : ILogViewProvider
    {
        public string Name { get; private set; }

        public const string GLOBAL_STORAGE_PARAMETER = "GlobalStorageProvider";

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

            // get global storage provider 
            if (!config.Properties.TryGetValue(GLOBAL_STORAGE_PARAMETER, out globalstorageprovidername))
                throw new Orleans.Storage.BadProviderConfigException("PrimaryViewStoreProvider is missing configuration parameter " + GLOBAL_STORAGE_PARAMETER);

            if (!((ILogViewProviderRuntime)providerRuntime).TryGetStorageProvider(globalstorageprovidername, out globalstorageprovider, true))
            {
                throw new Orleans.Storage.BadProviderConfigException("Could not find storage provider " + name);
            }

            return TaskDone.Done;
        }

        string globalstorageprovidername;
        IStorageProvider globalstorageprovider;


        public Task Close()
        {
            return TaskDone.Done;
        }

        public ILogViewAdaptor<TView, TEntry> MakeLogViewAdaptor<TView, TEntry>(ILogViewHost<TView, TEntry> hostgrain, TView initialstate, string graintypename, IProtocolServices services)
            where TView : class, new()
            where TEntry : class
        {
            return new StorageBasedLogViewAdaptor<TView, TEntry>(hostgrain, initialstate, this, globalstorageprovider, graintypename, services);
        }
    }

}