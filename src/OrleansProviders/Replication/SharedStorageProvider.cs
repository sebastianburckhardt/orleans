using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;
using Orleans.Replication;
using Orleans.Runtime;
using Orleans.Runtime.Replication;
using Orleans.Storage;
using System.Threading;

namespace Orleans.Providers.Replication
{
    public class SharedStorageProvider : IReplicationProvider
    {
        public string Name { get; private set; }

        public const string GLOBAL_STORAGE_PARAMETER = "GlobalStorageProvider";

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

            // get global storage provider 
            if (!config.Properties.TryGetValue(GLOBAL_STORAGE_PARAMETER, out globalstorageprovidername))
                throw new Orleans.Storage.BadProviderConfigException("Shared Storage Replication Provider is missing configuration parameter " + GLOBAL_STORAGE_PARAMETER);

            return TaskDone.Done;
        }

        string globalstorageprovidername;

        IStorageProvider globalstorageprovider;

        public void SetupDependedOnStorageProviders(Func<string, Storage.IStorageProvider> providermanagerlookup)
        {
            globalstorageprovider = providermanagerlookup(globalstorageprovidername);
        }

        public Task Close()
        {
            return TaskDone.Done;
        }

        public IQueuedGrainAdaptor<T> MakeReplicationAdaptor<T>(QueuedGrain<T> hostgrain, T initialstate, string graintypename, IReplicationProtocolServices services) where T : GrainState, new()
        {
            return new SharedStorageAdaptor<T>(hostgrain, initialstate, this, globalstorageprovider, graintypename, services);
        }
    }

}