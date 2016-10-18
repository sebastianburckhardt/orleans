﻿using System;
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
    /// A log view provider that stores the latest view in primary storage, using any standard storage provider.
    /// Supports multiple clusters connecting to the same primary storage (doing optimistic concurrency control via e-tags)
    ///<para>
    /// The log itself is transient, i.e. not actually saved to storage - only the latest view (snapshot) and some 
    /// metadata (the log position, and write flags) are stored in the primary. 
    /// </para>
    /// </summary>
    public class SharedStorageProvider : ILogViewProvider
    {
        /// <summary>
        /// Shared storage provider name
        /// </summary>
        public string Name { get; private set; }

        /// <summary>
        /// Global storage provider parameter
        /// </summary>
        public const string GLOBAL_STORAGE_PROVIDER_PARAMETER = "GlobalStorageProvider";

        /// <summary>
        /// Gets Logger
        /// </summary>
        public Logger Log { get; private set; }

        private static int counter;
        private int id;

        protected virtual string GetLoggerName()
        {
            return string.Format("LogViews.{0}.{1}", GetType().Name, id);
        }

        /// <summary>
        /// Init metrhod
        /// </summary>
        /// <param name="name">Storage provider name</param>
        /// <param name="providerRuntime">Provider runtime</param>
        /// <param name="config">Provider config</param>
        public Task Init(string name, IProviderRuntime providerRuntime, IProviderConfiguration config)
        {
            Name = name;
            id = Interlocked.Increment(ref counter);

            Log = providerRuntime.GetLogger(GetLoggerName());
            Log.Info("Init (Severity={0})", Log.SeverityLevel);

            // get global storage provider (mandatory parameter)
            if (!config.Properties.TryGetValue(GLOBAL_STORAGE_PROVIDER_PARAMETER, out globalStorageProviderName))
                throw new Orleans.Storage.BadProviderConfigException("SharedStorageProvider is missing configuration parameter " + GLOBAL_STORAGE_PROVIDER_PARAMETER);

   
            if (!((ILogViewProviderRuntime)providerRuntime).TryGetStorageProvider(globalStorageProviderName, out globalStorageProvider, true))
            {
                 throw new Orleans.Storage.BadProviderConfigException("Could not find storage provider " + name);
           }

            return TaskDone.Done;
        }

        string globalStorageProviderName;
        IStorageProvider globalStorageProvider;
      
        /// <summary>
        /// Close method
        /// </summary>
        public Task Close()
        {
            return TaskDone.Done;
        }

        /// <summary>
        /// Make log view adaptor 
        /// </summary>
        /// <typeparam name="TView">View type param</typeparam>
        /// <typeparam name="TEntry">Entry type param</typeparam>
        /// <param name="hostGrain">Host grain</param>
        /// <param name="initialState">Initial state</param>
        /// <param name="services">Protocol services</param>
        public ILogViewAdaptor<TView, TEntry> MakeLogViewAdaptor<TView, TEntry>(ILogViewHost<TView, TEntry> hostGrain, TView initialState, string grainTypeName, IProtocolServices services) 
            where TView : class, new()
            where TEntry : class
        {
            return new StorageProviderLogViewAdaptor<TView,TEntry>(hostGrain, initialState, this, globalStorageProvider, grainTypeName, services);
        }
    }

}