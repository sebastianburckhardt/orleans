using System.Threading;
using System.Threading.Tasks;
using Orleans.LogViews;
using Orleans.Runtime;

namespace Orleans.Providers.LogViews
{
    /// <summary>
    /// A log view provider that relies on grain-specific custom code for 
    /// loading states from storage, and writing deltas to storage.
    /// Grains that wish to use this provider must implement the <see cref="ICustomStorageInterface{TState, TDelta}"/>
    /// interface, to define how state is read and how deltas are written.
    /// If the provider attribute "PrimaryCluster" is supplied in the provider configuration, then only the specified cluster
    /// accesses storage, and other clusters may not issue updates. 
    /// </summary>
    public class CustomStorageProvider : ILogViewProvider
    {
        public string Name { get; private set; }

        public Logger Log { get; private set; }

        private static int counter;
        private int id;

        /// <summary>Primary cluster</summary>
        public string PrimaryCluster { get; private set; }

        protected virtual string GetLoggerName()
        {
            return string.Format("LogViews.{0}.{1}", GetType().Name, id);
        }

        /// <summary>
        /// Init function
        /// </summary>
        /// <param name="name">provider name</param>
        /// <param name="providerRuntime">provider runtime, see <see cref="IProviderRuntime"/></param>
        /// <param name="config">provider configuration</param>
        public Task Init(string name, IProviderRuntime providerRuntime, IProviderConfiguration config)
        {
            Name = name;
            id = Interlocked.Increment(ref counter);
            PrimaryCluster = config.GetProperty("PrimaryCluster", null);

            Log = providerRuntime.GetLogger(GetLoggerName());
            Log.Info("Init (Severity={0}) PrimaryCluster={1}", Log.SeverityLevel, 
                string.IsNullOrEmpty(PrimaryCluster) ? "(none specified)" : PrimaryCluster);

            return TaskDone.Done;
        }

        public Task Close()
        {
            return TaskDone.Done;
        }

        public ILogViewAdaptor<TView, TEntry> MakeLogViewAdaptor<TView, TEntry>(ILogViewHost<TView, TEntry> hostGrain, TView initialState, string grainTypeName, IProtocolServices services)
            where TView : class, new()
            where TEntry : class
        {
            return new CustomStorageAdaptor<TView, TEntry>(hostGrain, initialState, this, services, PrimaryCluster);
        }
    }

}