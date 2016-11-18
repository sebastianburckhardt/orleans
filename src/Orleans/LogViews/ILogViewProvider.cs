using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.LogViews
{
    /// <summary>
    /// Interface to be implemented for a log view provider.
    /// </summary>
    public interface ILogViewProvider : IPersistenceProvider
    {
        /// <summary>TraceLogger used by this log view provider.</summary>
        Logger Log { get; }

        /// <summary>
        /// Construct a <see cref="ILogViewAdaptor{TLogView,TLogEntry}"/> to be installed in the given host grain.
        /// </summary>
        ILogViewAdaptor<TLogView, TLogEntry> MakeLogViewAdaptor<TLogView, TLogEntry>(
            ILogViewHost<TLogView, TLogEntry> hostGrain,
            TLogView initialState,
            string grainTypeName,
            IProtocolServices services)

            where TLogView : class,new()
            where TLogEntry : class;
    }
}
