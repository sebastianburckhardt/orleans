
using System;
using System.Net;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using System.Collections.Generic;
using Orleans.Providers;
using Orleans.Runtime;
using Orleans.Core;
using Orleans.Storage;

namespace Orleans.LogViews
{
    /// <summary>
    /// Interface to be implemented for a log view provider.
    /// </summary>
    public interface ILogViewProvider : IPersistenceProvider
    {
        /// <summary>Gets the TraceLogger used by this log view provider.</summary>
        Logger Log { get; }

        /// <summary>
        /// Construct a <see cref="ILogViewAdaptor{TLogView,TLogEntry}"/> to be installed in the given host grain.
        /// </summary>
        ILogViewAdaptor<TLogView, TLogEntry> MakeLogViewAdaptor<TLogView, TLogEntry>(
            ILogViewHost<TLogView, TLogEntry> hostgrain,
            TLogView initialstate,
            string graintypename,
            IProtocolServices services)

            where TLogView : class,new()
            where TLogEntry : class;

    }

   
}
