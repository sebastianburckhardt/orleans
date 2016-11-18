using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.LogViews
{
    /// <summary>
    /// Interface implemented by all grains which use a log view provider for persistence 
    /// It gives the log view adaptor access to grain-specific information and callbacks.
    /// </summary>
    /// <typeparam name="TLogView">type of the log view</typeparam>
    /// <typeparam name="TLogEntry">type of log entries</typeparam>
    public interface ILogViewHost<TLogView, TLogEntry> : IConnectionIssueListener
    {
        /// <summary>
        /// Implementation of view transitions. 
        /// Any exceptions thrown will be caught and logged as a warning by <see cref="ILogViewProvider.Log"/>.
        /// </summary>
        void UpdateView(TLogView view, TLogEntry entry);

        /// <summary>
        /// Identity string for the host grain, for logging purposes only.
        /// </summary>
        string IdentityString { get; }

        /// <summary>
        /// Notifies the host grain about state changes. 
        /// Called by <see cref="ILogViewAdaptor{TLogView,TLogEntry}"/> whenever the tentative or confirmed state changes.
        /// Implementations may vary as to whether and how much they batch change notifications.
        /// Any exceptions thrown will be caught and logged as a warning  by <see cref="ILogViewProvider.Log"/>.
        /// </summary>
        void OnViewChanged(bool tentative, bool confirmed);

    }


}
