using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.LogViews
{
    /// <summary>
    /// Interface implemented by log view grains. 
    /// It gives the log view adaptor access to grain-specific information and callbacks.
    /// </summary>
    /// <typeparam name="TLogView">type of the log view</typeparam>
    /// <typeparam name="TLogEntry">type of log entries</typeparam>
    public interface ILogViewHost<TLogView,TLogEntry>  
    {
        /// <summary>
        /// Implementation of view transitions. 
        /// Any exceptions thrown will be caught and logged by the log view provider.
        /// </summary>
        void TransitionView(TLogView view, TLogEntry entry);

        /// <summary>
        /// Identity of this host, for logging purposes.
        /// </summary>
        string IdentityString { get; }

        /// <summary>
        /// Called by log view adaptor whenever the tentative or confirmed state changes.
        /// Implementations may vary as to whether and how much they batch change notifications.
        /// </summary>
        void OnViewChanged(bool tentative, bool confirmed);
    }


  
   

 
}
