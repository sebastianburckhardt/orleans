using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.LogViews
{
    public interface ILogView<TView, TLogEntry> 
    {
        /// <summary>
        /// Local, tentative view of the log (reflecting both confirmed and unconfirmed entries)
        /// </summary>
        TView TentativeView { get; }

        /// <summary>
        /// Confirmed view of the log (reflecting only confirmed entries)
        /// </summary>
        TView ConfirmedView { get; }

        /// <summary>
        /// The length of the confirmed prefix of the log
        /// </summary>
        int ConfirmedVersion { get; }

        /// <summary>
        /// A list of the submitted entries that do not yet appear in the confirmed prefix.
        /// </summary>
        IEnumerable<TLogEntry> UnconfirmedSuffix { get; }

    }
 
}
