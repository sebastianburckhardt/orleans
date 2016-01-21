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
        /// Subscribe to notifications on changes to the confirmed view.
        /// </summary>
        bool SubscribeViewListener(IViewListener aListener);

        /// <summary>
        /// Unsubscribe from notifications on changes to the confirmed view.
        /// </summary>
        bool UnSubscribeViewListener(IViewListener aListener);

    }

    /// <summary>
    /// A listener that is notified when the confirmed view changes.
    /// </summary>
    public interface IViewListener
    {
        /// <summary>
        /// Gets called after the confirmed prefix has changed.
        /// <param name="version">the new length of the confirmed prefix</param>
        /// </summary>
        /// 
        void OnViewChanged(int version);
    }
}
