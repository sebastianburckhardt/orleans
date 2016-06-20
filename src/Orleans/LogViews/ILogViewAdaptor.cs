using Orleans.MultiCluster;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.LogViews
{
    /// <summary>
    /// A log view adaptor is the storage interface for grains whose state is defined as a log view.
    ///<para>
    /// There is one adaptor per grain, which is installed by the LogViewProvider when a grain is activated.
    ///</para>
    /// </summary>
    /// <typeparam name="TView"></typeparam>
    public interface ILogViewAdaptor<TLogView, TLogEntry> : ILogViewStorageInterface<TLogView, TLogEntry>
        where TLogView: new()
    {

        #region Diagnostics

        Exception LastException { get; }

        void EnableStatsCollection();

        void DisableStatsCollection();

        LogViewStatistics GetStats();

        #endregion



        #region Framework interaction

        Task Activate();

        Task Deactivate();

        Task<IProtocolMessage> OnProtocolMessageReceived(IProtocolMessage payload);

        Task OnMultiClusterConfigurationChange(MultiClusterConfiguration next);

        #endregion
    }

 
}
