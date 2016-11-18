using System;
using System.Threading.Tasks;
using Orleans.MultiCluster;

namespace Orleans.LogViews
{
    /// <summary>
    /// A log view adaptor is the storage interface for <see cref="ILogViewGrain"/>, whose state is defined as a log view. 
    ///<para>
    /// There is one adaptor per grain, which is installed by <see cref="ILogViewProvider"/> when the grain is activated.
    ///</para>
    /// </summary>
    /// <typeparam name="TLogView"> Type for the log view </typeparam>
    /// <typeparam name="TLogEntry"> Type for the log entry </typeparam>
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
