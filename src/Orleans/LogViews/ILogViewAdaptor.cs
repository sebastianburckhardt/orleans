using Orleans.MultiCluster;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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


        /// <summary>Called during activation, right before the user grain activation code is run.</summary>
        Task Activate();

        /// <summary>Called during deactivation, right after the user grain deactivation code is run.</summary>
        Task Deactivate();

        /// <summary>Called when a grain receives a message from a remote instance.</summary>
        Task<IProtocolMessage> OnProtocolMessageReceived(IProtocolMessage payload);

        /// <summary>Called after the silo receives a new multi-cluster configuration.</summary>
        Task OnMultiClusterConfigurationChange(MultiClusterConfiguration next);


    }

 
}
