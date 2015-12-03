using Orleans.MultiCluster;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Replication
{
    /// <summary>
    /// A queued grain adaptor is used to encapsulate functionality for queued grains. 
    /// There is one adaptor per grain. 
    /// The adaptor is constructed by the ReplicationProvider when a grain is activated.
    /// All functions called on replicated grains are delegated to the adaptor.
    /// </summary>
    /// <typeparam name="TGrainState"></typeparam>
    public interface IQueuedGrainAdaptor<TGrainState> : IQueuedGrain<TGrainState> where TGrainState : GrainState,new()
    {

        Task<IProtocolMessage> OnProtocolMessageReceived(IProtocolMessage payload);

        Task OnMultiClusterConfigurationChange(MultiClusterConfiguration next);

        Task Activate();

        Task Deactivate();

        

    }
}
