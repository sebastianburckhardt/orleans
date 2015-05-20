using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.SystemTargetInterfaces
{
    internal interface IGraphPartitionSystemTarget : ISystemTarget
    {
        Task<List<ActivationAddress>> Swap(SiloAddress remoteSilo, Dictionary<ActivationAddress, double> proposal, int remoteActivations);
    }
}
