using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;
using Orleans.Core;
using Orleans.MultiCluster;
using Orleans.Replication;
using Orleans.Runtime;
using Orleans.SystemTargetInterfaces;
using Orleans.Concurrency;

namespace Orleans.Runtime.Replication
{
    [Reentrant]
    internal class ReplicationProtocolGateway : SystemTarget, IReplicationProtocolGateway
    {
        public ReplicationProtocolGateway(SiloAddress silo)
            : base(Constants.ReplicationProtocolGatewayId, silo)
        {
        }

        public async Task<IProtocolMessage> RelayMessage(GrainId id, IProtocolMessage payload)
        {
            var g = InsideRuntimeClient.Current.InternalGrainFactory.Cast<IProtocolParticipant>(GrainReference.FromGrainId(id));
            return await g.OnProtocolMessageReceived(payload);
        }

    }
}
