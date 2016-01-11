using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;
using Orleans.Core;
using Orleans.LogViews;
using Orleans.MultiCluster;
using Orleans.Runtime;
using Orleans.SystemTargetInterfaces;

namespace Orleans.Runtime.LogViews
{
    /// <summary>
    /// Functionality for use by log view adaptors that run distributed protocols. 
    /// This class allows access to these services to providers that cannot see runtime-internals.
    /// It also stores grain-specific information like the grain reference, and caches 
    /// </summary>
    internal class ProtocolServices : IProtocolServices
    {

        public GrainReference GrainReference { get { return grain.GrainReference; } }

        public ILogViewProvider Provider { get; private set; }

        
        private Grain grain;   // links to the grain that owns this service object

        // cached 


        internal ProtocolServices(Grain gr, ILogViewProvider provider)
        {
            this.grain = gr;
            this.Provider = provider;

            if (! Silo.CurrentSilo.GlobalConfig.HasMultiClusterNetwork)
                PseudoMultiClusterConfiguration = new MultiClusterConfiguration(DateTime.UtcNow, new string[] { PseudoReplicaId }.ToList());
        }


        public async Task<IProtocolMessage> SendMessage(IProtocolMessage payload, string clusterId) 
        {
            var silo = Silo.CurrentSilo;
            var mycluster = silo.ClusterId;

            Provider.Log.Verbose3("SendMessage {0}->{1}: {2}", mycluster, clusterId, payload);

            if (mycluster == clusterId)
            {
                var g = (IProtocolParticipant) grain;
                // we are on the same scheduler, so we can call the method directly
                return await g.OnProtocolMessageReceived(payload);
            }

            if (PseudoMultiClusterConfiguration != null)
               throw new ProtocolTransportException("no such cluster");

            if (Provider.Log.IsVerbose3)
            {
                var gws = Silo.CurrentSilo.LocalMultiClusterOracle.GetGateways();
                Provider.Log.Verbose3("Available Gateways:\n{0}", string.Join("\n", gws.Select((gw) => gw.ToString())));
            }

            var clusterGateway = Silo.CurrentSilo.LocalMultiClusterOracle.GetRandomClusterGateway(clusterId);
            
            if (clusterGateway == null)
                throw new ProtocolTransportException("no active gateways found for cluster");

            var repAgent = InsideRuntimeClient.Current.InternalGrainFactory.GetSystemTarget<IProtocolGateway>(Constants.ProtocolGatewayId, clusterGateway);

            try
            {
                var retMessage = await repAgent.RelayMessage(GrainReference.GrainId, payload);
                return retMessage;
            }
            catch (Exception e)
            {
                throw new ProtocolTransportException("failed sending message to cluster", e);
            }
        }

        // pseudo-configuration to use if there is no actual multicluster network
        private static MultiClusterConfiguration PseudoMultiClusterConfiguration;
        private static string PseudoReplicaId = "I";



        public string MyClusterId
        {
            get {
                if (PseudoMultiClusterConfiguration != null)
                    return PseudoReplicaId;
                else
                    return Silo.CurrentSilo.ClusterId;
            }
        }

        public MultiClusterConfiguration MultiClusterConfiguration
        {
            get
            {
                if (PseudoMultiClusterConfiguration != null)
                    return PseudoMultiClusterConfiguration;
                else
                    return Silo.CurrentSilo.LocalMultiClusterOracle.GetMultiClusterConfiguration();
            }
        }

        public IEnumerable<string> ActiveClusters
        {
            get
            {
                if (PseudoMultiClusterConfiguration != null)
                    return PseudoMultiClusterConfiguration.Clusters;
                else 
                    return Silo.CurrentSilo.LocalMultiClusterOracle.GetActiveClusters();
            }
        }
    }

}
