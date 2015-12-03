
using System;
using System.Net;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using System.Collections.Generic;
using Orleans.Providers;
using Orleans.Runtime;
using Orleans.MultiCluster;

namespace Orleans.Replication
{
    /// <summary>
    /// Functionality for use by  replication provider implementations. 
    /// </summary>
    public interface IReplicationProtocolServices
    {
        /// <summary>
        /// Send a message to a remote replica.
        /// </summary>
        /// <param name="payload">the message</param>
        /// <param name="clusterId">the destination cluster id</param>
        /// <returns></returns>
        Task<IProtocolMessage> SendMessage(IProtocolMessage payload, string clusterId);


        /// <summary>
        /// The untyped reference for this grain.
        /// </summary>
        GrainReference GrainReference { get;  }

        /// <summary>
        /// The id of this cluster.
        /// </summary>
        /// <returns></returns>
        string MyClusterId { get; }

    
        /// <summary>
        /// The current multicluster configuration (as injected by the administrator) or null if none.
        /// </summary>
        MultiClusterConfiguration MultiClusterConfiguration { get; }

        /// <summary>
        /// List of all clusters that currently appear to have at least one active
        /// gateway reporting to the multi-cluster network.
        /// </summary>
        /// <returns></returns>
        IEnumerable<string>  ActiveClusters { get; }
    }


    /// <summary>
    /// Exception thrown by messaging layer of multicluster configuration.
    /// </summary>
    [Serializable]
    public class ReplicationTransportException : OrleansException
    {
        public ReplicationTransportException()
        { }
        public ReplicationTransportException(string msg)
            : base(msg)
        { }
        public ReplicationTransportException(string msg, Exception exc)
            : base(msg, exc)
        { }
        protected ReplicationTransportException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        { }
    }

  
}
