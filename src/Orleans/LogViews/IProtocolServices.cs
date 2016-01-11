
using System;
using System.Net;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using System.Collections.Generic;
using Orleans.Providers;
using Orleans.Runtime;
using Orleans.MultiCluster;

namespace Orleans.LogViews
{
    /// <summary>
    /// Functionality for use by log view adaptors that use custom consistency or replication protocols.
    /// </summary>
    public interface IProtocolServices
    {
        /// <summary>
        /// Send a message to a remote cluster.
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
    /// Exception thrown by protocol messaging layer.
    /// </summary>
    [Serializable]
    public class ProtocolTransportException : OrleansException
    {
        public ProtocolTransportException()
        { }
        public ProtocolTransportException(string msg)
            : base(msg)
        { }
        public ProtocolTransportException(string msg, Exception exc)
            : base(msg, exc)
        { }
        protected ProtocolTransportException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        { }
    }

  
}
