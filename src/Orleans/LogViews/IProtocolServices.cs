﻿
using System;
using System.Net;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using System.Collections.Generic;
using Orleans.Providers;
using Orleans.Runtime;
using Orleans.MultiCluster;
using Orleans.GrainDirectory;

namespace Orleans.LogViews
{
    /// <summary>
    /// Functionality for use by log view adaptors that use custom consistency or replication protocols.
    /// Abstracts communication between replicas of the log view grain in different clusters.
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
        /// The multicluster registration strategy for this grain.
        /// </summary>
        MultiClusterRegistrationStrategy RegistrationStrategy { get; }


        /// <summary>
        /// Whether this cluster is running in a multi-cluster network.
        /// </summary>
        /// <returns></returns>
        bool MultiClusterEnabled { get; }


        /// <summary>
        /// The id of this cluster. Returns "I" if no multi-cluster network is present.
        /// </summary>
        /// <returns></returns>
        string MyClusterId { get; }

    
        /// <summary>
        /// The current multicluster configuration of this silo 
        /// (as injected by the administrator) or null if none.
        /// </summary>
        MultiClusterConfiguration MultiClusterConfiguration { get; }

        /// <summary>
        /// List of all clusters that currently appear to have at least one active
        /// gateway reporting to the multi-cluster network. 
        /// There are no guarantees that this membership view is complete or consistent.
        /// If there is no multi-cluster network, returns a list containing the single element "I".
        /// </summary>
        /// <returns></returns>
        IEnumerable<string>  ActiveClusters { get; }


        #region Logging Functionality

        /// <summary>
        /// Log an error that occurred in a log view protocol.
        /// </summary>
        void ProtocolError(string msg, bool throwexception);

        /// <summary>
        /// Log an exception that was caught in the log view protocol.
        /// </summary> 
        void CaughtException(string where, Exception e);

        /// <summary>
        /// Log an exception that occurred when trying to update a view.
        /// </summary>
        /// <param name="e"></param>
        void CaughtViewUpdateException(string where, Exception e);

        /// <summary> Output the specified message at <c>Info</c> log level. </summary>
        void Info(string format, params object[] args);        
        /// <summary> Output the specified message at <c>Verbose</c> log level. </summary>
        void Verbose(string format, params object[] args);
        /// <summary> Output the specified message at <c>Verbose2</c> log level. </summary>
        void Verbose2(string format, params object[] args);
        /// <summary> Output the specified message at <c>Verbose3</c> log level. </summary>
        void Verbose3(string format, params object[] args);

        #endregion
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
