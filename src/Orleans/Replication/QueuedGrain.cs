/*
Project Orleans Cloud Service SDK ver. 1.0
 
Copyright (c) Microsoft Corporation
 
All rights reserved.
 
MIT License

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and 
associated documentation files (the ""Software""), to deal in the Software without restriction,
including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense,
and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

using System.Threading.Tasks;
using Orleans.Concurrency;
using Orleans.Runtime;
using Orleans.MultiCluster;
using System;
using System.Collections.Generic;

namespace Orleans.Replication
{

    /// <summary>
    /// Queued grain base class. 
    /// </summary>
    public abstract class QueuedGrain<TGrainState> : Grain, IProtocolParticipant, IReplicationAdaptorHost,
                                                         IQueuedGrain<TGrainState> where TGrainState : GrainState, new()
    {
        protected QueuedGrain()
        { }

        /// <summary>
        /// this object is just a shell: all the state and logic is in the adaptor
        /// Using an adaptor hides adaptor internals 
        /// (while still allowing the latter to span class hierarchy and assemblies)
        /// </summary>
        internal IQueuedGrainAdaptor<TGrainState> Adaptor { get; private set; }

        /// <summary>
        /// Called right after grain is constructed, to install the replication adaptor.
        /// </summary>
        void IReplicationAdaptorHost.InstallAdaptor(IReplicationProvider provider, object initialstate, string graintypename, IReplicationProtocolServices services)
        {
            // call the replication provider to construct the adaptor, passing the type argument
            Adaptor = provider.MakeReplicationAdaptor<TGrainState>(this, (TGrainState) initialstate, graintypename, services);            
        }



        /// <summary>
        /// Notify replication adaptor of activation
        /// </summary>
        public Task ActivateProtocolParticipant()
        {
            return Adaptor.Activate();
        }

        /// <summary>
        /// Notify replication adaptor of deactivation
        /// </summary>
        public Task DeactivateProtocolParticipant()
        {
            return Adaptor.Deactivate();
        }

        /// <summary>
        /// Receive a message from other replicas, pass on to replication adaptor.
        /// </summary>
        [AlwaysInterleave]
        Task<IProtocolMessage> IProtocolParticipant.OnProtocolMessageReceived(IProtocolMessage payload)
        {
            return Adaptor.OnProtocolMessageReceived(payload);
        }

        [AlwaysInterleave]
        Task IProtocolParticipant.OnMultiClusterConfigurationChange(MultiCluster.MultiClusterConfiguration next)
        {
            return Adaptor.OnMultiClusterConfigurationChange(next);
        }


        #region IQueuedGrain

        // Delegate all methods of the public interface to the adaptor.
        // we are also adding the XML comments here so they show up in Intellisense for users.

        /// <summary>
        /// Enforces full synchronization with the global state.
        /// This both (a) drains all updates currently in the queue, and (b) retrieves the latest global state. 
        /// </summary>
        public Task SynchronizeNowAsync()
        {
            return Adaptor.SynchronizeNowAsync();
        }

        /// <summary>
        /// Queue an update.
        /// The update becomes visible in (TentativeState) immediately. All queued updates are written to the global state automatically in the background.
        /// <param name="update">An object representing the update</param>
        /// </summary>
        public void EnqueueUpdate(IUpdateOperation<TGrainState> update)
        {
            Adaptor.EnqueueUpdate(update);
        }


        /// <summary>
        /// Returns the current queue of unconfirmed updates.
        /// </summary>
        public IEnumerable<IUpdateOperation<TGrainState>> UnconfirmedUpdates
        { 
           get { return Adaptor.UnconfirmedUpdates; } 
        }

        /// <summary>
        /// Returns a task that can be waited on to ensure all updates currently in the queue have been confirmed.
        /// </summary>
        /// <returns></returns>
        public Task CurrentQueueHasDrained()
        {
            return Adaptor.CurrentQueueHasDrained();
        }


        /// <summary>
        /// The tentative state of this grain (read-only).
        /// This is always equal to (ConfirmedState) with all the updates in (UnconfirmedUpdates) applied on top.
        /// </summary>
        public TGrainState TentativeState
        {
            get { return Adaptor.TentativeState; }
        }

        /// <summary>
        /// The last confirmed snapshot of the global state (read-only).
        /// Does not include the effect of the updates in (UnconfirmedUpdates).
        /// </summary>
        public TGrainState ConfirmedState
        {
            get { return Adaptor.ConfirmedState; }
        }

        public bool SubscribeConfirmedStateListener(IConfirmedStateListener listener)
        {
            return Adaptor.SubscribeConfirmedStateListener(listener);
        }

        public bool UnSubscribeConfirmedStateListener(IConfirmedStateListener listener)
        {
            return Adaptor.UnSubscribeConfirmedStateListener(listener);
        }

        public Exception LastException 
        {
            get { return Adaptor.LastException;  }
        }

        public void EnableStatsCollection()
        {
            Adaptor.EnableStatsCollection();
        }

        public void DisableStatsCollection()
        {
            Adaptor.DisableStatsCollection();
        }

        public QueuedGrainStatistics GetStats()
        {
            return Adaptor.GetStats();
        }

        #endregion




    }
}
