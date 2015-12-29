using Orleans.Concurrency;
using Orleans.MultiCluster;
using Orleans.Replication;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Orleans.EventSourcing
{
    /// <summary>
    /// The base class for all grain classes that have event-sourced state.
    /// </summary>
    public abstract class JournaledGrain<TGrainState> : Grain, IReplicationAdaptorHost, IProtocolParticipant
        where TGrainState : GrainState, IJournaledGrainState, new()
    {
        protected JournaledGrain()  { }

        public int Version { get; internal set; }

        public int UncommitedVersion
        {
            get { return Version + this.UncommitedEvents.Count; }
        }

        private readonly List<object> uncommitedEvents = new List<object>();

        internal IReadOnlyCollection<object> UncommitedEvents
        {
            get { return this.uncommitedEvents; }
        }

        /// <summary>
        /// Raise an event.
        /// </summary>
        /// <param name="event">Event to raise</param>
        /// <returns></returns>
        protected virtual void RaiseEvent<TEvent>(TEvent @event)
            where TEvent : class
        {
            if (@event == null) throw new ArgumentNullException("event");

            if (Adaptor != null)
                Adaptor.EnqueueUpdate(new JournalUpdate<TGrainState>() { Event = @event });
            else
            {
                this.uncommitedEvents.Add(@event);
                this.tentativeState.TransitionState(@event);
            }
        }

        internal void CommitEvents(int version)
        {
            this.Version = version;
            this.uncommitedEvents.Clear();
            this.tentativeState = null;
        }


        /// <summary>
        /// Adaptor for storage interface (queued grain).
        /// The storage keeps the journal.
        /// </summary>
        internal IQueuedGrainAdaptor<TGrainState> Adaptor { get; private set; }

        /// <summary>
        /// Called right after grain is constructed, to install the adaptor.
        /// </summary>
        void IReplicationAdaptorHost.InstallAdaptor(IReplicationProvider provider, object initialState, string graintypename, IReplicationProtocolServices services)
        {
            var grainState = (TGrainState)initialState;
            this.GrainState = grainState;

            // call the replication provider to construct the adaptor, passing the type argument
            Adaptor = provider.MakeReplicationAdaptor<TGrainState>(this, grainState, graintypename, services);
        }

        private TGrainState tentativeState;

        protected TGrainState State
        {
            get
            {
                if (Adaptor != null)
                    return this.Adaptor.TentativeState;
                else
                {
                    if(tentativeState == null)
                    {
                        tentativeState = this.ConfirmedState;

                        foreach (dynamic @event in this.UncommitedEvents)
                            tentativeState.TransitionState(@event);
                    }

                    return tentativeState;
                }
            }
        }

        protected TGrainState ConfirmedState
        {
            get
            {
                if (Adaptor != null)
                    return this.Adaptor.ConfirmedState;
                else
                    return base.GrainState as TGrainState;
            }
        }
        
        /// <summary>
        /// Waits until all previously raised events have been written. 
        /// </summary>
        /// <returns></returns>
        protected Task Commit()
        {
            if (Adaptor != null)
                return Adaptor.CurrentQueueHasDrained();
            else
                return this.Storage.WriteStateAsync();
        }

        /// <summary>
        /// Retrieves all events now. 
        /// </summary>
        /// <returns></returns>
        //protected Task FetchAllEventsNow()
        //{
        //    return Adaptor.SynchronizeNowAsync();
        //}


        #region Adaptor Hookup

        /// <summary>
        /// Notify replication adaptor of activation
        /// </summary>
        Task IProtocolParticipant.ActivateProtocolParticipant()
        {
            return Adaptor == null ? TaskDone.Done : Adaptor.Activate();
        }

        /// <summary>
        /// Notify replication adaptor of deactivation
        /// </summary>
        Task IProtocolParticipant.DeactivateProtocolParticipant()
        {
            return Adaptor == null ? TaskDone.Done : Adaptor.Deactivate();
        }

        /// <summary>
        /// Receive a message from other replicas, pass on to replication adaptor.
        /// </summary>
        [AlwaysInterleave]
        Task<IProtocolMessage> IProtocolParticipant.OnProtocolMessageReceived(IProtocolMessage payload)
        {
            return Adaptor == null ? Task.FromResult(payload) : Adaptor.OnProtocolMessageReceived(payload);
        }

        [AlwaysInterleave]
        Task IProtocolParticipant.OnMultiClusterConfigurationChange(MultiCluster.MultiClusterConfiguration next)
        {
            return Adaptor == null ? TaskDone.Done : Adaptor.OnMultiClusterConfigurationChange(next);
        }

        #endregion
    }
}
