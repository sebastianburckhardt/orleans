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
    public abstract class JournaledGrain<StateType> : Grain, IProtocolParticipant, IReplicationAdaptorHost
                                                          where StateType : class, new()
    {
        protected JournaledGrain()  { }

        /// <summary>
        /// Adaptor for storage interface (queued grain).
        /// The storage keeps the journal.
        /// The journal has no dependency on the type TGrainState.
        /// </summary>
        internal IQueuedGrainAdaptor<Journal> Adaptor { get; private set; }

        /// <summary>
        /// Called right after grain is constructed, to install the adaptor.
        /// </summary>
        void IReplicationAdaptorHost.InstallAdaptor(IReplicationProvider provider, object initialstate, string graintypename, IReplicationProtocolServices services)
        {
            version = 0;
            state = (StateType)initialstate;

            // call the replication provider to construct the adaptor, passing the type argument
            Adaptor = provider.MakeReplicationAdaptor<Journal>(this, new Journal(), graintypename, services);
        }


        // the version and state are constructed from the journal
        // for now I am basing this off the confirmed state only
        // we can consider exposing the tentative state
        private int version = 0;
        private StateType state;

        protected StateType State
        {
            get
            {
                var confirmedstate = Adaptor.ConfirmedState;

                while (version < confirmedstate.Version)
                    StateTransition(state, confirmedstate.Events[version++]);

                return state;
            }
        }
        
        // subclasses can override this if they want to implement transitions differently
        // for example, if they want to use static typing
        protected virtual void StateTransition<TEvent>(StateType state, TEvent @event)
            where TEvent : class
        {
            dynamic dstate = state;

            try
            {
                dstate.Apply(@event);
            }
            catch(MissingMethodException)
            {
                OnMissingStateTransition(@event);
            }
        }

        protected virtual void OnMissingStateTransition(object @event)
        {
            // Log
        }

        /// <summary>
        /// Raise an event.
        /// </summary>
        /// <param name="event">Event to raise</param>
        /// <returns></returns>
        protected void RaiseStateEvent<TEvent>(TEvent @event)
            where TEvent : class
        {
            if (@event == null) throw new ArgumentNullException("event");

            Adaptor.EnqueueUpdate(new JournalUpdate() { Event = @event });
        }

        /// <summary>
        /// Waits until all previously raised events have been written. 
        /// </summary>
        /// <returns></returns>
        protected Task WaitForWriteCompletion()
        {
            return Adaptor.CurrentQueueHasDrained();
        }

        /// <summary>
        /// Retrieves all events now. 
        /// </summary>
        /// <returns></returns>
        protected Task FetchAllEventsNow()
        {
            return Adaptor.SynchronizeNowAsync();
        }


        #region Adaptor Hookup

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

        #endregion

    }
}
