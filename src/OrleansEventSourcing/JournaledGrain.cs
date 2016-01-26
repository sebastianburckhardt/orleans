using Orleans.Concurrency;
using Orleans.MultiCluster;
using Orleans.LogViews;
using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Orleans.EventSourcing
{
    /// <summary>
    /// The base class for all grain classes that have event-sourced state.
    /// </summary>
    public abstract class JournaledGrain<TGrainState> :
        LogViewGrain<TGrainState>, IProtocolParticipant,
        ILogViewAdaptorHost, ILogViewHost<TGrainState, object>
        where TGrainState : class,new(),IJournaledGrainState
    {
        protected JournaledGrain()  { }

        /// <summary>
        /// Raise an event.
        /// </summary>
        /// <param name="event">Event to raise</param>
        /// <returns></returns>
        protected virtual void RaiseEvent<TEvent>(TEvent @event)
            where TEvent : class
        {
            if (@event == null) throw new ArgumentNullException("event");

            LogView.Submit(@event);
        }

        /// <summary>
        /// Raise multiple events, as an atomic sequence.
        /// </summary>
        /// <param name="event">Events to raise</param>
        /// <returns></returns>
        protected virtual void RaiseEvents<TEvent>(IEnumerable<TEvent> events)
            where TEvent : class
        {
            if (events == null) throw new ArgumentNullException("events");

            LogView.SubmitRange(events);
        }


        /// <summary>
        /// Raise an event conditionally. 
        /// Succeeds only if there are no conflicts, that is, no other events were raised in the meantime.
        /// </summary>
        /// <param name="event">Event to raise</param>
        /// <returns>true if successful, false if there was a conflict.</returns>
        protected virtual Task<bool> RaiseConditionalEvent<TEvent>(TEvent @event)
            where TEvent : class
        {
            if (@event == null) throw new ArgumentNullException("event");

            return LogView.TryAppend(@event);
        }


        /// <summary>
        /// Raise multiple events, as an atomic sequence, conditionally. 
        /// Succeeds only if there are no conflicts, that is, no other events were raised in the meantime.
        /// </summary>
        /// <param name="event">Events to raise</param>
        /// <returns>true if successful, false if there was a conflict.</returns>
        protected virtual Task<bool> RaiseConditionalEvents<TEvent>(IEnumerable<TEvent> events)
            where TEvent : class
        {
            if (events == null) throw new ArgumentNullException("events");

            return LogView.TryAppendRange(events);
        }

        /// <summary>
        /// Adaptor for log view provider.
        /// The storage keeps the log and/or the latest state.
        /// </summary>
        internal ILogViewAdaptor<TGrainState,object> LogView { get; private set; }


        /// <summary>
        /// The current state (includes both confirmed and unconfirmed events).
        /// </summary>
        protected TGrainState State
        {
            get { return this.LogView.TentativeView; }
        }

        /// <summary>
        /// The version of the tentative state.
        /// Always equal to the confirmed version plus the number of unconfirmed events.
        /// </summary>
        protected int TentativeVersion 
        {
            get { return this.LogView.ConfirmedVersion + this.LogView.UnconfirmedSuffix.Count(); }
        }

        /// <summary>
        /// The current confirmed state (includes only confirmed events).
        /// </summary>
        protected TGrainState ConfirmedState
        {
            get { return this.LogView.ConfirmedView; }
        }
        
        /// <summary>
        /// The version of the confirmed state.
        /// Always equal to the number of confirmed events.
        /// </summary>
        protected int ConfirmedVersion 
        {
            get { return this.LogView.ConfirmedVersion; }
        }

               /// <summary>
        /// Waits until all previously raised events have been written. 
        /// </summary>
        /// <returns></returns>
        protected Task WaitForConfirmationOfEvents()
        {
           return LogView.ConfirmSubmittedEntriesAsync();
             
        }

        /// <summary>
        /// Retrieves all events now. 
        /// </summary>
        /// <returns></returns>
        protected Task FetchAllEventsNow()
        {
            return LogView.SynchronizeNowAsync();
        }


        /// <summary>
        /// Override this for custom ways of transitioning the state.
        /// All exceptions thrown by this method are caught and logged by the log view provider.
        /// </summary>
        /// <param name="state"></param>
        /// <param name="event"></param>
        protected virtual void TransitionState(TGrainState state, object @event)
        {
            dynamic x = state;
            x.Apply(@event);
        }


    

        #region Adaptor Hookup

        /// <summary>
        /// Called right after grain is constructed, to install the adaptor.
        /// </summary>
        void ILogViewAdaptorHost.InstallAdaptor(ILogViewProvider provider, object initialState, string graintypename, IProtocolServices services)
        {
            // call the replication provider to construct the adaptor, passing the type argument
            LogView = provider.MakeLogViewAdaptor<TGrainState, object>(this, (TGrainState)initialState, graintypename, services);            
        }
        
        void ILogViewHost<TGrainState, object>.TransitionView(TGrainState view, object entry)
        {
            TransitionState(view, entry);
        }

        Task IProtocolParticipant.ActivateProtocolParticipant()
        {
            return LogView == null ? TaskDone.Done : LogView.Activate();
        }

        Task IProtocolParticipant.DeactivateProtocolParticipant()
        {
            return LogView == null ? TaskDone.Done : LogView.Deactivate();
        }
 
        [AlwaysInterleave]
        Task<IProtocolMessage> IProtocolParticipant.OnProtocolMessageReceived(IProtocolMessage payload)
        {
            return LogView == null ? Task.FromResult(payload) : LogView.OnProtocolMessageReceived(payload);
        }
    
        [AlwaysInterleave]
        Task IProtocolParticipant.OnMultiClusterConfigurationChange(MultiCluster.MultiClusterConfiguration next)
        {
            return LogView == null ? TaskDone.Done : LogView.OnMultiClusterConfigurationChange(next);
        }

        #endregion
    }
}
