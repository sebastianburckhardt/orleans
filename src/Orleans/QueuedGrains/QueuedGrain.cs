using System.Threading.Tasks;
using Orleans.Concurrency;
using Orleans.Runtime;
using Orleans.MultiCluster;
using System;
using System.Collections.Generic;
using Orleans.Core;
using Orleans.LogViews;

namespace Orleans.QueuedGrains
{

    /// <summary>
    /// Queued grain base class.
    /// <typeparam name="TState">The type for the state of this grain.</typeparam>
    /// <typeparam name="TDelta">The type for objects that represent updates to the state.</typeparam>
    /// </summary>
    public abstract class QueuedGrain<TState,TDelta> : 
        LogViewGrainBase<TState>,
        ILogViewGrain, 
        IProtocolParticipant,
        ILogViewHost<TState, TDelta>
        where TState : class,new()
        where TDelta : class
    {
        protected QueuedGrain()
        { }

        /// <summary>
        /// Grain implementers do NOT have to expose this constructor but can choose to do so.
        /// This constructor is particularly useful for unit testing where test code can create a Grain and replace
        /// the IGrainIdentity and IGrainRuntime with test doubles (mocks/stubs).
        /// </summary>
        /// <param name="identity"></param>
        /// <param name="runtime"></param>
        protected QueuedGrain(IGrainIdentity identity, IGrainRuntime runtime) : base(identity, runtime)
        { }

        // the object encapsulating the log view provider functionality and local state
        internal ILogViewAdaptor<TState,TDelta> Adaptor { get; private set; }


        // Called right after grain is constructed, to install the log view adaptor
        void ILogViewGrain.InstallAdaptor(ILogViewProvider provider, object initialstate, string graintypename, IProtocolServices services)
        {
            // call the log view provider to construct the adaptor, passing the type argument
            Adaptor = provider.MakeLogViewAdaptor<TState,TDelta>(this, (TState) initialstate, graintypename, services);            
        }

        void ILogViewHost<TState, TDelta>.UpdateView(TState view, TDelta entry)
        {
            ApplyDeltaToState(view, entry);
        }

        /// <summary>
        /// Subclasses must implement this method to define how deltas are applied to states.
        /// </summary>
        /// <param name="state">The state to mutate</param>
        /// <param name="delta">The update object to apply</param>
        /// <returns></returns>
        protected abstract void ApplyDeltaToState(TState state, TDelta delta);
  

        string ILogViewHost<TState, TDelta>.IdentityString
        {
            get { return Identity.IdentityString; }
        }

        /// <summary>
        /// Notify log view adaptor of activation
        /// </summary>
        public Task ActivateProtocolParticipant()
        {
            return Adaptor.Activate();
        }

        /// <summary>
        /// Notify log view adaptor of deactivation
        /// </summary>
        public Task DeactivateProtocolParticipant()
        {
            return Adaptor.Deactivate();
        }

        
        // Receive a message from other clusters, passed on to log view adaptor.
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

        void ILogViewHost<TState, TDelta>.OnViewChanged(bool TentativeStateChanged, bool ConfirmedStateChanged)
        {
            if (TentativeStateChanged)
                OnTentativeStateChanged();
            if (ConfirmedStateChanged)
                OnConfirmedStateChanged();
        }

        /// <summary>
        /// Called after the tentative state may have changed due to local or remote events.
        /// <para>Override this to react to changes of the tentative state.</para>
        /// </summary>
        protected virtual void OnTentativeStateChanged()
        {
        }

        /// <summary>
        /// Called after the confirmed state may have changed (i.e. the confirmed version increased).
        /// <para>Override this to react to changes of the confirmed state.</para>
        /// </summary>
        protected virtual void OnConfirmedStateChanged()
        {
        }

     
        /// <summary>
        /// Retrieve the current, latest global version of the state, and confirm all updates currently in the queue. 
        /// <returns>A task that can be waited on.</returns>
        /// </summary>
        public Task SynchronizeNowAsync()
        {
            return Adaptor.SynchronizeNowAsync();
        }

        /// <summary>
        /// Enqueues an update.
        /// The update becomes visible in (TentativeState) immediately,
        /// and in (ConfirmedState) after it is confirmed, which happens automatically in the background.
        /// <param name="update">An object representing the update</param>
        /// </summary>
        public void EnqueueUpdate(TDelta update)
        {
            Adaptor.Submit(update);
        }


        /// <summary>
        /// Enqueues multiple updates, as an atomic sequence.
        /// The updates becomes visible in (TentativeState) immediately,
        /// and in (ConfirmedState) after they are confirmed, which happens automatically in the background.
        /// <param name="updates">A sequence of objects representing the updates</param>
        /// </summary>
        public void EnqueueUpdateSequence(IEnumerable<TDelta> updates)
        {
            Adaptor.SubmitRange(updates);
        }


        /// <summary>
        /// Perform a conditional update. The update fails if other updates modify the state first.
        /// <param name="update">An object representing the update</param>
        /// <returns>true if the update was successful, and false if the update failed due to conflicts.</returns>
        /// </summary>
        public Task<bool> TryConditionalUpdateAsync(TDelta update)
        {
           return Adaptor.TryAppend(update);
        }

        /// <summary>
        /// Perform a sequence of updates, conditionally and atomically. Fails if other updates modify the state first.
        /// <param name="updates">A sequence of objects representing the updates</param>
        /// <returns>true if the update was successful, and false if the update failed due to conflicts.</returns>
        /// </summary>
        public Task<bool> TryConditionalUpdateSequenceAsync(IEnumerable<TDelta> updates)
        {
            return Adaptor.TryAppendRange(updates);
        }

        /// <summary>
        /// Returns the current queue of unconfirmed updates.
        /// </summary>
        public IEnumerable<TDelta> UnconfirmedUpdates
        { 
           get { return Adaptor.UnconfirmedSuffix; } 
        }

        /// <summary>
        /// Waits until all updates currently in the queue are confirmed.
        /// </summary>
        /// <returns>A task that can be waited on.</returns>
        public Task ConfirmUpdates()
        {
            return Adaptor.ConfirmSubmittedEntriesAsync();
        }


        /// <summary>
        /// The tentative state of this grain (read-only).
        /// This state is equivalent to (ConfirmedState) with all the updates in (UnconfirmedUpdates) applied on top.
        /// </summary>
        public TState TentativeState
        {
            get { return Adaptor.TentativeView; }
        }

        /// <summary>
        /// The last confirmed snapshot of the global state (read-only).
        /// Does not include the effect of the updates in (UnconfirmedUpdates).
        /// </summary>
        public TState ConfirmedState
        {
            get { return Adaptor.ConfirmedView; }
        }

        /// <summary>
        /// The version of the last confirmed snapshot of the global state.
        /// </summary>
        public int ConfirmedVersion
        {
            get { return Adaptor.ConfirmedVersion; }
        }

        /// <summary>
        /// The last exception thrown by any internal storage or network operation,
        /// or null if the last such operation was successful.
        /// </summary>
        public Exception LastException
        {
            get { return Adaptor.LastException; }
        }

        /// <summary>
        /// Enable statistics collection in the log view provider.
        /// </summary>
        public void EnableStatsCollection()
        {
            Adaptor.EnableStatsCollection();
        }

        /// <summary>
        /// Disable statistics collection in the log view provider.
        /// </summary>
        public void DisableStatsCollection()
        {
            Adaptor.DisableStatsCollection();
        }

        /// <summary>
        /// access internal statistics about the log view provider.
        /// </summary>
        /// <returns>an object containing statistics.</returns>
        public LogViewStatistics GetStats()
        {
            return Adaptor.GetStats();
        }

       

    }

   
}
