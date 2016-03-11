using System.Threading.Tasks;
using Orleans.Concurrency;
using Orleans.Runtime;
using Orleans.MultiCluster;
using System;
using System.Collections.Generic;
using Orleans.LogViews;

namespace Orleans.QueuedGrains
{


    /// <summary>
    /// Queued grain specialization where all update objects implement IUpdateOperation.
    /// </summary>
    /// <typeparam name="TGrainState"></typeparam>

    public abstract class QueuedGrain<TGrainState> :
       QueuedGrain<TGrainState, IUpdateOperation<TGrainState>>
       where TGrainState : class,new()
    {
        protected override void ApplyUpdate(TGrainState state, IUpdateOperation<TGrainState> update)
        {
            update.Update(state);
        }

    }

    /// <summary>
    /// Queued grain base class. 
    /// </summary>
    public abstract class QueuedGrain<TGrainState,TUpdate> : 
        LogViewGrain<TGrainState>, IProtocolParticipant,
        ILogViewAdaptorHost, ILogViewHost<TGrainState, TUpdate>
        where TGrainState : class,new()
        where TUpdate : class
    {
        protected QueuedGrain()
        { }

        
        internal ILogViewAdaptor<TGrainState,TUpdate> Adaptor { get; private set; }

        /// <summary>
        /// Called right after grain is constructed, to install the log view adaptor.
        /// </summary>
        void ILogViewAdaptorHost.InstallAdaptor(ILogViewProvider provider, object initialstate, string graintypename, IProtocolServices services)
        {
            // call the log view provider to construct the adaptor, passing the type argument
            Adaptor = provider.MakeLogViewAdaptor<TGrainState,TUpdate>(this, (TGrainState) initialstate, graintypename, services);            
        }

        void ILogViewHost<TGrainState, TUpdate>.TransitionView(TGrainState view, TUpdate entry)
        {
            ApplyUpdate(view, entry);
        }

        /// <summary>
        /// By default, updates are applied to state objects by calling "Apply(updateobject)" dynamically. To change this behavior, override this method.
        /// </summary>
        /// <param name="state">The state to mutate</param>
        /// <param name="update">The update object to apply</param>
        /// <returns></returns>
        protected virtual void ApplyUpdate(TGrainState state, TUpdate update)
        {
             // call the Apply function dynamically
            dynamic s = state;
            dynamic u = update;
            s.Apply(u);
        }

        string ILogViewHost<TGrainState, TUpdate>.IdentityString
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

        /// <summary>
        /// Receive a message from other clusters, pass on to log view adaptor.
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
        public void EnqueueUpdate(TUpdate update)
        {
            Adaptor.Submit(update);
        }


        /// <summary>
        /// Perform an update directly on the global state,
        /// but only if the state has not been modified in the meantime already.
        /// <param name="update">An object representing the update</param>
        /// <returns>true if the update was successful, and false if the update failed due to conflicts.</returns>
        /// </summary>
        public Task<bool> TryConditionalUpdateAsync(TUpdate update)
        {
           return Adaptor.TryAppend(update);
        }


        /// <summary>
        /// Returns the current queue of unconfirmed updates.
        /// </summary>
        public IEnumerable<TUpdate> UnconfirmedUpdates
        { 
           get { return Adaptor.UnconfirmedSuffix; } 
        }

        /// <summary>
        /// Returns a task that can be waited on to ensure all updates currently in the queue have been confirmed.
        /// </summary>
        /// <returns></returns>
        public Task CurrentQueueHasDrained()
        {
            return Adaptor.ConfirmSubmittedEntriesAsync();
        }


        /// <summary>
        /// The tentative state of this grain (read-only).
        /// This is always equal to (ConfirmedState) with all the updates in (UnconfirmedUpdates) applied on top.
        /// </summary>
        public TGrainState TentativeState
        {
            get { return Adaptor.TentativeView; }
        }

        /// <summary>
        /// The last confirmed snapshot of the global state (read-only).
        /// Does not include the effect of the updates in (UnconfirmedUpdates).
        /// </summary>
        public TGrainState ConfirmedState
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

        public LogViewStatistics GetStats()
        {
            return Adaptor.GetStats();
        }

        #endregion

        void ILogViewHost<TGrainState, TUpdate>.OnViewChanged(bool TentativeStateChanged, bool ConfirmedStateChanged)
        {
            if (ConfirmedStateChanged && listeners != null)
                foreach (var l in listeners)
                    l.OnConfirmedStateChanged();
        }

     
        /// <summary>
        /// Subscribe to notifications on changes to the confirmed state.
        /// </summary>
        public bool SubscribeConfirmedStateListener(IStateChangedListener listener)
        {
            if (listeners == null)
                listeners = new List<IStateChangedListener>();

            if (listeners.Contains(listener))
            {
                return false;
            }
            else
            {
                listeners.Add(listener);
            }
            return true;
        }


        /// <summary>
        /// Unsubscribe from notifications on changes to the confirmed state.
        /// </summary>
        public bool UnSubscribeConfirmedStateListener(IStateChangedListener listener)
        {
            if (listeners == null)
                return false;
            return listeners.Remove(listener);
        }


        protected List<IStateChangedListener> listeners;

       

       
    


}

    /// <summary>
    /// A listener that is notified when the confirmed view changes.
    /// </summary>
    public interface IStateChangedListener
    {
        /// <summary>
        /// Gets called after the confirmed prefix has changed.
        /// <param name="version">the new length of the confirmed prefix</param>
        /// </summary>
        /// 
        void OnConfirmedStateChanged();
    }
}
