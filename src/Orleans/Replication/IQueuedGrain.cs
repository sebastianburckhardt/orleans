using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Replication
{
    /// <summary>
    /// The API of a queued grain.
    /// </summary>
    /// <typeparam name="TGrainState"></typeparam>
    public interface IQueuedGrain<TGrainState> where TGrainState: GrainState
    {

        #region Fast local operations

        TGrainState TentativeState { get; }

        TGrainState ConfirmedState { get; }

        void EnqueueUpdate(IUpdateOperation<TGrainState> update);

        IEnumerable<IUpdateOperation<TGrainState>> UnconfirmedUpdates { get; }

        #endregion


        #region Slow operations involving synchronization

        Task CurrentQueueHasDrained();
        
        Task SynchronizeNowAsync();

        #endregion


        #region Reactivity

        /// <summary>
        /// Subscribe to notifications on changes to the confirmed state.
        /// </summary>
        bool SubscribeConfirmedStateListener(IConfirmedStateListener aListener);

        /// <summary>
        /// Unsubscribe from notifications on changes to the confirmed state.
        /// </summary>
        bool UnSubscribeConfirmedStateListener(IConfirmedStateListener aListener);

        #endregion


        #region Diagnostics

        Exception LastException { get; }

        void EnableStatsCollection();

        void DisableStatsCollection();

        QueuedGrainStatistics GetStats();

        #endregion
    }


    /// <summary>
    /// A listener that can observe changes to the confirmed state.
    /// </summary>
    public interface IConfirmedStateListener
    {
        /// <summary>
        /// Gets called after the confirmed state has changed.
        /// </summary>
        /// 
        void OnConfirmedStateChanged();
    }

    /// <summary>
    /// A collection of statistics for queued grains
    /// </summary>
    public class QueuedGrainStatistics
    {
        /// <summary>
        /// A map from event names to a count
        /// </summary>
        public Dictionary<String, long> eventCounters;
        /// <summary>
        /// A list of all measured stabilization latencies
        /// </summary>
        public List<int> stabilizationLatenciesInMsecs;
    }

}
