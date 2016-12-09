using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.EventSourcing
{
    /// <summary>
    /// A queued grain where deltas
    /// are applied to the state by dynamically calling state.Apply(delta).
    /// </summary>
    public class QueuedGrainWithDynamicApply<TState> : QueuedGrain<TState,object>
        where TState : class, new()
    {
        protected override void ApplyDeltaToState(TState state, object delta)
        {
            // call the Apply function dynamically
            dynamic s = state;
            dynamic u = delta;
            s.Apply(u);
        }
    }

    /// <summary>
    /// A queued grain where delta objects implement the IUpdateOperation interface
    /// </summary>
    public class QueuedGrainWithApplicableDeltas<TState> : QueuedGrain<TState, IUpdateOperation<TState>>
        where TState : class, new()
    {
        protected override void ApplyDeltaToState(TState state, IUpdateOperation<TState> delta)
        {
            delta.ApplyToState(state);
        }
    }

    /// <summary>
    /// A queued grain where state objects implement the IUpdateableBy interface
    /// </summary>
    public class QueuedGrainWithApplicableState<TState, TDelta> : QueuedGrain<TState, TDelta>
        where TState : class, IUpdatedBy<TDelta>, new()
        where TDelta : class 
    {
        protected override void ApplyDeltaToState(TState state, TDelta delta)
        {
            state.ApplyDelta(delta);
        }
    }


    /// <summary>
    /// Interface for objects that are deltas of some state type.
    /// </summary>
    public interface IUpdateOperation<TState>
    {
        /// <summary>
        /// Apply this delta to the given state.
        /// </summary>
        void ApplyToState(TState state);
    }


 
    /// <summary>
    /// Interface for state objects that can be updated by delta objects. 
    /// </summary>
    public interface IUpdatedBy<TDelta>
    {
        /// <summary>
        /// Apply the given delta to this state.
        /// </summary>
        void ApplyDelta(TDelta delta);
    }
}
