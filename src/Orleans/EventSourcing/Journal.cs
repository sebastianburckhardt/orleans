using System;
using System.Collections;
using System.Collections.Generic;
using Orleans.CodeGeneration;
using Orleans.Replication;


namespace Orleans.EventSourcing
{
    /// <summary>
    /// The journal is updated by appending an event.
    /// </summary>
    [Serializable]
    public class JournalUpdate<TGrainState> : IUpdateOperation<TGrainState>
        where TGrainState : GrainState, IJournaledGrainState
    {
        public object Event { get; set; }

        public void Update(TGrainState state)
        {
            state.TransitionState((dynamic)this.Event);
        }
    }
}
