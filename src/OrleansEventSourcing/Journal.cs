using System;
using System.Collections;
using System.Collections.Generic;
using Orleans.CodeGeneration;
using Orleans.Replication;


namespace Orleans.EventSourcing
{

    /// <summary>
    /// The stored state of an event sourced grain is a journal of events.
    /// </summary>
    [Serializable]
    internal class Journal  : GrainState
    {
        public List<object> Events { get; set; }

        public int Version { get { return Events.Count; } }

        public Journal()
        {
            Events = new List<object>();
        }

    }

    /// <summary>
    /// The journal is updated by appending an event.
    /// </summary>
    [Serializable]
    internal class JournalUpdate : IUpdateOperation<Journal>
    {
        public object Event { get; set; }

        public void Update(Journal state)
        {
            state.Events.Add(Event);
        }
    }


 
}
