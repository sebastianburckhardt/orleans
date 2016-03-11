﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ReplicatedEventSample.Grains
{
    /// <summary>
    ///  state of an event
    /// </summary>
    [Serializable]
    public class EventState
    {
        /// <summary>
        ///  list of all outcomes, sorted by timestamp
        /// </summary>
        public SortedDictionary<DateTime,Outcome> outcomes;


        public void Apply(Outcome outcome)
        {
            if (outcome == null)
                throw new ArgumentNullException("outcome");

            // idempotency check: ignore update if already there
            if (outcomes.ContainsKey(outcome.Timestamp))
                return;

            outcomes.Add(outcome.Timestamp, outcome);
        }
    }


}
