using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;

using ReplicatedEventSample.Interfaces;

using Orleans;
using Orleans.Providers;
using Orleans.QueuedGrains;
using Orleans.Concurrency;
using Orleans.Core;

namespace ReplicatedEventSample.Grains
{

    public class EventGrain : QueuedGrain<EventState, Outcome>, IEventGrain, IStateChangedListener
    {

        public Task NewOutcome(Outcome outcome)
        {
            EnqueueUpdate(outcome);
            return TaskDone.Done;
        }

        public Task<List<KeyValuePair<string, int>>> GetTopThree()
        {
            var result = ConfirmedState.outcomes
                .OrderByDescending(o => o.Value.Score)
                .Take(3)
                .Select(o => new KeyValuePair<string, int>(o.Value.Name, o.Value.Score))
                .ToList();
           
            return Task.FromResult(result);
        }


        public override Task OnActivateAsync()
        {
            // get reference to ticker grain (there is just one per deployment, it has key 0)
            tickergrain = GrainFactory.GetGrain<ITickerGrain>(0);

            // we want to react to changes in the event state, so we subscribe on activation
            SubscribeConfirmedStateListener(this);

            return TaskDone.Done;
        }


        bool results_have_started;
        string last_announced_leader;
        ITickerGrain tickergrain;

        public void OnConfirmedStateChanged()
        {
            string message = null;

            // notify on first results
            if (!results_have_started && ConfirmedState.outcomes.Count > 0)
            {
                message = string.Format("first results arriving for {0}", this.GetPrimaryKeyString());
                results_have_started = true;
            }

            // notify about leader after first 5 results are in
            if (ConfirmedState.outcomes.Count > 5)
            {
                var leader = ConfirmedState.outcomes.OrderByDescending(o => o.Value.Score).First().Value.Name;
                if (last_announced_leader == null)
                    message = string.Format("{0} is leading {1}", leader, this.GetPrimaryKeyString());
                else if (last_announced_leader != leader)
                    message = string.Format("{0} is now leading {1}", leader, this.GetPrimaryKeyString());
                last_announced_leader = leader;
            }

            if (message != null)
            {
                // send message as a background task
                var bgtask = tickergrain.SomethingHappened(message);
            }
        }

    }
}
