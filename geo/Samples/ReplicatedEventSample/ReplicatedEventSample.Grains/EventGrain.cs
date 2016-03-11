using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using ReplicatedEventSample.Interfaces;
using Orleans;
using Orleans.Providers;
using Orleans.QueuedGrains;
using Orleans.Concurrency;


namespace ReplicatedEventSample.Grains
{

    public class EventGrain : QueuedGrain<EventState>, IEventGrain
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
                .Select(o => new KeyValuePair<string,int>(o.Value.Name, o.Value.Score))
                .ToList();
            return Task.FromResult(result);
        }

        public Task<string> GetRecentOutcome()
        {
            var result = ConfirmedState.outcomes.LastOrDefault().Value;

            // if there is no recent outcome, return empty string
            if (result == null || (DateTime.UtcNow - result.Timestamp > TimeSpan.FromSeconds(10)))
                return Task.FromResult("");

            return Task.FromResult(string.Format("{0}: {1} scores {2}", result.Timestamp, result.Name, result.Score);
        }
    }
}
