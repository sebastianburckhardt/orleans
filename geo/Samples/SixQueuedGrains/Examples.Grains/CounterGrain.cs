using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Examples.Interfaces;
using Orleans;
using Orleans.Providers;
using Orleans.Replication;

namespace Examples.Grains
{

    #region Classes for State And Updates

    /// <summary>
    /// The state of the counter grain
    /// </summary>
    [Serializable]
    public class CounterState : GrainState
    {
        // the current count
        // it's a public property so it gets serialized/deserialized
        public int Count { get; set; }
    }


    /// <summary>
    /// The class that defines the increment operation
    /// </summary>
    [Serializable]
    public class IncrementedEvent : IUpdateOperation<CounterState>
    {
        /// <summary>
        /// Effect of the increment on the counter state.
        /// </summary>
        public void Update(CounterState state)
        {
            state.Count++;
        }
    }

    #endregion

    /// <summary>
    /// The grain implementation for the counter grain.
    /// It favors availability over consistency - an approximate counter value is returned in
    /// situations where cluster communication is not working.
    /// </summary>
    [ReplicationProvider(ProviderName = "SharedStorage")]
    public class CounterGrain : QueuedGrain<CounterState>, ICounterGrain
    {
        public Task<int> Get()
        {
            return Task.FromResult(TentativeState.Count);
        }

        public Task Increment()
        {
            EnqueueUpdate(new IncrementedEvent());
            return TaskDone.Done;
        }
    }

    /// <summary>
    /// Another version of the counter grain that favors consistency over availability.
    /// All operations are linearizable. Meaning they block if communication is not working.
    /// </summary>
    [ReplicationProvider(ProviderName = "SharedStorage")]
    public class LinearizableCounterGrain : QueuedGrain<CounterState>, ICounterGrain
    {
        public async Task Increment()
        {
            EnqueueUpdate(new IncrementedEvent());
            await CurrentQueueHasDrained();
        }
        public async Task<int> Get()
        {
            await SynchronizeNowAsync();
            return ConfirmedState.Count;
        }

    }
}

