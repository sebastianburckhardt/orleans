
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Examples.Interfaces;
using Orleans;
using Orleans.Providers;
using Orleans.QueuedGrains;



namespace Examples.Grains
{

    /// <summary>
    /// The state of the blob grain
    /// </summary>
    [Serializable]
    public class BlobState
    {
        public byte[] Value { get; set; }
    }


    /// <summary>
    /// The class that defines the operation for replacing the blob content
    /// </summary>
    [Serializable]
    public class ValueChanged : IUpdateOperation<BlobState>
    {
        public byte[] NewValue { get; set; }

        public void ApplyToState(BlobState state)
        {
            state.Value = NewValue;
        }
    }




    /// <summary>
    /// The grain implementation
    /// </summary>
    public class BlobGrain : QueuedGrain<BlobState,IUpdateOperation<BlobState>>, IBlobGrain
    {
        protected override void ApplyDeltaToState(BlobState state, IUpdateOperation<BlobState> delta)
        {
            delta.ApplyToState(state);
        }


        public Task<byte[]> Get()
        {
            return Task.FromResult(this.TentativeState.Value);
        }

        public Task Set(byte[] value)
        {
            EnqueueUpdate(new ValueChanged() { NewValue = value });
            return TaskDone.Done;
        }
    }



}

