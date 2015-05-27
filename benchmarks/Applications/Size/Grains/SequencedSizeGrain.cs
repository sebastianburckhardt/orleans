using Orleans;
using ReplicatedGrains;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Size.Interfaces;
using Orleans.Providers;
#pragma warning disable 1998

namespace Size.Grains
{

    // An implementation of the leaderboard based on sequenced updates.
    // all operations are synchronous

    [StorageProvider(ProviderName = "AzureStore")]
    public class SequencedSizeGrain : SequencedGrain<SequencedSizeGrain.State>, Size.Interfaces.ISequencedSizeGrain
    {
        [Serializable]
        public new class State
        {
            public byte[] payload { get; set; }

            public State()
            {
                
            } 

        }

        #region Queries

        public async Task<byte[]> ReadApprox(string post)
        {
            
            return (await GetLocalStateAsync()).payload;
        }

        public async Task<byte[]> ReadCurrent(string post)
        {
            return (await GetGlobalStateAsync()).payload;
        }

        #endregion

        #region Updates

        public async Task WriteNow(byte[] pPayload)
        {
            await UpdateGloballyAsync(new WriteEvent() { payload = pPayload });
        }


        public async Task WriteLater(byte[] pPayload)
        {
            await UpdateLocallyAsync(new WriteEvent() { payload = pPayload });
        }

        public override Task OnActivateAsync()
        {
            
            return base.OnActivateAsync();
        }

        [Serializable]
        public class WriteEvent : IAppliesTo<State>
        {
            public byte[] payload { get; set; } 
            public void Update(State state)
            {
                state.payload = payload;
            }
        }

        #endregion


    }
}

