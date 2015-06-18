using Orleans;
using GeoOrleans.Runtime.Strawman.ReplicatedGrains;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GeoOrleans.Benchmarks.Size.Interfaces;
using Orleans.Providers;
using GeoOrleans.Benchmarks.Common;

#pragma warning disable 1998

namespace GeoOrleans.Benchmarks.Size.Grains
{

    // An implementation of the leaderboard based on sequenced updates.
    // all operations are synchronous

    [StorageProvider(ProviderName = "AzureStore")]
    public class SequencedSizeGrain : SequencedGrain<SequencedSizeGrain.State>, GeoOrleans.Benchmarks.Size.Interfaces.ISequencedSizeGrain
    {
        [Serializable]
        public new class State
        {
            public Byte[] payload { get; set; }

            public State()
            {
                payload = new Byte[100];
            }
        }

        #region Queries

        //     public async Task<byte[]> ReadApprox(string post)
        public async Task<Byte[]> ReadApprox(string post)
        {

            return (await GetLocalStateAsync()).payload;
        }

        public async Task<Byte[]> ReadCurrent(string post)
        {
            return (await GetGlobalStateAsync()).payload;
        }

        #endregion

        #region Updates

        public async Task WriteNow(Byte[] pPayload)
        {
            GeoOrleans.Runtime.Common.Util.Assert(pPayload != null, "payload should never be null");
            await UpdateGloballyAsync(new WriteEvent() { payload = pPayload });
        }


        public async Task WriteLater(Byte[] pPayload)
        {
            GeoOrleans.Runtime.Common.Util.Assert(pPayload != null, "payload should never be null");
            await UpdateLocallyAsync(new WriteEvent() { payload = pPayload },false);
        }

        public override Task OnActivateAsync()
        {
            return base.OnActivateAsync();
        }

        [Serializable]
        public class WriteEvent : IAppliesTo<State>
        {
            public Byte[] payload { get; set; }

            public void Update(State state)
            {
                GeoOrleans.Runtime.Common.Util.Assert(payload != null, "Payload should not be null");
                state.payload = payload;
            }
        }

        #endregion


    }
}

